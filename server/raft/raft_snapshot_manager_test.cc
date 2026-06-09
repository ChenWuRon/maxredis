// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/snapshot_manager.h"

#include <gmock/gmock.h>

#include <absl/container/flat_hash_map.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "base/gtest.h"
#include "base/logging.h"
#include "server/raft/command_log.h"
#include "server/raft/raft_types.h"
#include "server/raft/snapshot_writer.h"
#include "server/state_machine/state_machine.h"

namespace dfly {

using namespace testing;

// --- Mock state machine that counts snapshot calls ---

class MockSnapshotSM : public IStateMachine {
 public:
  int snapshot_call_count = 0;
  std::string last_snapshot_path;

  ApplyResult Apply(const CommandId*, CmdArgList) override {
    return {ApplyOp::OK, 0};
  }

  ApplyResult ApplyLogEntry(const LogEntry&) override {
    return {ApplyOp::OK, 1};
  }

  void Set(DbIndex, std::string_view, std::string_view) override {}
  bool Del(DbIndex, std::string_view) override { return false; }
  bool Expire(DbIndex, std::string_view, uint64_t) override { return false; }
  OpResult<std::string> Get(DbIndex, std::string_view) override {
    return OpStatus::KEY_NOTFOUND;
  }
  size_t DbSize(DbIndex) const override { return 0; }
  void Schedule(DbIndex, std::string_view,
                std::function<void(EngineShard*)>) override {}

  bool SaveSnapshot(const std::string& path) override {
    snapshot_call_count++;
    last_snapshot_path = path;

    // Write a valid empty snapshot so the file exists.
    SnapshotWriter writer(path);
    if (!writer.Open())
      return false;
    return writer.Finalize(0);
  }
};

// --- In-memory log storage for testing ---

class TestLogStorage : public ILogStorage {
 public:
  LogIndex last_index = 0;
  Term last_term = 0;

  size_t LogSize() const override { return last_index; }
  LogIndex FirstIndex() const override { return 0; }
  LogIndex LastIndex() const override { return last_index; }
  Term LastTerm() const override { return last_term; }
  const LogEntry* Get(LogIndex) const override { return nullptr; }
  LogIndex Append(LogEntry) override { return 0; }
  std::vector<LogEntry> GetRange(LogIndex, size_t) const override { return {}; }
  void TruncateFrom(LogIndex) override {}
  bool CompactUpTo(LogIndex) override { return true; }
  void Clear() override {}
};

class SnapshotManagerTest : public Test {
 protected:
  void SetUp() override {
    dir_ = "/tmp/snapshot_mgr_test_" + std::to_string(getpid());
    mkdir(dir_.c_str(), 0755);
  }

  void TearDown() override {
    unlink((dir_ + "/snapshot.meta").c_str());
    unlink((dir_ + "/snapshot.meta.tmp").c_str());
    unlink((dir_ + "/snapshot.bin").c_str());
    unlink((dir_ + "/snapshot.bin.tmp").c_str());
    rmdir(dir_.c_str());
  }

  std::string dir_;
};

// --- SnapshotBarrier tests ---

TEST(SnapshotBarrierTest, NoContention) {
  SnapshotBarrier barrier;

  barrier.BeginRead();
  barrier.EndRead();

  barrier.BeginRead();
  barrier.EndRead();

  barrier.BeginWrite();
  barrier.EndWrite();
}

TEST(SnapshotBarrierTest, ConcurrentReads) {
  SnapshotBarrier barrier;

  barrier.BeginRead();
  barrier.BeginRead();
  barrier.BeginRead();
  barrier.EndRead();
  barrier.EndRead();
  barrier.EndRead();

  // Write should proceed immediately after all reads end.
  barrier.BeginWrite();
  barrier.EndWrite();
}

TEST(SnapshotBarrierTest, WriteBlocksUntilReadsComplete) {
  SnapshotBarrier barrier;

  barrier.BeginRead();

  // Schedule a fiber that tries BeginWrite (should block).
  bool write_completed = false;
  util::fb2::Fiber writer_fiber("writer", [&] {
    barrier.BeginWrite();
    write_completed = true;
    barrier.EndWrite();
  });

  // Give writer fiber a chance to start and block.
  util::ThisFiber::SleepFor(std::chrono::milliseconds(10));
  EXPECT_FALSE(write_completed);

  // End read → writer should proceed.
  barrier.EndRead();
  util::ThisFiber::SleepFor(std::chrono::milliseconds(10));
  EXPECT_TRUE(write_completed);

  writer_fiber.Join();
}

// --- SnapshotManager tests ---

TEST_F(SnapshotManagerTest, CreateSnapshotNoData) {
  MockSnapshotSM sm;
  TestLogStorage log;
  log.last_index = 0;
  log.last_term = 0;

  SnapshotManager mgr(dir_, &sm, &log);
  ASSERT_TRUE(mgr.CreateSnapshot());

  // Verify snapshot file exists.
  std::ifstream ifs(dir_ + "/snapshot.bin");
  EXPECT_TRUE(ifs.is_open());
  ifs.close();

  // Verify metadata was saved.
  EXPECT_EQ(0u, mgr.meta().index);
  EXPECT_EQ(0u, mgr.meta().term);
}

TEST_F(SnapshotManagerTest, CreateSnapshotWithData) {
  MockSnapshotSM sm;
  TestLogStorage log;
  log.last_index = 1000;
  log.last_term = 8;

  SnapshotManager mgr(dir_, &sm, &log);
  ASSERT_TRUE(mgr.CreateSnapshot());

  // Verify metadata.
  EXPECT_EQ(1000u, mgr.meta().index);
  EXPECT_EQ(8u, mgr.meta().term);
  EXPECT_GT(mgr.meta().timestamp_ms, 0u);
}

TEST_F(SnapshotManagerTest, ScheduleCreateIfNeededBelowThreshold) {
  MockSnapshotSM sm;
  TestLogStorage log;
  log.last_index = 50000;
  log.last_term = 1;

  SnapshotManager mgr(dir_, &sm, &log);
  mgr.set_log_gap(100000);

  // 50000 < 100000, should not trigger.
  EXPECT_FALSE(mgr.ScheduleCreateIfNeeded());
  EXPECT_EQ(0, sm.snapshot_call_count);
}

TEST_F(SnapshotManagerTest, ScheduleCreateIfNeededAboveThreshold) {
  MockSnapshotSM sm;
  TestLogStorage log;
  log.last_index = 150000;
  log.last_term = 1;

  SnapshotManager mgr(dir_, &sm, &log);
  mgr.set_log_gap(100000);

  // 150000 - 0 >= 100000, should trigger.
  EXPECT_TRUE(mgr.ScheduleCreateIfNeeded());
  EXPECT_EQ(1, sm.snapshot_call_count);
  EXPECT_EQ(150000u, mgr.meta().index);
}

TEST_F(SnapshotManagerTest, ScheduleDoesNotTriggerAfterSnapshot) {
  MockSnapshotSM sm;
  TestLogStorage log;
  log.last_index = 150000;
  log.last_term = 1;

  SnapshotManager mgr(dir_, &sm, &log);
  mgr.set_log_gap(100000);

  // Trigger snapshot at index 150000.
  ASSERT_TRUE(mgr.ScheduleCreateIfNeeded());
  EXPECT_EQ(1, sm.snapshot_call_count);
  EXPECT_EQ(150000u, mgr.meta().index);

  // Advance to 200000, gap = 50000 < 100000, should not trigger.
  log.last_index = 200000;
  EXPECT_FALSE(mgr.ScheduleCreateIfNeeded());

  // Advance to 260000, gap = 110000 >= 100000, trigger.
  log.last_index = 260000;
  EXPECT_TRUE(mgr.ScheduleCreateIfNeeded());
  EXPECT_EQ(2, sm.snapshot_call_count);
  EXPECT_EQ(260000u, mgr.meta().index);
}

TEST_F(SnapshotManagerTest, ConfigurableLogGap) {
  MockSnapshotSM sm;
  TestLogStorage log;
  log.last_index = 50;

  SnapshotManager mgr(dir_, &sm, &log);
  mgr.set_log_gap(25);

  // 50 - 0 >= 25, trigger.
  EXPECT_TRUE(mgr.ScheduleCreateIfNeeded());
  EXPECT_EQ(1, sm.snapshot_call_count);
}

TEST_F(SnapshotManagerTest, MetadataSurvivesRestart) {
  MockSnapshotSM sm;
  TestLogStorage log;
  log.last_index = 500;
  log.last_term = 3;

  {
    SnapshotManager mgr(dir_, &sm, &log);
    ASSERT_TRUE(mgr.CreateSnapshot());
    EXPECT_EQ(500u, mgr.meta().index);
    EXPECT_EQ(3u, mgr.meta().term);
  }

  // New manager loads existing metadata.
  SnapshotManager mgr2(dir_, &sm, &log);
  EXPECT_EQ(500u, mgr2.meta().index);
  EXPECT_EQ(3u, mgr2.meta().term);
}

TEST_F(SnapshotManagerTest, NoStateMachineNoCrash) {
  TestLogStorage log;
  log.last_index = 100;

  SnapshotManager mgr(dir_, nullptr, &log);
  EXPECT_FALSE(mgr.CreateSnapshot());
  EXPECT_FALSE(mgr.ScheduleCreateIfNeeded());
}

TEST_F(SnapshotManagerTest, NoLogStorageNoCrash) {
  MockSnapshotSM sm;
  SnapshotManager mgr(dir_, &sm, nullptr);
  EXPECT_FALSE(mgr.CreateSnapshot());
  EXPECT_FALSE(mgr.ScheduleCreateIfNeeded());
}

// Barrier integration: SnapshotManager uses barrier during CreateSnapshot.
TEST_F(SnapshotManagerTest, CreateSnapshotUsesBarrier) {
  MockSnapshotSM sm;
  TestLogStorage log;
  log.last_index = 1000;
  log.last_term = 5;

  SnapshotManager mgr(dir_, &sm, &log);
  mgr.barrier().BeginRead();  // Simulate an in-flight write.

  // Schedule CreateSnapshot on a separate fiber (would block on BeginWrite).
  bool snapshot_done = false;
  util::fb2::Fiber snap_fiber("snap", [&] {
    ASSERT_TRUE(mgr.CreateSnapshot());
    snapshot_done = true;
  });

  util::ThisFiber::SleepFor(std::chrono::milliseconds(10));
  EXPECT_FALSE(snapshot_done);  // Snapshot blocked by the outstanding read.

  // Complete the outstanding read → snapshot proceeds.
  mgr.barrier().EndRead();
  util::ThisFiber::SleepFor(std::chrono::milliseconds(10));
  EXPECT_TRUE(snapshot_done);

  snap_fiber.Join();
}

}  // namespace dfly
