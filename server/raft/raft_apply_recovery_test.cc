// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_node.h"

#include <gmock/gmock.h>

#include <absl/container/flat_hash_map.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "base/gtest.h"
#include "server/raft/apply_progress.h"
#include "server/raft/command_log.h"
#include "server/raft/command_encoder.h"
#include "server/raft/replicated_command.h"
#include "server/raft/segment_log_storage.h"
#include "server/service/command_registry.h"
#include "server/state_machine/state_machine.h"

namespace dfly {

using namespace testing;

// --- In-memory KV store for integration testing ---

class InMemoryKV : public IStateMachine {
 public:
  std::vector<LogEntry> applied;

  ApplyResult Apply(const CommandId*, CmdArgList) override {
    return {ApplyOp::OK, 0};
  }

  ApplyResult ApplyLogEntry(const LogEntry& entry) override {
    applied.push_back(entry);
    std::string_view cmd = entry.command;
    auto space1 = cmd.find(' ');
    if (space1 == std::string_view::npos)
      return {ApplyOp::ERROR, 0};
    std::string_view name = cmd.substr(0, space1);
    if (name == "SET") {
      auto space2 = cmd.find(' ', space1 + 1);
      if (space2 == std::string_view::npos)
        return {ApplyOp::ERROR, 0};
      std::string_view key = cmd.substr(space1 + 1, space2 - space1 - 1);
      std::string_view val = cmd.substr(space2 + 1);
      Set(0, key, val);
      return {ApplyOp::OK, 1};
    }
    if (name == "DEL") {
      bool deleted = Del(0, cmd.substr(space1 + 1));
      return {ApplyOp::OK, deleted ? 1u : 0u};
    }
    return {ApplyOp::ERROR, 0};
  }

  void Set(DbIndex, std::string_view key, std::string_view val) override {
    store_[std::string(key)] = std::string(val);
  }

  bool Del(DbIndex, std::string_view key) override {
    return store_.erase(std::string(key)) > 0;
  }

  bool Expire(DbIndex, std::string_view, uint64_t) override {
    return false;
  }

  OpResult<std::string> Get(DbIndex, std::string_view key) override {
    auto it = store_.find(std::string(key));
    if (it != store_.end())
      return it->second;
    return OpStatus::KEY_NOTFOUND;
  }

  size_t DbSize(DbIndex) const override {
    return store_.size();
  }

  void Schedule(DbIndex, std::string_view,
                std::function<void(EngineShard*)>) override {}

 private:
  absl::flat_hash_map<std::string, std::string> store_;
};

// --- Helpers ---

std::vector<MutableStrSpan> MakeCmdArgs(std::initializer_list<const char*> args) {
  std::vector<MutableStrSpan> result;
  for (auto* s : args) {
    result.emplace_back(const_cast<char*>(s), strlen(s));
  }
  return result;
}

// Writes apply.meta with the given last_applied value.
void WriteApplyMeta(const std::string& dir, LogIndex last_applied) {
  std::string path = dir + "/apply.meta";
  std::ofstream ofs(path);
  ofs << "{\"last_applied\":" << last_applied << "}\n";
}

// ============================================================
// ApplyProgress Unit Tests
// ============================================================

class ApplyProgressTest : public Test {
};

TEST_F(ApplyProgressTest, DefaultState) {
  ApplyProgress ap;
  EXPECT_EQ(0u, ap.LastApplied());
}

TEST_F(ApplyProgressTest, UpdateAndQuery) {
  ApplyProgress ap;
  ap.Update(5);
  EXPECT_EQ(5u, ap.LastApplied());
}

TEST_F(ApplyProgressTest, Monotonic) {
  ApplyProgress ap;
  ap.Update(10);
  ap.Update(20);
  EXPECT_EQ(20u, ap.LastApplied());
}

class ApplyProgressPersistenceTest : public Test {
 protected:
  void SetUp() override {
    path_ = "/tmp/apply_progress_test_" + std::to_string(getpid()) + ".meta";
  }

  void TearDown() override {
    unlink(path_.c_str());
    unlink((path_ + ".tmp").c_str());
  }

  std::string ReadRawFile() const {
    std::ifstream ifs(path_);
    if (!ifs.is_open())
      return "";
    std::ostringstream oss;
    oss << ifs.rdbuf();
    return oss.str();
  }

  std::string path_;
};

TEST_F(ApplyProgressPersistenceTest, PersistLastApplied) {
  ApplyProgress ap(path_);
  ASSERT_TRUE(ap.Load());
  EXPECT_EQ(0u, ap.LastApplied());

  ap.Update(15);
  EXPECT_EQ(15u, ap.LastApplied());

  ApplyProgress ap2(path_);
  EXPECT_TRUE(ap2.Load());
  EXPECT_EQ(15u, ap2.LastApplied());
}

TEST_F(ApplyProgressPersistenceTest, LoadNonExistent) {
  unlink(path_.c_str());
  ApplyProgress ap(path_);
  EXPECT_TRUE(ap.Load());
  EXPECT_EQ(0u, ap.LastApplied());
}

TEST_F(ApplyProgressPersistenceTest, JsonFormat) {
  ApplyProgress ap(path_);
  ASSERT_TRUE(ap.Load());
  ap.Update(42);

  std::string content = ReadRawFile();
  EXPECT_THAT(content, HasSubstr("\"last_applied\":42"));
}

TEST_F(ApplyProgressPersistenceTest, LoadExistingFile) {
  {
    std::ofstream ofs(path_);
    ofs << "{\"last_applied\":99}\n";
  }

  ApplyProgress ap(path_);
  EXPECT_TRUE(ap.Load());
  EXPECT_EQ(99u, ap.LastApplied());
}

TEST_F(ApplyProgressPersistenceTest, RestartRecovery) {
  {
    ApplyProgress ap(path_);
    ASSERT_TRUE(ap.Load());
    ap.Update(5);
  }
  {
    ApplyProgress ap(path_);
    EXPECT_TRUE(ap.Load());
    EXPECT_EQ(5u, ap.LastApplied());
  }
  {
    ApplyProgress ap(path_);
    EXPECT_TRUE(ap.Load());
    EXPECT_EQ(5u, ap.LastApplied());
    ap.Update(10);
  }
  ApplyProgress ap(path_);
  EXPECT_TRUE(ap.Load());
  EXPECT_EQ(10u, ap.LastApplied());
}

// ============================================================
// Integration Tests: RaftNode + ApplyProgress + Replay
// ============================================================

class RaftApplyRecoveryTest : public Test {
 protected:
  void SetUp() override {
    dir_ = "/tmp/raft_apply_recovery_test_" + std::to_string(getpid());
    mkdir(dir_.c_str(), 0755);
  }

  void TearDown() override {
    unlink((dir_ + "/apply.meta").c_str());
    unlink((dir_ + "/apply.meta.tmp").c_str());
    unlink((dir_ + "/meta.json").c_str());
    unlink((dir_ + "/meta.json.tmp").c_str());
    rmdir(dir_.c_str());
  }

  // Appends count entries with sequential "SET key{i} value{i}" commands to log.
  void PopulateLog(CommandLog& log, size_t count) {
    CommandId set_cmd("SET", CO::WRITE, 3, 1, 1, 1);
    for (size_t i = 1; i <= count; i++) {
      auto args_vec = MakeCmdArgs({"SET", ("k" + std::to_string(i)).c_str(),
                                   ("v" + std::to_string(i)).c_str()});
      CmdArgList args{args_vec.data(), args_vec.size()};
      auto encoded = CommandEncoder::Encode(&set_cmd, args);
      CHECK(encoded.has_value());
      LogEntry entry(1, 0, encoded->Serialize());
      log.Append(entry);
    }
  }

  void VerifyKVState(InMemoryKV& kv, LogIndex start, LogIndex end) {
    size_t expected = (end >= start) ? (end - start + 1) : 0;
    EXPECT_EQ(expected, kv.applied.size());
    EXPECT_EQ(expected, kv.DbSize(0));
    for (LogIndex i = start; i <= end; i++) {
      auto result = kv.Get(0, "k" + std::to_string(i));
      ASSERT_TRUE(result) << "key k" << i << " not found";
      EXPECT_EQ("v" + std::to_string(i), result.value())
          << "value mismatch for k" << i;
    }
  }

  std::string dir_;
};

// Acceptance Criterion 1: All logs applied before crash, nothing to replay.
TEST_F(RaftApplyRecoveryTest, AllAppliedNoReplay) {
  WriteApplyMeta(dir_, 100);

  CommandLog log;
  PopulateLog(log, 100);
  EXPECT_EQ(100u, log.LastIndex());

  InMemoryKV kv;
  RaftNode node("N1");
  node.SetLogStorage(&log);
  node.SetStateMachine(&kv);
  node.SetStoragePath(dir_);
  node.ReplayUnappliedLogs();

  // last_applied == last_index == 100 → nothing to replay.
  EXPECT_EQ(0u, kv.applied.size());
  EXPECT_EQ(100u, node.last_applied());
  EXPECT_EQ(100u, node.apply_progress().LastApplied());
}

// Acceptance Criterion 2: Apply 50 out of 100, crash, replay 51..100.
TEST_F(RaftApplyRecoveryTest, PartialApplyThenReplay) {
  WriteApplyMeta(dir_, 50);

  CommandLog log;
  PopulateLog(log, 100);

  InMemoryKV kv;
  RaftNode node("N1");
  node.SetLogStorage(&log);
  node.SetStateMachine(&kv);
  node.SetStoragePath(dir_);
  node.ReplayUnappliedLogs();

  // Replayed 51..100 (50 entries).
  VerifyKVState(kv, 51, 100);
  EXPECT_EQ(100u, node.last_applied());
  EXPECT_EQ(100u, node.apply_progress().LastApplied());
}

// Acceptance Criterion 3: After recovery, last_applied matches log size.
TEST_F(RaftApplyRecoveryTest, LastAppliedMatchesAfterRecovery) {
  WriteApplyMeta(dir_, 75);

  CommandLog log;
  PopulateLog(log, 75);

  InMemoryKV kv;
  RaftNode node("N1");
  node.SetLogStorage(&log);
  node.SetStateMachine(&kv);
  node.SetStoragePath(dir_);
  node.ReplayUnappliedLogs();

  // last_applied == last_index → nothing replayed.
  EXPECT_EQ(0u, kv.applied.size());
  EXPECT_EQ(75u, node.last_applied());
  EXPECT_EQ(75u, node.apply_progress().LastApplied());
}

// Acceptance Criterion 4: Database state consistent after recovery.
TEST_F(RaftApplyRecoveryTest, KvsStateConsistentAfterRecovery) {
  WriteApplyMeta(dir_, 25);

  CommandLog log;
  PopulateLog(log, 50);

  InMemoryKV kv;
  RaftNode node("N1");
  node.SetLogStorage(&log);
  node.SetStateMachine(&kv);
  node.SetStoragePath(dir_);
  node.ReplayUnappliedLogs();

  // Replayed 26..50 (25 entries).
  VerifyKVState(kv, 26, 50);
  EXPECT_EQ(50u, node.last_applied());
  EXPECT_EQ(50u, node.apply_progress().LastApplied());
}

// Acceptance Criterion 5: Repeated restart cycles maintain state.
TEST_F(RaftApplyRecoveryTest, RepeatedRestartCycles) {
  // Cycle 1: 30 entries, 15 applied → replay 16..30.
  WriteApplyMeta(dir_, 15);
  {
    CommandLog log;
    PopulateLog(log, 30);
    InMemoryKV kv;
    RaftNode node("N1");
    node.SetLogStorage(&log);
    node.SetStateMachine(&kv);
    node.SetStoragePath(dir_);
    node.ReplayUnappliedLogs();
    VerifyKVState(kv, 16, 30);
    EXPECT_EQ(30u, node.apply_progress().LastApplied());
  }

  // Cycle 2: 50 entries, 35 applied → replay 36..50.
  WriteApplyMeta(dir_, 35);
  {
    CommandLog log;
    PopulateLog(log, 50);
    InMemoryKV kv;
    RaftNode node("N1");
    node.SetLogStorage(&log);
    node.SetStateMachine(&kv);
    node.SetStoragePath(dir_);
    node.ReplayUnappliedLogs();
    VerifyKVState(kv, 36, 50);
    EXPECT_EQ(50u, node.apply_progress().LastApplied());
  }

  // Cycle 3: caught up, nothing to replay.
  WriteApplyMeta(dir_, 50);
  {
    CommandLog log;
    PopulateLog(log, 50);
    InMemoryKV kv;
    RaftNode node("N1");
    node.SetLogStorage(&log);
    node.SetStateMachine(&kv);
    node.SetStoragePath(dir_);
    node.ReplayUnappliedLogs();
    EXPECT_EQ(0u, kv.applied.size());
    EXPECT_EQ(50u, node.apply_progress().LastApplied());
  }
}

// --- Edge cases ---

TEST_F(RaftApplyRecoveryTest, NoStoragePath) {
  CommandLog log;
  PopulateLog(log, 10);

  InMemoryKV kv;
  RaftNode node("N1");
  node.SetLogStorage(&log);
  node.SetStateMachine(&kv);
  // No SetStoragePath — in-memory mode, last_applied starts at 0.
  // ReplayUnappliedLogs sets commit_index=10, applies 1..10.
  node.ReplayUnappliedLogs();

  VerifyKVState(kv, 1, 10);
  EXPECT_EQ(10u, node.last_applied());
  // In-memory ApplyProgress should also be updated.
  EXPECT_EQ(10u, node.apply_progress().LastApplied());
}

TEST_F(RaftApplyRecoveryTest, NoLogStorage) {
  WriteApplyMeta(dir_, 10);

  RaftNode node("N1");
  node.SetStoragePath(dir_);
  // No SetLogStorage — no-op.
  node.ReplayUnappliedLogs();
  EXPECT_EQ(10u, node.last_applied());
  EXPECT_EQ(10u, node.apply_progress().LastApplied());
}

}  // namespace dfly
