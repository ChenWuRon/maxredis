// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_node.h"

#include <gmock/gmock.h>

#include <absl/container/flat_hash_map.h>

#include <chrono>
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
#include "server/raft/snapshot_meta.h"
#include "server/raft/snapshot_writer.h"
#include "server/raft/wal_writer.h"
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

  OpResult<std::string> Get(DbIndex, std::string_view key, ReadConsistency) override {
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

  bool SaveSnapshot(const std::string& path) override {
    SnapshotWriter writer(path);
    if (!writer.Open())
      return false;
    for (const auto& [k, v] : store_) {
      if (!writer.Add({k, v, 0}))
        return false;
    }
    return writer.Finalize(store_.size());
  }

  bool LoadSnapshot(const std::string& path) override {
    FILE* fp = fopen(path.c_str(), "rb");
    if (!fp)
      return false;
    uint32_t magic;
    if (fread(&magic, sizeof(magic), 1, fp) != 1 || magic != kSnapshotMagic) {
      fclose(fp);
      return false;
    }
    uint32_t num_records;
    if (fread(&num_records, sizeof(num_records), 1, fp) != 1) {
      fclose(fp);
      return false;
    }
    store_.clear();
    for (uint32_t i = 0; i < num_records; i++) {
      uint32_t key_len, value_len;
      uint64_t expire_at;
      if (fread(&key_len, sizeof(key_len), 1, fp) != 1) break;
      if (fread(&value_len, sizeof(value_len), 1, fp) != 1) break;
      if (fread(&expire_at, sizeof(expire_at), 1, fp) != 1) break;
      std::string key(key_len, '\0');
      if (key_len > 0 && fread(key.data(), 1, key_len, fp) != key_len) break;
      std::string value(value_len, '\0');
      if (value_len > 0 && fread(value.data(), 1, value_len, fp) != value_len) break;
      store_[key] = value;
    }
    fclose(fp);
    return true;
  }

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
    unlink((dir_ + "/snapshot.meta").c_str());
    unlink((dir_ + "/snapshot.bin").c_str());
    rmdir(dir_.c_str());
  }

  // Writes snapshot.meta with the given index/term.
  void WriteSnapshotMeta(LogIndex index, Term term) {
    SnapshotMetaStorage sms(dir_ + "/snapshot.meta");
    sms.Load();
    sms.SetMeta({index, term, 100});
  }

  // Creates a snapshot.bin with the given entries.
  void WriteSnapshotBin(const std::vector<SnapshotRecord>& records) {
    SnapshotWriter writer(dir_ + "/snapshot.bin");
    CHECK(writer.Open());
    for (const auto& r : records)
      CHECK(writer.Add(r));
    CHECK(writer.Finalize(records.size()));
  }

  // Writes a WAL segment with "SET key_{idx} val_{idx}" commands for use with InMemoryKV.
  void WriteSegmentWithCommands(uint32_t segment_id, uint32_t count,
                                LogIndex start_index, Term term) {
    char buf[256];
    snprintf(buf, sizeof(buf), "%s/segment_%08lu.log", dir_.c_str(),
             static_cast<unsigned long>(segment_id));
    WalWriter writer;
    CHECK(writer.Open(buf));
    for (uint32_t i = 0; i < count; i++) {
      LogIndex idx = start_index + i;
      LogEntry entry;
      entry.term = term;
      entry.index = idx;
      entry.command = "SET key_" + std::to_string(idx) + " val_" + std::to_string(idx);
      writer.Append(entry);
    }
    CHECK(writer.Flush());
    writer.Close();
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
      auto result = kv.Get(0, "k" + std::to_string(i), ReadConsistency::kLocal);
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

// --- Snapshot recovery tests ---

TEST_F(RaftApplyRecoveryTest, BootstrapWithSnapshot) {
  WriteSnapshotMeta(500000, 17);
  WriteSnapshotBin({{"k1", "v1", 0}, {"k2", "v2", 0}});

  InMemoryKV kv;
  RaftNode node("N1");
  node.SetStateMachine(&kv);
  node.SetStoragePath(dir_);
  node.ReplayUnappliedLogs();

  // Snapshot should have been loaded during SetStoragePath.
  EXPECT_EQ(500000u, node.last_snapshot_index());
  EXPECT_EQ(17u, node.last_snapshot_term());
  EXPECT_EQ(500000u, node.last_applied());
  EXPECT_EQ(2u, kv.DbSize(0));
  EXPECT_EQ("v1", kv.Get(0, "k1", ReadConsistency::kLocal).value());
  EXPECT_EQ("v2", kv.Get(0, "k2", ReadConsistency::kLocal).value());
}

TEST_F(RaftApplyRecoveryTest, SnapshotIndexDominatesApplyMeta) {
  // apply.meta says 10, but snapshot is at 500000.
  WriteApplyMeta(dir_, 10);
  WriteSnapshotMeta(500000, 17);
  WriteSnapshotBin({{"k1", "v1", 0}});

  InMemoryKV kv;
  RaftNode node("N1");
  node.SetStateMachine(&kv);
  node.SetStoragePath(dir_);

  // last_applied_ should be max(apply.meta, snapshot.index).
  EXPECT_EQ(500000u, node.last_applied());
  EXPECT_EQ(500000u, node.last_snapshot_index());
  EXPECT_EQ(17u, node.last_snapshot_term());
}

TEST_F(RaftApplyRecoveryTest, NoSnapshotWithoutStateMachine) {
  WriteSnapshotMeta(500000, 17);
  WriteSnapshotBin({{"k1", "v1", 0}});

  RaftNode node("N1");
  // No SetStateMachine → snapshot loading skipped in SetStoragePath.
  node.SetStoragePath(dir_);
  EXPECT_EQ(0u, node.last_snapshot_index());
  EXPECT_EQ(0u, node.last_snapshot_term());
  EXPECT_EQ(0u, node.last_applied());
}

TEST_F(RaftApplyRecoveryTest, SnapshotRecoveryWithWALReplay) {
  WriteSnapshotMeta(500000, 17);
  WriteSnapshotBin({{"k_snap", "v_snap", 0}});

  CommandLog log;
  PopulateLog(log, 10);  // entries 1..10

  InMemoryKV kv;
  RaftNode node("N1");
  node.SetLogStorage(&log);
  node.SetStateMachine(&kv);
  node.SetStoragePath(dir_);

  // SetStoragePath loaded snapshot → last_applied = 500000.
  EXPECT_EQ(500000u, node.last_applied());

  // WAL has entries 1..10, all below last_applied → nothing to replay.
  node.ReplayUnappliedLogs();
  EXPECT_EQ(500000u, node.last_applied());
  EXPECT_EQ("v_snap", kv.Get(0, "k_snap", ReadConsistency::kLocal).value());
  // WAL entries were not replayed since they're before the snapshot.
  EXPECT_EQ(0u, kv.applied.size());
}

TEST_F(RaftApplyRecoveryTest, BootstrapWithSnapshotNoSnapshotDir) {
  // No snapshot files exist, just apply.meta.
  WriteApplyMeta(dir_, 42);

  InMemoryKV kv;
  RaftNode node("N1");
  node.SetStateMachine(&kv);
  node.SetStoragePath(dir_);

  EXPECT_EQ(42u, node.last_applied());
  EXPECT_EQ(0u, node.last_snapshot_index());
  EXPECT_EQ(0u, node.last_snapshot_term());
  EXPECT_EQ(0u, kv.DbSize(0));
}

// --- Delta replay after snapshot recovery ---

TEST_F(RaftApplyRecoveryTest, DeltaReplayAfterSnapshot) {
  // Setup: snapshot at 1000, WAL has 2000 entries.
  // After recovery, only entries 1001..2000 should be replayed (1000 entries).
  WriteSnapshotMeta(1000, 5);
  WriteSnapshotBin({{"k_snap", "v_snap", 0}});

  CommandLog log;
  PopulateLog(log, 2000);

  InMemoryKV kv;
  RaftNode node("N1");
  node.SetLogStorage(&log);
  node.SetStateMachine(&kv);
  node.SetStoragePath(dir_);
  node.ReplayUnappliedLogs();

  // Verify snapshot data is present.
  EXPECT_EQ("v_snap", kv.Get(0, "k_snap", ReadConsistency::kLocal).value());

  // Only entries 1001..2000 should have been replayed (1000 entries).
  // After replay, last_applied advances to 2000.
  EXPECT_EQ(2000u, node.last_applied());
  EXPECT_EQ(1000u, kv.applied.size());

  // Verify first and last replayed entries.
  EXPECT_EQ("v1001", kv.Get(0, "k1001", ReadConsistency::kLocal).value());
  EXPECT_EQ("v2000", kv.Get(0, "k2000", ReadConsistency::kLocal).value());

  // Entries before the snapshot should NOT be present.
  EXPECT_FALSE(kv.Get(0, "k999", ReadConsistency::kLocal).ok());
}

TEST_F(RaftApplyRecoveryTest, DeltaReplayOnlyUnappliedPortion) {
  // 1500 entries in WAL, snapshot at 500, apply.meta at 300.
  // SetStoragePath: last_applied = max(300, 500) = 500.
  // Replay should cover entries 501..1500 (1000 entries).
  WriteApplyMeta(dir_, 300);
  WriteSnapshotMeta(500, 3);
  WriteSnapshotBin({{"k_snap", "v_snap", 0}});

  CommandLog log;
  PopulateLog(log, 1500);

  InMemoryKV kv;
  RaftNode node("N1");
  node.SetLogStorage(&log);
  node.SetStateMachine(&kv);
  node.SetStoragePath(dir_);

  EXPECT_EQ(500u, node.last_applied());
  EXPECT_EQ(500u, node.last_snapshot_index());

  node.ReplayUnappliedLogs();
  EXPECT_EQ(1500u, node.last_applied());
  EXPECT_EQ(1000u, kv.applied.size());  // 501..1500
  EXPECT_EQ("v1500", kv.Get(0, "k1500", ReadConsistency::kLocal).value());
}

TEST_F(RaftApplyRecoveryTest, DeltaReplayNothingToReplay) {
  // WAL fully covered by snapshot.
  WriteSnapshotMeta(2000, 7);
  WriteSnapshotBin({});

  CommandLog log;
  PopulateLog(log, 500);

  InMemoryKV kv;
  RaftNode node("N1");
  node.SetLogStorage(&log);
  node.SetStateMachine(&kv);
  node.SetStoragePath(dir_);
  node.ReplayUnappliedLogs();

  // Everything is before the snapshot — nothing to replay.
  EXPECT_EQ(2000u, node.last_applied());
  EXPECT_EQ(0u, kv.applied.size());
}

TEST_F(RaftApplyRecoveryTest, DeltaReplayFullWalAfterSnapshot) {
  // WAL starts after the snapshot index.
  WriteSnapshotMeta(3000, 10);
  WriteSnapshotBin({{"k_old", "v_old", 0}});

  CommandLog log;
  PopulateLog(log, 1000);

  InMemoryKV kv;
  RaftNode node("N1");
  node.SetLogStorage(&log);
  node.SetStateMachine(&kv);
  node.SetStoragePath(dir_);
  node.ReplayUnappliedLogs();

  // last_applied was 3000 after snapshot, log has 1000 entries (indices 1..1000).
  // All log entries are before the snapshot → nothing replayed.
  EXPECT_EQ(3000u, node.last_applied());
  EXPECT_EQ(0u, kv.applied.size());
  EXPECT_EQ("v_old", kv.Get(0, "k_old", ReadConsistency::kLocal).value());
}

// --- End-to-end snapshot recovery ---

TEST_F(RaftApplyRecoveryTest, EndToEndSnapshotRecovery) {
  constexpr LogIndex kSnapshotIndex = 10000;
  constexpr LogIndex kDeltaWrites = 1000;
  constexpr LogIndex kTotalEntries = kSnapshotIndex + kDeltaWrites;
  constexpr Term kPreSnapTerm = 3;
  constexpr Term kSnapTerm = 5;

  // Phase 1: Create persistent WAL segments.
  // Pre-snapshot entries (indices 1..10000, term=3).
  WriteSegmentWithCommands(1, kSnapshotIndex, 1, kPreSnapTerm);
  // Post-snapshot entries (indices 10001..11000, term=5).
  WriteSegmentWithCommands(2, kDeltaWrites, kSnapshotIndex + 1, kSnapTerm);

  // Phase 2: Create snapshot at index 10000, containing every 10th key.
  WriteSnapshotMeta(kSnapshotIndex, kSnapTerm);
  std::vector<SnapshotRecord> snap_records;
  for (LogIndex i = 10; i <= kSnapshotIndex; i += 10) {
    snap_records.push_back({"snap_key_" + std::to_string(i),
                            "snap_val_" + std::to_string(i), 0});
  }
  WriteSnapshotBin(snap_records);

  // Phase 3: Set apply.meta to match snapshot index.
  WriteApplyMeta(dir_, kSnapshotIndex);

  // Phase 4: Recover from persistent WAL.
  SegmentLogStorage log(dir_);
  ASSERT_TRUE(log.Open());
  ASSERT_EQ(static_cast<size_t>(kTotalEntries), log.LastIndex());

  InMemoryKV kv;
  RaftNode node("N1");
  node.SetLogStorage(&log);
  node.SetStateMachine(&kv);

  auto t_start = std::chrono::steady_clock::now();
  node.SetStoragePath(dir_);
  auto t_snap = std::chrono::steady_clock::now();
  node.ReplayUnappliedLogs();
  auto t_end = std::chrono::steady_clock::now();

  // Phase 5: Verify recovery state.

  // Snapshot metadata recovered.
  EXPECT_EQ(kSnapTerm, node.last_snapshot_term());
  EXPECT_EQ(kSnapshotIndex, node.last_snapshot_index());
  EXPECT_EQ(kTotalEntries, node.last_applied());

  // Snapshot keys present.
  for (LogIndex i = 10; i <= kSnapshotIndex; i += 10) {
    auto res = kv.Get(0, "snap_key_" + std::to_string(i), ReadConsistency::kLocal);
    ASSERT_TRUE(res.ok()) << "missing snap key " << i;
    EXPECT_EQ("snap_val_" + std::to_string(i), res.value());
  }

  // Pre-snapshot keys NOT present (they were not included in the snapshot).
  // Key 5 was not in the snapshot's every-10th selection.
  auto pre_snap = kv.Get(0, "key_5", ReadConsistency::kLocal);
  EXPECT_FALSE(pre_snap.ok()) << "pre-snapshot key 5 should not exist";

  // Delta keys (post-snapshot WAL) all present.
  for (LogIndex i = kSnapshotIndex + 1; i <= kTotalEntries; i++) {
    auto res = kv.Get(0, "key_" + std::to_string(i), ReadConsistency::kLocal);
    ASSERT_TRUE(res.ok()) << "missing delta key " << i;
    EXPECT_EQ("val_" + std::to_string(i), res.value());
  }

  // Metrics.
  auto recovery_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
  auto snapshot_ms = std::chrono::duration<double, std::milli>(t_snap - t_start).count();
  auto wal_ms = std::chrono::duration<double, std::milli>(t_end - t_snap).count();
  LOG(INFO) << "Recovery metrics: total=" << recovery_ms << "ms"
            << " snapshot_load=" << snapshot_ms << "ms"
            << " wal_replay=" << wal_ms << "ms";
  // Verify recovery completed within reasonable time (10s for 11K entries).
  EXPECT_LT(recovery_ms, 10000);
}

}  // namespace dfly
