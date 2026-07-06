#include <gmock/gmock.h>

#include <absl/container/flat_hash_map.h>

#include <cstring>
#include <string>
#include <vector>

#include "base/gtest.h"
#include "base/logging.h"
#include "server/raft/command_encoder.h"
#include "server/raft/command_log.h"
#include "server/raft/raft_node.h"
#include "server/raft/replicated_command.h"
#include "server/service/command_registry.h"
#include "server/state_machine/state_machine.h"

namespace dfly {

using namespace testing;
using namespace std;

// In-memory KV store implementing IStateMachine for integration testing.
// Parses serialized log entries (e.g. "SET a 1") and applies to a local map.
class InMemoryKV : public IStateMachine {
 public:
  ApplyResult Apply(const CommandId*, CmdArgList) override {
    return {ApplyOp::OK, 0};
  }

  ApplyResult ApplyLogEntry(const LogEntry& entry) override {
    string_view cmd = entry.command;
    auto space1 = cmd.find(' ');
    if (space1 == string_view::npos)
      return {ApplyOp::ERROR, 0};

    string_view name = cmd.substr(0, space1);
    if (name == "SET") {
      auto space2 = cmd.find(' ', space1 + 1);
      if (space2 == string_view::npos)
        return {ApplyOp::ERROR, 0};
      string_view key = cmd.substr(space1 + 1, space2 - space1 - 1);
      string_view val = cmd.substr(space2 + 1);
      Set(0, key, val);
      return {ApplyOp::OK, 1};
    }
    if (name == "DEL") {
      bool deleted = Del(0, cmd.substr(space1 + 1));
      return {ApplyOp::OK, deleted ? 1u : 0u};
    }
    return {ApplyOp::ERROR, 0};
  }

  void Set(DbIndex, string_view key, string_view val) override {
    store_[string(key)] = string(val);
  }

  bool Del(DbIndex, string_view key) override {
    return store_.erase(string(key)) > 0;
  }

  bool Expire(DbIndex, string_view, uint64_t) override {
    return false;
  }

  OpResult<string> Get(DbIndex, string_view key, ReadConsistency) override {
    auto it = store_.find(string(key));
    if (it != store_.end())
      return it->second;
    return OpStatus::KEY_NOTFOUND;
  }

  size_t DbSize(DbIndex) const override {
    return store_.size();
  }

  void Schedule(DbIndex, string_view, function<void(EngineShard*)>) override {}

 private:
  absl::flat_hash_map<string, string> store_;
};

std::vector<MutableStrSpan> MakeCmdArgs(initializer_list<const char*> args) {
  std::vector<MutableStrSpan> result;
  for (auto* s : args) {
    result.emplace_back(const_cast<char*>(s), strlen(s));
  }
  return result;
}

class RaftIntegrationTest : public Test {
};

TEST_F(RaftIntegrationTest, SetCommandApply) {
  CommandLog log;
  InMemoryKV kv;
  RaftNode node("N1");
  node.SetLogStorage(&log);
  node.SetStateMachine(&kv);
  node.BecomeCandidate();
  node.BecomeLeader();

  CommandId set_cmd("SET", CO::WRITE, 3, 1, 1, 1);
  auto args_vec = MakeCmdArgs({"SET", "a", "1"});
  CmdArgList args{args_vec.data(), args_vec.size()};

  auto encoded = CommandEncoder::Encode(&set_cmd, args);
  ASSERT_TRUE(encoded.has_value());
  EXPECT_EQ("SET a 1", encoded->Serialize());

  LogEntry entry(node.term(), 0, encoded->Serialize());
  log.Append(entry);

  node.AdvanceCommitIndex();
  node.ApplyCommittedLogs();

  EXPECT_EQ(1u, node.commit_index());
  EXPECT_EQ(1u, node.last_applied());

  auto result = kv.Get(0, "a", ReadConsistency::kLocal);
  ASSERT_TRUE(result);
  EXPECT_EQ("1", result.value());

  // Unset key returns NOTFOUND
  auto missing = kv.Get(0, "nonexistent", ReadConsistency::kLocal);
  EXPECT_EQ(OpStatus::KEY_NOTFOUND, missing.status());
}

TEST_F(RaftIntegrationTest, MultipleCommandsApply) {
  CommandLog log;
  InMemoryKV kv;
  RaftNode node("N1");
  node.SetLogStorage(&log);
  node.SetStateMachine(&kv);
  node.BecomeCandidate();
  node.BecomeLeader();

  CommandId set_cmd("SET", CO::WRITE, 3, 1, 1, 1);

  // Append three commands
  auto append = [&](string_view key, string_view val) {
    auto args_vec = MakeCmdArgs({"SET", key.data(), val.data()});
    CmdArgList args{args_vec.data(), args_vec.size()};
    auto encoded = CommandEncoder::Encode(&set_cmd, args);
    CHECK(encoded.has_value());
    LogEntry entry(node.term(), 0, encoded->Serialize());
    log.Append(entry);
  };

  append("a", "1");
  append("b", "2");
  append("c", "3");

  EXPECT_EQ(3u, log.LogSize());

  node.AdvanceCommitIndex();
  EXPECT_EQ(3u, node.commit_index());

  node.ApplyCommittedLogs();
  EXPECT_EQ(3u, node.last_applied());

  // Verify all keys
  auto va = kv.Get(0, "a", ReadConsistency::kLocal);
  ASSERT_TRUE(va);
  EXPECT_EQ("1", va.value());

  auto vb = kv.Get(0, "b", ReadConsistency::kLocal);
  ASSERT_TRUE(vb);
  EXPECT_EQ("2", vb.value());

  auto vc = kv.Get(0, "c", ReadConsistency::kLocal);
  ASSERT_TRUE(vc);
  EXPECT_EQ("3", vc.value());

  // DbSize reflects applied entries
  EXPECT_EQ(3u, kv.DbSize(0));
}

}  // namespace dfly
