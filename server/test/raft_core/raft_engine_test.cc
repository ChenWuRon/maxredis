#include "server/raft/raft_engine.h"

#include <gmock/gmock.h>

#include <cstring>
#include <string>
#include <vector>

#include "base/gtest.h"
#include "server/raft/command_encoder.h"
#include "server/raft/replicated_command.h"
#include "server/service/command_registry.h"
#include "server/state_machine/state_machine.h"

namespace dfly {

using namespace testing;

class TestFastCommitSM : public IStateMachine {
 public:
  std::vector<LogEntry> applied;

  ApplyResult Apply(const CommandId*, CmdArgList) override {
    return {ApplyOp::OK, 0};
  }
  void Set(DbIndex, std::string_view, std::string_view) override {}
  bool Del(DbIndex, std::string_view) override { return false; }
  bool Expire(DbIndex, std::string_view, uint64_t) override { return false; }
  OpResult<std::string> Get(DbIndex, std::string_view, ReadConsistency) override {
    return OpStatus::KEY_NOTFOUND;
  }
  size_t DbSize(DbIndex) const override { return 0; }
  void Schedule(DbIndex, std::string_view,
                std::function<void(EngineShard*)>) override {}

  ApplyResult ApplyLogEntry(const LogEntry& entry) override {
    applied.push_back(entry);
    return {ApplyOp::OK, 1};
  }
};

std::vector<MutableStrSpan> MakeCmdArgs(std::initializer_list<const char*> args) {
  std::vector<MutableStrSpan> result;
  for (auto* s : args) {
    result.emplace_back(const_cast<char*>(s), strlen(s));
  }
  return result;
}

TEST(RaftEngineTest, FastCommitPathSingleNode) {
  RaftEngine engine(nullptr, nullptr);
  TestFastCommitSM sm;

  engine.group().node().SetStateMachine(&sm);
  engine.group().node().BecomeCandidate();
  engine.group().node().BecomeLeader();

  EXPECT_EQ(RaftRole::Leader, engine.group().node().role());
  EXPECT_EQ(0u, engine.log_storage()->LogSize());
  EXPECT_EQ(0u, engine.group().node().commit_index());
  EXPECT_EQ(0u, engine.group().node().last_applied());

  CommandId set_cmd("SET", CO::WRITE, 3, 1, 1, 1);
  auto args_vec = MakeCmdArgs({"SET", "a", "1"});
  CmdArgList args{args_vec.data(), args_vec.size()};

  ApplyResult result = engine.SubmitCommand(&set_cmd, args);

  // Log entry appended
  EXPECT_EQ(1u, engine.log_storage()->LogSize());
  ASSERT_NE(nullptr, engine.log_storage()->Get(1));
  EXPECT_EQ("SET a 1", engine.log_storage()->Get(1)->command);

  // Commit index advanced to last log index
  EXPECT_EQ(1u, engine.group().node().commit_index());
  EXPECT_EQ(1u, engine.group().node().last_applied());

  // State machine applied the entry
  ASSERT_EQ(1u, sm.applied.size());
  EXPECT_EQ("SET a 1", sm.applied[0].command);

  // Result is OK
  EXPECT_EQ(ApplyOp::OK, result.op);
  EXPECT_EQ(1u, result.affected_rows);
}

}  // namespace dfly
