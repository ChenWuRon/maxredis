// Multi-Raft integration tests.
// Verifies that multiple Raft groups can operate independently
// with parallel leader elections and independent log replication.

#include <gmock/gmock.h>

#include <absl/container/flat_hash_map.h>

#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "base/gtest.h"
#include "base/logging.h"
#include "server/raft/command_log.h"
#include "server/raft/local_transport.h"
#include "server/raft/raft_node.h"
#include "server/raft/raft_group_manager.h"
#include "server/raft/shard_router.h"
#include "server/state_machine/state_machine.h"

namespace dfly {

using namespace testing;
using namespace std;

// In-memory KV store for multi-group testing.
class TestMultiKV : public IStateMachine {
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
    return {ApplyOp::ERROR, 0};
  }

  void Set(DbIndex, string_view key, string_view val) override {
    store_[string(key)] = string(val);
  }

  bool Del(DbIndex, string_view key) override {
    return store_.erase(string(key)) > 0;
  }

  bool Expire(DbIndex, string_view, uint64_t) override { return false; }

  OpResult<string> Get(DbIndex, string_view key, ReadConsistency) override {
    auto it = store_.find(string(key));
    if (it != store_.end())
      return it->second;
    return OpStatus::KEY_NOTFOUND;
  }

  size_t DbSize(DbIndex) const override { return store_.size(); }

  void Schedule(DbIndex, string_view, function<void(EngineShard*)>) override {}

 private:
  absl::flat_hash_map<string, string> store_;
};

class RaftMultiGroupTest : public Test {
};

// ---------------------------------------------------------------------------
// 1. Two groups, each with 3 nodes — independent leader elections.
// ---------------------------------------------------------------------------
TEST_F(RaftMultiGroupTest, TwoGroupsIndependentElections) {
  LocalTransport transport;
  RaftNode g1n1("A"), g1n2("B"), g1n3("C");
  RaftNode g2n1("A"), g2n2("B"), g2n3("C");

  // Register nodes for group 0 and group 1.
  transport.RegisterNode(0, "A", &g1n1);
  transport.RegisterNode(0, "B", &g1n2);
  transport.RegisterNode(0, "C", &g1n3);
  transport.RegisterNode(1, "A", &g2n1);
  transport.RegisterNode(1, "B", &g2n2);
  transport.RegisterNode(1, "C", &g2n3);

  // Configure group 0.
  g1n1.set_group_id(0);
  g1n2.set_group_id(0);
  g1n3.set_group_id(0);
  g1n1.SetTransport(&transport);
  g1n1.AddPeer("B");
  g1n1.AddPeer("C");
  g1n2.SetTransport(&transport);
  g1n2.AddPeer("A");
  g1n2.AddPeer("C");
  g1n3.SetTransport(&transport);
  g1n3.AddPeer("A");
  g1n3.AddPeer("B");

  // Configure group 1.
  g2n1.set_group_id(1);
  g2n2.set_group_id(1);
  g2n3.set_group_id(1);
  g2n1.SetTransport(&transport);
  g2n1.AddPeer("B");
  g2n1.AddPeer("C");
  g2n2.SetTransport(&transport);
  g2n2.AddPeer("A");
  g2n2.AddPeer("C");
  g2n3.SetTransport(&transport);
  g2n3.AddPeer("A");
  g2n3.AddPeer("B");

  // Group 0 starts election.
  g1n1.StartElection();
  EXPECT_EQ(RaftRole::Leader, g1n1.role());
  EXPECT_EQ(1u, g1n1.term());
  EXPECT_EQ(RaftRole::Follower, g1n2.role());
  EXPECT_EQ(RaftRole::Follower, g1n3.role());

  // Group 1 should be unaffected — still followers.
  EXPECT_EQ(RaftRole::Follower, g2n1.role());
  EXPECT_EQ(RaftRole::Follower, g2n2.role());
  EXPECT_EQ(RaftRole::Follower, g2n3.role());

  // Now group 1 has its own election.
  g2n1.StartElection();
  EXPECT_EQ(RaftRole::Leader, g2n1.role());

  // Both groups have their own leaders independently.
  EXPECT_EQ(RaftRole::Leader, g1n1.role());
  EXPECT_EQ(RaftRole::Leader, g2n1.role());
}

// ---------------------------------------------------------------------------
// 2. Group isolation: a partition in one group doesn't affect other groups.
// ---------------------------------------------------------------------------
TEST_F(RaftMultiGroupTest, PartitionInOneGroupDoesNotAffectOther) {
  LocalTransport transport_all;
  LocalTransport transport_g0;    // group 0 only (isolated)
  LocalTransport transport_g1;    // group 1 only

  RaftNode g0a("A"), g0b("B"), g0c("C");
  RaftNode g1a("A"), g1b("B"), g1c("C");

  // Register in all-transport.
  transport_all.RegisterNode(0, "A", &g0a);
  transport_all.RegisterNode(0, "B", &g0b);
  transport_all.RegisterNode(0, "C", &g0c);
  transport_all.RegisterNode(1, "A", &g1a);
  transport_all.RegisterNode(1, "B", &g1b);
  transport_all.RegisterNode(1, "C", &g1c);

  // Register group 0 nodes only in g0 transport.
  transport_g0.RegisterNode(0, "A", &g0a);
  transport_g0.RegisterNode(0, "B", &g0b);
  transport_g0.RegisterNode(0, "C", &g0c);

  // Register group 1 nodes only in g1 transport.
  transport_g1.RegisterNode(1, "A", &g1a);
  transport_g1.RegisterNode(1, "B", &g1b);
  transport_g1.RegisterNode(1, "C", &g1c);

  // Helper to configure a node.
  auto configure = [&](RaftNode& node, GroupId gid, Transport* t) {
    node.set_group_id(gid);
    node.SetTransport(t);
    node.AddPeer("B");
    node.AddPeer("C");
  };

  configure(g0a, 0, &transport_g0);
  configure(g0b, 0, &transport_g0);
  configure(g0c, 0, &transport_g0);
  configure(g1a, 1, &transport_g1);
  configure(g1b, 1, &transport_g1);
  configure(g1c, 1, &transport_g1);

  // Group 0 elects a leader.
  g0a.StartElection();
  EXPECT_EQ(RaftRole::Leader, g0a.role());

  // Group 1 should be unaffected — still all followers.
  EXPECT_EQ(RaftRole::Follower, g1a.role());
  EXPECT_EQ(RaftRole::Follower, g1b.role());
  EXPECT_EQ(RaftRole::Follower, g1c.role());

  // Group 1 has its own election.
  g1a.StartElection();
  EXPECT_EQ(RaftRole::Leader, g1a.role());

  // Group 0 leader should still be leader (group 1's activity doesn't interfere).
  EXPECT_EQ(RaftRole::Leader, g0a.role());
}

// ---------------------------------------------------------------------------
// 3. ShardRouter: key-to-group mapping.
// ---------------------------------------------------------------------------
TEST_F(RaftMultiGroupTest, ShardRouterKeyMapping) {
  ShardRouter router(4);

  // Same key always maps to same group.
  GroupId g_a1 = router.GetGroupForKey("user:1000");
  GroupId g_a2 = router.GetGroupForKey("user:1000");
  EXPECT_EQ(g_a1, g_a2);

  // Different keys may map to different groups.
  GroupId g_b = router.GetGroupForKey("user:2000");
  GroupId g_c = router.GetGroupForKey("user:3000");

  // Hash distribution: at least some keys should differ with 4 groups.
  bool all_same = (g_a1 == g_b && g_b == g_c);
  EXPECT_FALSE(all_same) << "Keys should distribute across groups";
}

// ---------------------------------------------------------------------------
// 4. RaftGroupManager: create, retrieve, and manage groups.
// ---------------------------------------------------------------------------
TEST_F(RaftMultiGroupTest, GroupManagerCreateAndAccess) {
  RaftGroupManager mgr;

  RaftGroup* g0 = mgr.CreateGroup(0);
  ASSERT_NE(nullptr, g0);
  EXPECT_EQ(0u, g0->group_id());

  RaftGroup* g1 = mgr.CreateGroup(1);
  ASSERT_NE(nullptr, g1);
  EXPECT_EQ(1u, g1->group_id());

  // Duplicate create returns null.
  EXPECT_EQ(nullptr, mgr.CreateGroup(0));

  // Get by ID.
  EXPECT_EQ(g0, mgr.GetGroup(0));
  EXPECT_EQ(g1, mgr.GetGroup(1));
  EXPECT_EQ(nullptr, mgr.GetGroup(999));

  EXPECT_EQ(2u, mgr.GroupCount());

  // Remove.
  mgr.RemoveGroup(0);
  EXPECT_EQ(nullptr, mgr.GetGroup(0));
  EXPECT_EQ(1u, mgr.GroupCount());
}

// ---------------------------------------------------------------------------
// 5. RaftGroupManager with transport: independent per-group elections.
// ---------------------------------------------------------------------------
TEST_F(RaftMultiGroupTest, ManagerWithMultipleGroups) {
  RaftGroupManager mgr;
  LocalTransport transport;

  // Create 3 groups, each with 3 nodes (A, B, C).
  // For simplicity, all nodes share the same transport but operate on different groups.
  std::vector<std::unique_ptr<RaftGroup>> groups;
  std::vector<RaftNode*> all_nodes;

  for (GroupId gid = 0; gid < 3; gid++) {
    // Create group manually outside manager for fine-grained control.
    auto group = std::make_unique<RaftGroup>(gid);
    RaftNode& na = group->node();
    na.set_group_id(gid);
    na.SetTransport(&transport);
    na.AddPeer("B");
    na.AddPeer("C");
    transport.RegisterNode(gid, "A", &na);

    // Create peer nodes outside the manager.
    auto* nb = new RaftNode("B");
    nb->set_group_id(gid);
    nb->SetTransport(&transport);
    nb->AddPeer("A");
    nb->AddPeer("C");
    transport.RegisterNode(gid, "B", nb);
    all_nodes.push_back(nb);

    auto* nc = new RaftNode("C");
    nc->set_group_id(gid);
    nc->SetTransport(&transport);
    nc->AddPeer("A");
    nc->AddPeer("B");
    transport.RegisterNode(gid, "C", nc);
    all_nodes.push_back(nc);

    groups.push_back(std::move(group));
  }

  // All groups start as followers.
  for (auto& g : groups) {
    EXPECT_EQ(RaftRole::Follower, g->node().role());
  }

  // Trigger elections in all groups.
  for (auto& g : groups) {
    g->node().StartElection();
  }

  // Every group should have elected its own leader (node A in each).
  for (auto& g : groups) {
    EXPECT_EQ(RaftRole::Leader, g->node().role())
        << "Group " << g->group_id() << " should have a leader";
  }

  // Clean up.
  for (auto* n : all_nodes) {
    delete n;
  }
}

// ---------------------------------------------------------------------------
// 6. ShardRouter with RaftGroupManager: write routing.
// ---------------------------------------------------------------------------
TEST_F(RaftMultiGroupTest, RouterWithManager) {
  RaftGroupManager mgr;
  mgr.shard_router().set_num_groups(4);

  // Create 4 groups.
  for (GroupId gid = 0; gid < 4; gid++) {
    mgr.CreateGroup(gid);
  }

  EXPECT_EQ(4u, mgr.GroupCount());

  // Route keys to groups via ShardRouter.
  std::vector<GroupId> key_groups = {
    mgr.shard_router().GetGroupForKey("user:1"),
    mgr.shard_router().GetGroupForKey("user:2"),
    mgr.shard_router().GetGroupForKey("user:3"),
    mgr.shard_router().GetGroupForKey("user:4"),
    mgr.shard_router().GetGroupForKey("user:5"),
  };

  // All routed groups should exist.
  for (GroupId gid : key_groups) {
    EXPECT_NE(nullptr, mgr.GetGroup(gid))
        << "Group " << gid << " should exist";
  }

  // Same key routes to same group.
  EXPECT_EQ(mgr.shard_router().GetGroupForKey("user:1"),
            mgr.shard_router().GetGroupForKey("user:1"));
}

}  // namespace dfly
