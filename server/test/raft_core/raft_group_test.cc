// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_group.h"

#include <gmock/gmock.h>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

class RaftGroupTest : public Test {
};

TEST_F(RaftGroupTest, CreateGroup) {
  RaftGroup group(1);
  EXPECT_EQ(1u, group.group_id());
}

TEST_F(RaftGroupTest, DefaultGroupId) {
  RaftGroup group(0);
  EXPECT_EQ(0u, group.group_id());
}

TEST_F(RaftGroupTest, AccessNode) {
  RaftGroup group(42);
  EXPECT_EQ(RaftRole::Follower, group.node().role());
  EXPECT_EQ(0u, group.node().term());
}

TEST_F(RaftGroupTest, NodeStateTransition) {
  RaftGroup group(7);
  group.node().BecomeCandidate();
  EXPECT_EQ(RaftRole::Candidate, group.node().role());
  EXPECT_EQ(1u, group.node().term());

  group.node().BecomeLeader();
  EXPECT_EQ(RaftRole::Leader, group.node().role());

  group.node().BecomeFollower(2);
  EXPECT_EQ(RaftRole::Follower, group.node().role());
  EXPECT_EQ(2u, group.node().term());
}

TEST_F(RaftGroupTest, MultipleGroups) {
  RaftGroup g1(1);
  RaftGroup g2(2);

  EXPECT_EQ(1u, g1.group_id());
  EXPECT_EQ(2u, g2.group_id());

  g1.node().BecomeCandidate();
  EXPECT_EQ(RaftRole::Candidate, g1.node().role());
  EXPECT_EQ(RaftRole::Follower, g2.node().role());
}

}  // namespace dfly
