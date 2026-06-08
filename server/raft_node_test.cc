// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_node.h"

#include <gmock/gmock.h>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

class RaftNodeTest : public Test {
};

TEST_F(RaftNodeTest, InitialState) {
  RaftNode node;
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(0u, node.term());
}

TEST_F(RaftNodeTest, BecomeFollower) {
  RaftNode node;
  node.BecomeFollower(3);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(3u, node.term());
}

TEST_F(RaftNodeTest, BecomeFollowerHigherTerm) {
  RaftNode node;
  node.BecomeFollower(1);
  EXPECT_EQ(1u, node.term());
  node.BecomeFollower(5);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(5u, node.term());
}

TEST_F(RaftNodeTest, BecomeCandidate) {
  RaftNode node;
  node.BecomeCandidate();
  EXPECT_EQ(RaftRole::Candidate, node.role());
  EXPECT_EQ(1u, node.term());
}

TEST_F(RaftNodeTest, BecomeCandidateIncrementsTerm) {
  RaftNode node;
  node.BecomeFollower(5);
  node.BecomeCandidate();
  EXPECT_EQ(RaftRole::Candidate, node.role());
  EXPECT_EQ(6u, node.term());
}

TEST_F(RaftNodeTest, BecomeLeader) {
  RaftNode node;
  node.BecomeCandidate();
  node.BecomeLeader();
  EXPECT_EQ(RaftRole::Leader, node.role());
  EXPECT_EQ(1u, node.term());
}

TEST_F(RaftNodeTest, FullCycleFollowerToLeader) {
  RaftNode node;
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(0u, node.term());

  node.BecomeCandidate();
  EXPECT_EQ(RaftRole::Candidate, node.role());
  EXPECT_EQ(1u, node.term());

  node.BecomeLeader();
  EXPECT_EQ(RaftRole::Leader, node.role());
  EXPECT_EQ(1u, node.term());
}

TEST_F(RaftNodeTest, LeaderStepsDown) {
  RaftNode node;
  node.BecomeCandidate();
  node.BecomeLeader();
  EXPECT_EQ(RaftRole::Leader, node.role());

  node.BecomeFollower(2);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(2u, node.term());
}

TEST_F(RaftNodeTest, CandidateStepsDown) {
  RaftNode node;
  node.BecomeCandidate();
  EXPECT_EQ(RaftRole::Candidate, node.role());

  node.BecomeFollower(3);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(3u, node.term());
}

TEST_F(RaftNodeTest, TermNeverDecreases) {
  RaftNode node;
  node.BecomeFollower(10);
  node.BecomeCandidate();
  EXPECT_EQ(11u, node.term());

  node.BecomeFollower(11);
  EXPECT_EQ(11u, node.term());
  EXPECT_EQ(RaftRole::Follower, node.role());
}

}  // namespace dfly
