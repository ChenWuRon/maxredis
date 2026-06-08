// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/election_timer.h"

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "server/raft/raft_node.h"

namespace dfly {

using namespace testing;

class ElectionTimerTest : public Test {
};

TEST_F(ElectionTimerTest, OnElectionTimeoutFromFollower) {
  RaftNode node;
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(0u, node.term());

  node.OnElectionTimeout();
  EXPECT_EQ(RaftRole::Candidate, node.role());
  EXPECT_EQ(1u, node.term());
}

TEST_F(ElectionTimerTest, OnElectionTimeoutFromCandidateNoOp) {
  RaftNode node;
  node.BecomeCandidate();
  EXPECT_EQ(RaftRole::Candidate, node.role());

  node.OnElectionTimeout();
  EXPECT_EQ(RaftRole::Candidate, node.role());
}

TEST_F(ElectionTimerTest, OnElectionTimeoutFromLeaderNoOp) {
  RaftNode node;
  node.BecomeCandidate();
  node.BecomeLeader();
  EXPECT_EQ(RaftRole::Leader, node.role());

  node.OnElectionTimeout();
  EXPECT_EQ(RaftRole::Leader, node.role());
}

TEST_F(ElectionTimerTest, TimerStartStop) {
  RaftNode node;
  ElectionTimer timer(&node);

  EXPECT_FALSE(timer.IsRunning());
  timer.Start();
  EXPECT_TRUE(timer.IsRunning());
  timer.Stop();
  EXPECT_FALSE(timer.IsRunning());
}

TEST_F(ElectionTimerTest, TimerDoubleStartIsSafe) {
  RaftNode node;
  ElectionTimer timer(&node);

  timer.Start();
  timer.Start();  // should be a no-op
  EXPECT_TRUE(timer.IsRunning());
  timer.Stop();
}

}  // namespace dfly
