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
  RaftNode node("node1");
  EXPECT_EQ("node1", node.node_id());
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(0u, node.term());
  EXPECT_TRUE(node.voted_for().empty());
  EXPECT_EQ(0u, node.vote_count());
}

TEST_F(RaftNodeTest, BecomeFollower) {
  RaftNode node("n1");
  node.BecomeFollower(3);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(3u, node.term());
  EXPECT_TRUE(node.voted_for().empty());
  EXPECT_EQ(0u, node.vote_count());
}

TEST_F(RaftNodeTest, BecomeFollowerHigherTerm) {
  RaftNode node("n1");
  node.BecomeFollower(1);
  EXPECT_EQ(1u, node.term());
  node.BecomeFollower(5);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(5u, node.term());
  EXPECT_TRUE(node.voted_for().empty());
}

TEST_F(RaftNodeTest, BecomeCandidate) {
  RaftNode node("node1");
  node.BecomeCandidate();
  EXPECT_EQ(RaftRole::Candidate, node.role());
  EXPECT_EQ(1u, node.term());
  EXPECT_EQ("node1", node.voted_for());
  EXPECT_EQ(1u, node.vote_count());
}

TEST_F(RaftNodeTest, BecomeCandidateIncrementsTerm) {
  RaftNode node("mynode");
  node.BecomeFollower(5);
  node.BecomeCandidate();
  EXPECT_EQ(RaftRole::Candidate, node.role());
  EXPECT_EQ(6u, node.term());
  EXPECT_EQ("mynode", node.voted_for());
  EXPECT_EQ(1u, node.vote_count());
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

// --- OnRequestVote tests ---

TEST_F(RaftNodeTest, RejectStaleTerm) {
  RaftNode node("A");
  node.BecomeFollower(10);  // current_term = 10

  VoteRequest req{9, "B", 0, 0};  // request.term = 9 (stale)
  VoteResponse rsp = node.OnRequestVote(req);

  EXPECT_FALSE(rsp.vote_granted);
  EXPECT_EQ(10u, rsp.term);
  EXPECT_EQ(10u, node.term());
  EXPECT_TRUE(node.voted_for().empty());
}

TEST_F(RaftNodeTest, GrantVoteHigherTerm) {
  RaftNode node("A");
  node.BecomeFollower(10);  // current_term = 10

  VoteRequest req{11, "B", 0, 0};  // request.term = 11 (higher)
  VoteResponse rsp = node.OnRequestVote(req);

  EXPECT_TRUE(rsp.vote_granted);
  EXPECT_EQ(11u, rsp.term);
  EXPECT_EQ(11u, node.term());
  EXPECT_EQ("B", node.voted_for());
  EXPECT_EQ(RaftRole::Follower, node.role());
}

TEST_F(RaftNodeTest, RejectVoteForDifferentCandidate) {
  RaftNode node("A");
  node.BecomeFollower(20);

  // First grant vote to Node2
  VoteRequest req2{20, "Node2", 0, 0};
  VoteResponse rsp1 = node.OnRequestVote(req2);
  EXPECT_TRUE(rsp1.vote_granted);
  EXPECT_EQ("Node2", node.voted_for());

  // Now Node3 requests — should reject
  VoteRequest req3{20, "Node3", 0, 0};
  VoteResponse rsp2 = node.OnRequestVote(req3);

  EXPECT_FALSE(rsp2.vote_granted);
  EXPECT_EQ(20u, rsp2.term);
  EXPECT_EQ("Node2", node.voted_for());  // unchanged
}

TEST_F(RaftNodeTest, GrantVoteToSameCandidateAgain) {
  RaftNode node("A");
  node.BecomeFollower(20);

  VoteRequest req{20, "Node2", 0, 0};
  VoteResponse rsp1 = node.OnRequestVote(req);
  EXPECT_TRUE(rsp1.vote_granted);
  EXPECT_EQ("Node2", node.voted_for());

  // Same candidate again — should still grant
  VoteResponse rsp2 = node.OnRequestVote(req);
  EXPECT_TRUE(rsp2.vote_granted);
  EXPECT_EQ(20u, rsp2.term);
  EXPECT_EQ("Node2", node.voted_for());
}

}  // namespace dfly
