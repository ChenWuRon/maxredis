// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_node.h"

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "server/raft/raft_storage.h"

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

// --- StartElection tests ---

TEST_F(RaftNodeTest, ThreeNodesAllGrant) {
  RaftNode n1("Node1");
  RaftNode n2("Node2");
  RaftNode n3("Node3");

  n1.AddPeer(&n2);
  n1.AddPeer(&n3);

  ElectionResult result = n1.StartElection();

  EXPECT_EQ(RaftRole::Leader, n1.role());
  EXPECT_EQ(1u, n1.term());
  EXPECT_EQ(1u, n1.leader_term());
  EXPECT_EQ(3u, result.votes_received);
  EXPECT_EQ(0u, result.votes_rejected);
}

TEST_F(RaftNodeTest, OnePeerRejects) {
  RaftNode n1("Node1");
  RaftNode n2("Node2");
  RaftNode n3("Node3");

  // n2 already voted for someone else in this term
  n2.BecomeFollower(1);
  n2.OnRequestVote(VoteRequest{1, "Other", 0, 0});

  n1.AddPeer(&n2);
  n1.AddPeer(&n3);

  ElectionResult result = n1.StartElection();

  EXPECT_EQ(2u, result.votes_received);  // self + n3
  EXPECT_EQ(1u, result.votes_rejected);  // n2
}

TEST_F(RaftNodeTest, StaleTermPeerRejects) {
  RaftNode n1("Node1");
  RaftNode n2("Node2");

  // n2 has a higher term
  n2.BecomeFollower(5);

  n1.AddPeer(&n2);

  ElectionResult result = n1.StartElection();

  // n1's term starts at 0, becomes 1 after StartElection
  // n2 has term 5 > 1, so n2 rejects n1's vote request
  EXPECT_EQ(1u, result.votes_received);  // only self
  EXPECT_EQ(1u, result.votes_rejected);
  EXPECT_EQ(1u, n1.term());
  EXPECT_EQ(5u, n2.term());
  EXPECT_EQ(RaftRole::Candidate, n1.role());
}

// --- TryBecomeLeader tests ---

TEST_F(RaftNodeTest, ThreeNodesThreeVotesBecomesLeader) {
  RaftNode n1("N1"), n2("N2"), n3("N3");
  n1.AddPeer(&n2);
  n1.AddPeer(&n3);

  ElectionResult r = n1.StartElection();
  EXPECT_EQ(3u, r.votes_received);
  EXPECT_EQ(RaftRole::Leader, n1.role());
}

TEST_F(RaftNodeTest, ThreeNodesTwoVotesBecomesLeader) {
  RaftNode n1("N1"), n2("N2"), n3("N3");
  // n2 already voted for other in this term
  n2.BecomeFollower(1);
  n2.OnRequestVote(VoteRequest{1, "Other", 0, 0});

  n1.AddPeer(&n2);
  n1.AddPeer(&n3);

  ElectionResult r = n1.StartElection();
  // n1 term = 2 (since n2 has term 1, n1 becomes term 2 via Follower step-down)
  // Actually: n1.StartElection → BecomeCandidate → term 0→1
  // n2 rejects (voted for Other), n3 grants
  // votes = self(1) + n3(1) = 2
  // Majority = 3/2+1 = 2
  EXPECT_EQ(2u, r.votes_received);
  EXPECT_EQ(RaftRole::Leader, n1.role());
}

TEST_F(RaftNodeTest, ThreeNodesOneVoteStaysCandidate) {
  RaftNode n1("N1"), n2("N2"), n3("N3");
  // both peers already voted for other
  n2.BecomeFollower(1);
  n2.OnRequestVote(VoteRequest{1, "Other", 0, 0});
  n3.BecomeFollower(1);
  n3.OnRequestVote(VoteRequest{1, "Other", 0, 0});

  n1.AddPeer(&n2);
  n1.AddPeer(&n3);

  ElectionResult r = n1.StartElection();
  EXPECT_EQ(1u, r.votes_received);  // only self
  EXPECT_EQ(RaftRole::Candidate, n1.role());
}

TEST_F(RaftNodeTest, FiveNodesThreeVotesBecomesLeader) {
  RaftNode n1("N1"), n2("N2"), n3("N3"), n4("N4"), n5("N5");
  n1.AddPeer(&n2);
  n1.AddPeer(&n3);
  n1.AddPeer(&n4);
  n1.AddPeer(&n5);

  ElectionResult r = n1.StartElection();
  EXPECT_EQ(5u, r.votes_received);
  EXPECT_EQ(RaftRole::Leader, n1.role());
}

TEST_F(RaftNodeTest, FiveNodesTwoVotesStaysCandidate) {
  RaftNode n1("N1"), n2("N2"), n3("N3"), n4("N4"), n5("N5");
  // three peers already voted for other
  n2.BecomeFollower(1);
  n2.OnRequestVote(VoteRequest{1, "Other", 0, 0});
  n3.BecomeFollower(1);
  n3.OnRequestVote(VoteRequest{1, "Other", 0, 0});
  n4.BecomeFollower(1);
  n4.OnRequestVote(VoteRequest{1, "Other", 0, 0});

  n1.AddPeer(&n2);
  n1.AddPeer(&n3);
  n1.AddPeer(&n4);
  n1.AddPeer(&n5);

  ElectionResult r = n1.StartElection();
  EXPECT_EQ(2u, r.votes_received);  // self + n5
  EXPECT_EQ(RaftRole::Candidate, n1.role());
}

// --- OnHeartbeat tests ---

TEST_F(RaftNodeTest, HeartbeatStaleTermRejected) {
  RaftNode node("A");
  node.BecomeFollower(10);

  HeartbeatRequest req{5, "Leader"};  // stale term
  HeartbeatResponse rsp = node.OnHeartbeat(req);

  EXPECT_FALSE(rsp.success);
  EXPECT_EQ(10u, rsp.term);
  EXPECT_EQ(10u, node.term());
  EXPECT_EQ(RaftRole::Follower, node.role());
}

TEST_F(RaftNodeTest, HeartbeatSameTermAccepted) {
  RaftNode node("A");
  node.BecomeFollower(5);

  HeartbeatRequest req{5, "Leader"};
  HeartbeatResponse rsp = node.OnHeartbeat(req);

  EXPECT_TRUE(rsp.success);
  EXPECT_EQ(5u, rsp.term);
  EXPECT_EQ(RaftRole::Follower, node.role());
}

TEST_F(RaftNodeTest, HeartbeatHigherTermAccepted) {
  RaftNode node("A");
  node.BecomeFollower(5);

  HeartbeatRequest req{8, "NewLeader"};
  HeartbeatResponse rsp = node.OnHeartbeat(req);

  EXPECT_TRUE(rsp.success);
  EXPECT_EQ(8u, rsp.term);
  EXPECT_EQ(8u, node.term());
  EXPECT_EQ(RaftRole::Follower, node.role());
}

TEST_F(RaftNodeTest, HeartbeatFromCandidateStepsDown) {
  RaftNode n1("N1"), n2("N2"), n3("N3");
  n1.AddPeer(&n2);
  n1.AddPeer(&n3);

  // n1 becomes Candidate
  n1.BecomeCandidate();

  // Another node sends heartbeat with same term — n1 should step down
  HeartbeatRequest req{1, "N2"};
  HeartbeatResponse rsp = n1.OnHeartbeat(req);

  EXPECT_TRUE(rsp.success);
  EXPECT_EQ(RaftRole::Follower, n1.role());
  EXPECT_TRUE(n1.voted_for().empty());
}

TEST_F(RaftNodeTest, HeartbeatKeepsLeaderStable) {
  RaftNode n1("N1"), n2("N2"), n3("N3");
  n1.AddPeer(&n2);
  n1.AddPeer(&n3);
  n2.AddPeer(&n1);
  n2.AddPeer(&n3);

  // Elect n1 as leader
  n1.StartElection();
  ASSERT_EQ(RaftRole::Leader, n1.role());

  // Send heartbeats from n1 to followers
  HeartbeatRequest hb{n1.term(), n1.node_id()};
  HeartbeatResponse rsp2 = n2.OnHeartbeat(hb);
  HeartbeatResponse rsp3 = n3.OnHeartbeat(hb);

  EXPECT_TRUE(rsp2.success);
  EXPECT_TRUE(rsp3.success);
  EXPECT_EQ(RaftRole::Follower, n2.role());
  EXPECT_EQ(RaftRole::Follower, n3.role());
  EXPECT_EQ(RaftRole::Leader, n1.role());
}

// --- AppendEntries tests ---

TEST_F(RaftNodeTest, AppendEntriesReplicatesLog) {
  RaftStorage leader_storage, follower_storage;
  RaftNode leader("L1"), follower("F1");
  leader.SetStorage(&leader_storage);
  follower.SetStorage(&follower_storage);
  leader.AddPeer(&follower);

  leader_storage.AppendLog(LogEntry{1, 0, "cmd1"});
  leader_storage.AppendLog(LogEntry{1, 0, "cmd2"});
  leader_storage.AppendLog(LogEntry{2, 0, "cmd3"});

  leader.ReplicateLog();

  EXPECT_EQ(3, follower_storage.LogSize());
  EXPECT_EQ("cmd1", follower_storage.EntryAt(1).command);
  EXPECT_EQ("cmd2", follower_storage.EntryAt(2).command);
  EXPECT_EQ("cmd3", follower_storage.EntryAt(3).command);
}

TEST_F(RaftNodeTest, AppendEntriesFillsGaps) {
  RaftStorage leader_storage, follower_storage;
  RaftNode leader("L1"), follower("F1");
  leader.SetStorage(&leader_storage);
  follower.SetStorage(&follower_storage);
  leader.AddPeer(&follower);

  leader_storage.AppendLog(LogEntry{1, 0, "a"});
  leader_storage.AppendLog(LogEntry{1, 0, "b"});
  leader_storage.AppendLog(LogEntry{2, 0, "c"});

  // Follower already has first entry
  follower_storage.AppendLog(LogEntry{1, 0, "a"});

  leader.ReplicateLog();

  EXPECT_EQ(3, follower_storage.LogSize());
  EXPECT_EQ("b", follower_storage.EntryAt(2).command);
  EXPECT_EQ("c", follower_storage.EntryAt(3).command);
}

TEST_F(RaftNodeTest, AppendEntriesRejectsPrevLogMismatch) {
  RaftStorage leader_storage, follower_storage;
  RaftNode leader("L1"), follower("F1");
  leader.SetStorage(&leader_storage);
  follower.SetStorage(&follower_storage);
  leader.AddPeer(&follower);

  leader_storage.AppendLog(LogEntry{2, 0, "x"});

  // Follower has a different entry at index 1
  follower_storage.AppendLog(LogEntry{1, 0, "y"});

  // Manually send with specific prev_log_index/term that won't match
  AppendEntriesRequest req;
  req.term = 2;
  req.leader_id = "L1";
  req.prev_log_index = 1;
  req.prev_log_term = 999;  // won't match follower's term at index 1

  AppendEntriesResponse rsp = follower.OnAppendEntries(req);

  EXPECT_FALSE(rsp.success);
  EXPECT_EQ(0u, rsp.last_log_index);
}

TEST_F(RaftNodeTest, AppendEntriesAcceptsMatchingPrevLog) {
  RaftStorage follower_storage;
  RaftNode follower("F1");
  follower.SetStorage(&follower_storage);

  follower_storage.AppendLog(LogEntry{1, 0, "a"});
  follower_storage.AppendLog(LogEntry{2, 0, "b"});

  AppendEntriesRequest req;
  req.term = 2;
  req.leader_id = "L1";
  req.prev_log_index = 2;
  req.prev_log_term = 2;  // matches follower's entry at index 2
  LogEntry new_entry{2, 0, "c"};
  new_entry.index = 3;  // this will be the next index after prev_log_index
  req.entries = {new_entry};

  AppendEntriesResponse rsp = follower.OnAppendEntries(req);

  EXPECT_TRUE(rsp.success);
  EXPECT_EQ(3, follower_storage.LogSize());
  EXPECT_EQ("c", follower_storage.EntryAt(3).command);
}

TEST_F(RaftNodeTest, AppendEntriesStaleTermRejected) {
  RaftStorage follower_storage;
  RaftNode follower("F1");
  follower.SetStorage(&follower_storage);
  follower.BecomeFollower(10);

  AppendEntriesRequest req;
  req.term = 5;  // stale

  AppendEntriesResponse rsp = follower.OnAppendEntries(req);

  EXPECT_FALSE(rsp.success);
  EXPECT_EQ(10u, rsp.term);
}

}  // namespace dfly
