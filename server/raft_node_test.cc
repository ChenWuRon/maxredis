// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_node.h"

#include <gmock/gmock.h>

#include <cstring>
#include <string>
#include <vector>

#include "base/gtest.h"
#include "server/raft/command_log.h"
#include "server/raft/command_encoder.h"
#include "server/raft/local_transport.h"
#include "server/raft/log_storage.h"
#include "server/raft/replicated_command.h"
#include "server/raft/transport.h"
#include "server/service/command_registry.h"
#include "server/state_machine/state_machine.h"

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
  CommandLog storage;
  RaftNode node("A");
  node.SetLogStorage(&storage);
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
  CommandLog storage;
  RaftNode node("A");
  node.SetLogStorage(&storage);
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
  CommandLog storage;
  RaftNode node("A");
  node.SetLogStorage(&storage);
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

// --- Election restriction tests (§5.4.1) ---

TEST_F(RaftNodeTest, RejectVoteWhenCandidateLogTermLower) {
  CommandLog storage;
  storage.Append(LogEntry{3, 0, "x"});  // local last term = 3
  storage.Append(LogEntry{4, 0, "y"});  // local last term = 4

  RaftNode node("A");
  node.SetLogStorage(&storage);
  node.BecomeFollower(10);

  // Candidate has last_log_term = 1 < local_last_term = 4
  VoteRequest req{10, "C", 1, 1};
  VoteResponse rsp = node.OnRequestVote(req);

  EXPECT_FALSE(rsp.vote_granted);
  EXPECT_EQ(10u, rsp.term);
  EXPECT_TRUE(node.voted_for().empty());
}

TEST_F(RaftNodeTest, RejectVoteWhenCandidateLogTermEqualButIndexLower) {
  CommandLog storage;
  storage.Append(LogEntry{2, 0, "a"});  // idx 1, term 2
  storage.Append(LogEntry{2, 0, "b"});  // idx 2, term 2
  storage.Append(LogEntry{2, 0, "c"});  // idx 3, term 2
  storage.Append(LogEntry{2, 0, "d"});  // idx 4, term 2
  storage.Append(LogEntry{2, 0, "e"});  // idx 5, term 2 — last term = 2, last index = 5

  RaftNode node("A");
  node.SetLogStorage(&storage);
  node.BecomeFollower(5);

  // Candidate has same term (2) but lower index (3 < 5)
  // VoteRequest(term, candidate, last_log_index, last_log_term)
  VoteRequest req{5, "C", 3, 2};
  VoteResponse rsp = node.OnRequestVote(req);

  EXPECT_FALSE(rsp.vote_granted);
  EXPECT_EQ(5u, rsp.term);
  EXPECT_TRUE(node.voted_for().empty());
}

TEST_F(RaftNodeTest, GrantVoteWhenCandidateLogTermHigher) {
  CommandLog storage;
  storage.Append(LogEntry{2, 0, "a"});  // last term = 2

  RaftNode node("A");
  node.SetLogStorage(&storage);
  node.BecomeFollower(5);

  // Candidate's last_log_term = 3 > local_last_term = 2
  VoteRequest req{5, "C", 3, 10};
  VoteResponse rsp = node.OnRequestVote(req);

  EXPECT_TRUE(rsp.vote_granted);
  EXPECT_EQ(5u, rsp.term);
  EXPECT_EQ("C", node.voted_for());
}

TEST_F(RaftNodeTest, GrantVoteWhenCandidateLogTermEqualAndIndexHigher) {
  CommandLog storage;
  storage.Append(LogEntry{2, 0, "a"});
  storage.Append(LogEntry{2, 0, "b"});  // last term = 2, last index = 2

  RaftNode node("A");
  node.SetLogStorage(&storage);
  node.BecomeFollower(5);

  // Candidate has same term (2) but higher index (5 > 2)
  VoteRequest req{5, "C", 2, 5};
  VoteResponse rsp = node.OnRequestVote(req);

  EXPECT_TRUE(rsp.vote_granted);
  EXPECT_EQ(5u, rsp.term);
  EXPECT_EQ("C", node.voted_for());
}

TEST_F(RaftNodeTest, GrantVoteWhenCandidateLogExactlySame) {
  CommandLog storage;
  storage.Append(LogEntry{3, 0, "a"});
  storage.Append(LogEntry{3, 0, "b"});
  storage.Append(LogEntry{3, 0, "c"});  // last term = 3, last index = 3

  RaftNode node("A");
  node.SetLogStorage(&storage);
  node.BecomeFollower(10);

  // Candidate has exactly the same log info
  VoteRequest req{10, "C", 3, 3};
  VoteResponse rsp = node.OnRequestVote(req);

  EXPECT_TRUE(rsp.vote_granted);
  EXPECT_EQ(10u, rsp.term);
  EXPECT_EQ("C", node.voted_for());
}

TEST_F(RaftNodeTest, RejectVoteHigherTermButStaleLog) {
  CommandLog storage;
  storage.Append(LogEntry{5, 0, "a"});  // last term = 5

  RaftNode node("A");
  node.SetLogStorage(&storage);
  node.BecomeFollower(3);

  // Candidate has higher term (5 > 3) but stale log (last_log_term=4 < local 5)
  // VoteRequest(term, candidate, last_log_index, last_log_term)
  VoteRequest req{5, "C", 10, 4};
  VoteResponse rsp = node.OnRequestVote(req);

  EXPECT_FALSE(rsp.vote_granted);
  EXPECT_EQ(5u, rsp.term);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_TRUE(node.voted_for().empty());
}

TEST_F(RaftNodeTest, GrantVoteWithNonEmptyLogToSameCandidate) {
  CommandLog storage;
  storage.Append(LogEntry{5, 0, "a"});
  storage.Append(LogEntry{6, 0, "b"});  // last term = 6, last index = 2

  RaftNode node("A");
  node.SetLogStorage(&storage);
  node.BecomeFollower(10);

  // First vote grant: candidate's log (term=6, idx=2) matches local
  // VoteRequest(term, candidate, last_log_index, last_log_term)
  VoteRequest req{10, "C", 2, 6};
  VoteResponse rsp1 = node.OnRequestVote(req);
  EXPECT_TRUE(rsp1.vote_granted);
  EXPECT_EQ("C", node.voted_for());

  // Same candidate requests again — still grants
  VoteResponse rsp2 = node.OnRequestVote(req);
  EXPECT_TRUE(rsp2.vote_granted);
  EXPECT_EQ(10u, rsp2.term);
  EXPECT_EQ("C", node.voted_for());
}

// --- StartElection tests ---

TEST_F(RaftNodeTest, ThreeNodesAllGrant) {
  LocalTransport transport;
  RaftNode n1("Node1");
  RaftNode n2("Node2");
  RaftNode n3("Node3");

  transport.RegisterNode("Node1", &n1);
  transport.RegisterNode("Node2", &n2);
  transport.RegisterNode("Node3", &n3);
  n1.SetTransport(&transport);
  n1.AddPeer("Node2");
  n1.AddPeer("Node3");

  ElectionResult result = n1.StartElection();

  EXPECT_EQ(RaftRole::Leader, n1.role());
  EXPECT_EQ(1u, n1.term());
  EXPECT_EQ(1u, n1.leader_term());
  EXPECT_EQ(3u, result.votes_received);
  EXPECT_EQ(0u, result.votes_rejected);
}

TEST_F(RaftNodeTest, OnePeerRejects) {
  LocalTransport transport;
  RaftNode n1("Node1");
  RaftNode n2("Node2");
  RaftNode n3("Node3");

  // n2 already voted for someone else in this term
  n2.BecomeFollower(1);
  n2.OnRequestVote(VoteRequest{1, "Other", 0, 0});

  transport.RegisterNode("Node1", &n1);
  transport.RegisterNode("Node2", &n2);
  transport.RegisterNode("Node3", &n3);
  n1.SetTransport(&transport);
  n1.AddPeer("Node2");
  n1.AddPeer("Node3");

  ElectionResult result = n1.StartElection();

  EXPECT_EQ(2u, result.votes_received);  // self + n3
  EXPECT_EQ(1u, result.votes_rejected);  // n2
}

TEST_F(RaftNodeTest, StaleTermPeerRejects) {
  LocalTransport transport;
  RaftNode n1("Node1");
  RaftNode n2("Node2");

  // n2 has a higher term
  n2.BecomeFollower(5);

  transport.RegisterNode("Node1", &n1);
  transport.RegisterNode("Node2", &n2);
  n1.SetTransport(&transport);
  n1.AddPeer("Node2");

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
  LocalTransport transport;
  RaftNode n1("N1"), n2("N2"), n3("N3");

  transport.RegisterNode("N1", &n1);
  transport.RegisterNode("N2", &n2);
  transport.RegisterNode("N3", &n3);
  n1.SetTransport(&transport);
  n1.AddPeer("N2");
  n1.AddPeer("N3");

  ElectionResult r = n1.StartElection();
  EXPECT_EQ(3u, r.votes_received);
  EXPECT_EQ(RaftRole::Leader, n1.role());
}

TEST_F(RaftNodeTest, ThreeNodesTwoVotesBecomesLeader) {
  LocalTransport transport;
  RaftNode n1("N1"), n2("N2"), n3("N3");
  // n2 already voted for other in this term
  n2.BecomeFollower(1);
  n2.OnRequestVote(VoteRequest{1, "Other", 0, 0});

  transport.RegisterNode("N1", &n1);
  transport.RegisterNode("N2", &n2);
  transport.RegisterNode("N3", &n3);
  n1.SetTransport(&transport);
  n1.AddPeer("N2");
  n1.AddPeer("N3");

  ElectionResult r = n1.StartElection();
  EXPECT_EQ(2u, r.votes_received);
  EXPECT_EQ(RaftRole::Leader, n1.role());
}

TEST_F(RaftNodeTest, ThreeNodesOneVoteStaysCandidate) {
  LocalTransport transport;
  RaftNode n1("N1"), n2("N2"), n3("N3");
  // both peers already voted for other
  n2.BecomeFollower(1);
  n2.OnRequestVote(VoteRequest{1, "Other", 0, 0});
  n3.BecomeFollower(1);
  n3.OnRequestVote(VoteRequest{1, "Other", 0, 0});

  transport.RegisterNode("N1", &n1);
  transport.RegisterNode("N2", &n2);
  transport.RegisterNode("N3", &n3);
  n1.SetTransport(&transport);
  n1.AddPeer("N2");
  n1.AddPeer("N3");

  ElectionResult r = n1.StartElection();
  EXPECT_EQ(1u, r.votes_received);  // only self
  EXPECT_EQ(RaftRole::Candidate, n1.role());
}

TEST_F(RaftNodeTest, FiveNodesThreeVotesBecomesLeader) {
  LocalTransport transport;
  RaftNode n1("N1"), n2("N2"), n3("N3"), n4("N4"), n5("N5");

  transport.RegisterNode("N1", &n1);
  transport.RegisterNode("N2", &n2);
  transport.RegisterNode("N3", &n3);
  transport.RegisterNode("N4", &n4);
  transport.RegisterNode("N5", &n5);
  n1.SetTransport(&transport);
  n1.AddPeer("N2");
  n1.AddPeer("N3");
  n1.AddPeer("N4");
  n1.AddPeer("N5");

  ElectionResult r = n1.StartElection();
  EXPECT_EQ(5u, r.votes_received);
  EXPECT_EQ(RaftRole::Leader, n1.role());
}

TEST_F(RaftNodeTest, FiveNodesTwoVotesStaysCandidate) {
  LocalTransport transport;
  RaftNode n1("N1"), n2("N2"), n3("N3"), n4("N4"), n5("N5");
  // three peers already voted for other
  n2.BecomeFollower(1);
  n2.OnRequestVote(VoteRequest{1, "Other", 0, 0});
  n3.BecomeFollower(1);
  n3.OnRequestVote(VoteRequest{1, "Other", 0, 0});
  n4.BecomeFollower(1);
  n4.OnRequestVote(VoteRequest{1, "Other", 0, 0});

  transport.RegisterNode("N1", &n1);
  transport.RegisterNode("N2", &n2);
  transport.RegisterNode("N3", &n3);
  transport.RegisterNode("N4", &n4);
  transport.RegisterNode("N5", &n5);
  n1.SetTransport(&transport);
  n1.AddPeer("N2");
  n1.AddPeer("N3");
  n1.AddPeer("N4");
  n1.AddPeer("N5");

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
  n1.AddPeer("N2");
  n1.AddPeer("N3");

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
  LocalTransport transport;
  RaftNode n1("N1"), n2("N2"), n3("N3");

  transport.RegisterNode("N1", &n1);
  transport.RegisterNode("N2", &n2);
  transport.RegisterNode("N3", &n3);
  n1.SetTransport(&transport);
  n2.SetTransport(&transport);
  n1.AddPeer("N2");
  n1.AddPeer("N3");
  n2.AddPeer("N1");
  n2.AddPeer("N3");

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
  LocalTransport transport;
  CommandLog leader_storage, follower_storage;
  RaftNode leader("L1"), follower("F1");
  leader.SetLogStorage(&leader_storage);
  follower.SetLogStorage(&follower_storage);

  transport.RegisterNode("L1", &leader);
  transport.RegisterNode("F1", &follower);
  leader.SetTransport(&transport);
  leader.AddPeer("F1");

  leader_storage.Append(LogEntry{1, 0, "cmd1"});
  leader_storage.Append(LogEntry{1, 0, "cmd2"});
  leader_storage.Append(LogEntry{2, 0, "cmd3"});

  leader.ReplicateLog();

  EXPECT_EQ(3, follower_storage.LogSize());
  EXPECT_EQ("cmd1", follower_storage.Get(1).command);
  EXPECT_EQ("cmd2", follower_storage.Get(2).command);
  EXPECT_EQ("cmd3", follower_storage.Get(3).command);
}

TEST_F(RaftNodeTest, AppendEntriesFillsGaps) {
  LocalTransport transport;
  CommandLog leader_storage, follower_storage;
  RaftNode leader("L1"), follower("F1");
  leader.SetLogStorage(&leader_storage);
  follower.SetLogStorage(&follower_storage);

  transport.RegisterNode("L1", &leader);
  transport.RegisterNode("F1", &follower);
  leader.SetTransport(&transport);
  leader.AddPeer("F1");

  leader_storage.Append(LogEntry{1, 0, "a"});
  leader_storage.Append(LogEntry{1, 0, "b"});
  leader_storage.Append(LogEntry{2, 0, "c"});

  // Follower already has first entry
  follower_storage.Append(LogEntry{1, 0, "a"});

  leader.ReplicateLog();

  EXPECT_EQ(3, follower_storage.LogSize());
  EXPECT_EQ("b", follower_storage.Get(2).command);
  EXPECT_EQ("c", follower_storage.Get(3).command);
}

TEST_F(RaftNodeTest, AppendEntriesRejectsPrevLogMismatch) {
  CommandLog leader_storage, follower_storage;
  RaftNode leader("L1"), follower("F1");
  leader.SetLogStorage(&leader_storage);
  follower.SetLogStorage(&follower_storage);
  leader.AddPeer("F1");

  leader_storage.Append(LogEntry{2, 0, "x"});

  // Follower has a different entry at index 1
  follower_storage.Append(LogEntry{1, 0, "y"});

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
  CommandLog follower_storage;
  RaftNode follower("F1");
  follower.SetLogStorage(&follower_storage);

  follower_storage.Append(LogEntry{1, 0, "a"});
  follower_storage.Append(LogEntry{2, 0, "b"});

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
  EXPECT_EQ("c", follower_storage.Get(3).command);
}

TEST_F(RaftNodeTest, AppendEntriesStaleTermRejected) {
  CommandLog follower_storage;
  RaftNode follower("F1");
  follower.SetLogStorage(&follower_storage);
  follower.BecomeFollower(10);

  AppendEntriesRequest req;
  req.term = 5;  // stale

  AppendEntriesResponse rsp = follower.OnAppendEntries(req);

  EXPECT_FALSE(rsp.success);
  EXPECT_EQ(10u, rsp.term);
}

// --- Commit & Apply tests ---

// Test-only state machine that records applied commands.
class TestStateMachine : public IStateMachine {
 public:
  std::vector<LogEntry> applied;

  ApplyResult Apply(const CommandId*, CmdArgList) override {
    return {ApplyOp::OK, 0};
  }
  void Set(DbIndex, std::string_view, std::string_view) override {}
  bool Del(DbIndex, std::string_view) override { return false; }
  bool Expire(DbIndex, std::string_view, uint64_t) override { return false; }
  OpResult<std::string> Get(DbIndex, std::string_view) override { return OpStatus::KEY_NOTFOUND; }
  size_t DbSize(DbIndex) const override { return 0; }
  void Schedule(DbIndex, std::string_view, std::function<void(EngineShard*)>) override {}

  ApplyResult ApplyLogEntry(const LogEntry& entry) override {
    applied.push_back(entry);
    return {ApplyOp::OK, 1};
  }
};

TEST_F(RaftNodeTest, CommitAdvancesWithMajority) {
  LocalTransport transport;
  CommandLog leader_storage, f1_storage, f2_storage;
  TestStateMachine sm;

  RaftNode leader("L1"), follower1("F1"), follower2("F2");
  leader.SetLogStorage(&leader_storage);
  leader.SetStateMachine(&sm);
  follower1.SetLogStorage(&f1_storage);
  follower2.SetLogStorage(&f2_storage);

  transport.RegisterNode("L1", &leader);
  transport.RegisterNode("F1", &follower1);
  transport.RegisterNode("F2", &follower2);
  leader.SetTransport(&transport);
  leader.AddPeer("F1");
  leader.AddPeer("F2");

  leader_storage.Append(LogEntry{1, 0, "SET a 1"});
  leader_storage.Append(LogEntry{1, 0, "SET b 2"});

  leader.ReplicateLog();

  // 3 nodes: leader + 2 followers → majority = 2
  // All 3 have both entries → commit_index = 2
  EXPECT_EQ(2u, leader.commit_index());
  EXPECT_EQ(2u, leader.last_applied());
  ASSERT_EQ(2u, sm.applied.size());
  EXPECT_EQ("SET a 1", sm.applied[0].command);
  EXPECT_EQ("SET b 2", sm.applied[1].command);
}

TEST_F(RaftNodeTest, CommitStopsWithoutMajority) {
  // 5 nodes: majority = 3. Only 2 peers receive the entry → commit doesn't advance.
  CommandLog leader_storage;
  TestStateMachine sm;

  RaftNode leader("L1");
  leader.SetLogStorage(&leader_storage);
  leader.SetStateMachine(&sm);

  RaftNode p1("P1"), p2("P2"), p3("P3"), p4("P4");
  CommandLog s1, s2, s3, s4;
  p1.SetLogStorage(&s1);
  p2.SetLogStorage(&s2);
  p3.SetLogStorage(&s3);
  p4.SetLogStorage(&s4);
  leader.AddPeer("P1");
  leader.AddPeer("P2");
  leader.AddPeer("P3");
  leader.AddPeer("P4");

  leader_storage.Append(LogEntry{1, 0, "SET a 1"});

  // Manually replicate to only 1 peer (total with leader = 2, majority = 3)
  AppendEntriesRequest req;
  req.term = 1;
  req.leader_id = "L1";
  req.entries = leader_storage.GetRange(1);
  p1.OnAppendEntries(req);

  // Update peer tracking manually
  leader.AdvanceCommitIndex();

  // commit_index should still be 0 since only 2/5 have the entry
  EXPECT_EQ(0u, leader.commit_index());
  EXPECT_EQ(0u, leader.last_applied());
}

TEST_F(RaftNodeTest, FollowerAppliesViaLeaderCommit) {
  CommandLog storage;
  TestStateMachine sm;
  RaftNode follower("F1");
  follower.SetLogStorage(&storage);
  follower.SetStateMachine(&sm);

  storage.Append(LogEntry{1, 0, "SET a 1"});
  storage.Append(LogEntry{1, 0, "SET b 2"});

  // Simulate leader sending AppendEntries with leader_commit=2
  AppendEntriesRequest req;
  req.term = 1;
  req.leader_id = "L1";
  req.leader_commit = 2;

  AppendEntriesResponse rsp = follower.OnAppendEntries(req);

  EXPECT_TRUE(rsp.success);
  EXPECT_EQ(2u, follower.commit_index());
  EXPECT_EQ(2u, follower.last_applied());
  ASSERT_EQ(2u, sm.applied.size());
  EXPECT_EQ("SET a 1", sm.applied[0].command);
  EXPECT_EQ("SET b 2", sm.applied[1].command);
}

// --- ILogStorage mock-based tests ---

// A mock ILogStorage for testing RaftNode interactions.
class MockLogStorage : public ILogStorage {
 public:
  MOCK_METHOD(size_t, LogSize, (), (const, override));
  MOCK_METHOD(LogIndex, LastIndex, (), (const, override));
  MOCK_METHOD(Term, LastTerm, (), (const, override));
  MOCK_METHOD(const LogEntry&, Get, (LogIndex), (const, override));
  MOCK_METHOD(void, Append, (LogEntry), (override));
  MOCK_METHOD(std::vector<LogEntry>, GetRange, (LogIndex, size_t), (const, override));
  MOCK_METHOD(void, TruncateFrom, (LogIndex), (override));
  MOCK_METHOD(void, Clear, (), (override));

  // Convenience helper for GetRange with default limit.
  std::vector<LogEntry> GetRange(LogIndex start) const {
    return GetRange(start, 0);
  }
};

TEST_F(RaftNodeTest, RaftNodeUsesLogStorageInterface) {
  LocalTransport transport;
  MockLogStorage mock_storage;
  CommandLog f1_storage, f2_storage;

  RaftNode node("N1"), f1("F1"), f2("F2");
  node.SetLogStorage(&mock_storage);
  f1.SetLogStorage(&f1_storage);
  f2.SetLogStorage(&f2_storage);

  transport.RegisterNode("N1", &node);
  transport.RegisterNode("F1", &f1);
  transport.RegisterNode("F2", &f2);
  node.SetTransport(&transport);
  node.AddPeer("F1");
  node.AddPeer("F2");

  EXPECT_CALL(mock_storage, LastIndex())
      .WillOnce(Return(5));

  EXPECT_CALL(mock_storage, GetRange(1, 0))
      .WillOnce(Return(std::vector<LogEntry>{
          LogEntry{1, 1, "a"}, LogEntry{1, 2, "b"}, LogEntry{1, 3, "c"},
          LogEntry{2, 4, "d"}, LogEntry{2, 5, "e"}}));

  EXPECT_CALL(mock_storage, LogSize())
      .WillRepeatedly(Return(5));

  node.BecomeCandidate();
  node.BecomeLeader();

  node.ReplicateLog();

  // commit_index should advance (3 nodes, majority=2, all have 5 entries)
  EXPECT_EQ(5u, node.commit_index());

  // Verify peers received the entries
  EXPECT_EQ(5u, f1_storage.LastIndex());
  EXPECT_EQ(5u, f2_storage.LastIndex());
  EXPECT_EQ("e", f1_storage.Get(5).command);
}

TEST_F(RaftNodeTest, MockStorageTruncateOnConflict) {
  MockLogStorage mock_storage;
  RaftNode follower("F1");
  follower.SetLogStorage(&mock_storage);

  // Follower log:
  // idx 1: term 1, "a"
  // idx 2: term 2, "b"
  // idx 3: term 2, "c"
  LogEntry entry1{1, 1, "a"};
  LogEntry entry2{2, 2, "b"};
  LogEntry entry3{2, 3, "c"};

  // Leader sends a heartbeat with new entries.
  // prev_log_index=2, prev_log_term=2 matches follower → pass the check.
  // Then at index 3, leader's entry has term 3 but follower has term 2 → conflict!
  AppendEntriesRequest req;
  req.term = 3;
  req.leader_id = "L1";
  req.prev_log_index = 2;
  req.prev_log_term = 2;  // matches follower's idx 2

  LogEntry leader_entry{3, 3, "SET x 1"};  // term 3 at index 3 — conflicts with follower
  req.entries = {leader_entry};

  EXPECT_CALL(mock_storage, LastIndex())
      .WillRepeatedly(Return(3));

  EXPECT_CALL(mock_storage, Get(1))
      .WillRepeatedly(ReturnRef(entry1));
  EXPECT_CALL(mock_storage, Get(2))
      .WillRepeatedly(ReturnRef(entry2));
  EXPECT_CALL(mock_storage, Get(3))
      .WillRepeatedly(ReturnRef(entry3));

  // Should detect conflict at index 3, truncate to index 2, and append leader entry
  EXPECT_CALL(mock_storage, TruncateFrom(2));
  EXPECT_CALL(mock_storage, Append(_));

  AppendEntriesResponse rsp = follower.OnAppendEntries(req);

  EXPECT_TRUE(rsp.success);
}

// --- New pipeline tests ---

// Helper to create a CmdArgList from string literals for testing.
std::vector<MutableStrSpan> MakeCmdArgs(std::initializer_list<const char*> args) {
  std::vector<MutableStrSpan> result;
  for (auto* s : args) {
    result.emplace_back(const_cast<char*>(s), strlen(s));
  }
  return result;
}

TEST_F(RaftNodeTest, SingleNodeCommitAdvances) {
  CommandLog storage;
  TestStateMachine sm;
  RaftNode node("N1");
  node.SetLogStorage(&storage);
  node.SetStateMachine(&sm);
  node.BecomeCandidate();
  node.BecomeLeader();

  storage.Append(LogEntry{1, 0, "SET a 1"});

  ApplyResult result = node.ReplicateLog();

  EXPECT_EQ(1u, node.commit_index());
  EXPECT_EQ(1u, node.last_applied());
  ASSERT_EQ(1u, sm.applied.size());
  EXPECT_EQ("SET a 1", sm.applied[0].command);
  EXPECT_EQ(ApplyOp::OK, result.op);
  EXPECT_EQ(1u, result.affected_rows);
}

TEST_F(RaftNodeTest, SingleNodeCommitMultipleEntries) {
  CommandLog storage;
  TestStateMachine sm;
  RaftNode node("N1");
  node.SetLogStorage(&storage);
  node.SetStateMachine(&sm);
  node.BecomeCandidate();
  node.BecomeLeader();

  storage.Append(LogEntry{1, 0, "SET a 1"});
  storage.Append(LogEntry{1, 0, "SET b 2"});

  ApplyResult result = node.ReplicateLog();

  EXPECT_EQ(2u, node.commit_index());
  EXPECT_EQ(2u, node.last_applied());
  ASSERT_EQ(2u, sm.applied.size());
  EXPECT_EQ("SET a 1", sm.applied[0].command);
  EXPECT_EQ("SET b 2", sm.applied[1].command);
  EXPECT_EQ(result.op, ApplyOp::OK);
}

TEST_F(RaftNodeTest, SingleNodeCommitFollowerRole) {
  CommandLog storage;
  TestStateMachine sm;
  RaftNode node("N1");
  node.SetLogStorage(&storage);
  node.SetStateMachine(&sm);

  storage.Append(LogEntry{1, 0, "SET a 1"});

  node.ReplicateLog();

  // Single-node with no peers should commit regardless of role.
  EXPECT_EQ(1u, node.commit_index());
  EXPECT_EQ(1u, node.last_applied());
}

TEST_F(RaftNodeTest, CommandEncoderEncodesSET) {
  CommandId set_cmd("SET", CO::WRITE, -3, 1, 1, 1);
  auto args_vec = MakeCmdArgs({"SET", "a", "1"});
  CmdArgList args{args_vec.data(), args_vec.size()};

  auto cmd = CommandEncoder::Encode(&set_cmd, args);
  ASSERT_TRUE(cmd.has_value());
  EXPECT_EQ(CommandType::SET, cmd->type);
  ASSERT_EQ(3u, cmd->args.size());
  EXPECT_EQ("SET", cmd->args[0]);
  EXPECT_EQ("a", cmd->args[1]);
  EXPECT_EQ("1", cmd->args[2]);
  EXPECT_EQ("SET a 1", cmd->Serialize());
}

TEST_F(RaftNodeTest, CommandEncoderEncodesDEL) {
  CommandId del_cmd("DEL", CO::WRITE, -2, 1, 1, 1);
  auto args_vec = MakeCmdArgs({"DEL", "mykey"});
  CmdArgList args{args_vec.data(), args_vec.size()};

  auto cmd = CommandEncoder::Encode(&del_cmd, args);
  ASSERT_TRUE(cmd.has_value());
  EXPECT_EQ(CommandType::DEL, cmd->type);
  ASSERT_EQ(2u, cmd->args.size());
  EXPECT_EQ("DEL", cmd->args[0]);
  EXPECT_EQ("mykey", cmd->args[1]);
  EXPECT_EQ("DEL mykey", cmd->Serialize());
}

TEST_F(RaftNodeTest, CommandEncoderEncodesEXPIRE) {
  CommandId expire_cmd("EXPIRE", CO::WRITE, 3, 1, 1, 1);
  auto args_vec = MakeCmdArgs({"EXPIRE", "a", "10"});
  CmdArgList args{args_vec.data(), args_vec.size()};

  auto cmd = CommandEncoder::Encode(&expire_cmd, args);
  ASSERT_TRUE(cmd.has_value());
  EXPECT_EQ(CommandType::EXPIRE, cmd->type);
  ASSERT_EQ(3u, cmd->args.size());
  EXPECT_EQ("EXPIRE", cmd->args[0]);
  EXPECT_EQ("a", cmd->args[1]);
  EXPECT_EQ("10", cmd->args[2]);
  EXPECT_EQ("EXPIRE a 10", cmd->Serialize());
}

TEST_F(RaftNodeTest, CommandEncoderRejectsReadOnly) {
  CommandId get_cmd("GET", CO::READONLY | CO::FAST, 2, 1, 1, 1);
  auto args_vec = MakeCmdArgs({"GET", "a"});
  CmdArgList args{args_vec.data(), args_vec.size()};

  auto cmd = CommandEncoder::Encode(&get_cmd, args);
  EXPECT_FALSE(cmd.has_value());
}

TEST_F(RaftNodeTest, CommandEncoderRejectsUnknown) {
  CommandId unknown_cmd("UNKNOWN", CO::WRITE, -1, 0, 0, 0);
  auto args_vec = MakeCmdArgs({"UNKNOWN", "arg"});
  CmdArgList args{args_vec.data(), args_vec.size()};

  auto cmd = CommandEncoder::Encode(&unknown_cmd, args);
  EXPECT_FALSE(cmd.has_value());
}

TEST_F(RaftNodeTest, ReplicatedCommandRoundTrip) {
  ReplicatedCommand original;
  original.type = CommandType::SET;
  original.args = {"SET", "a", "1"};

  std::string serialized = original.Serialize();
  EXPECT_EQ("SET a 1", serialized);

  ReplicatedCommand deserialized = ReplicatedCommand::Deserialize(serialized);
  EXPECT_EQ(original.type, deserialized.type);
  ASSERT_EQ(original.args.size(), deserialized.args.size());
  EXPECT_EQ(original.args[0], deserialized.args[0]);
  EXPECT_EQ(original.args[1], deserialized.args[1]);
  EXPECT_EQ(original.args[2], deserialized.args[2]);
}

// ---------------------------------------------------------------------------
// Role transition tests: verify full Follower→Candidate→Leader→Follower cycle
// with current_term, voted_for, and role all checked per transition.
// ---------------------------------------------------------------------------

// Follower receives election timeout → Candidate.
// Verified: role, term incremented, voted_for=self, vote_count=1.
TEST_F(RaftNodeTest, FollowerToCandidate) {
  RaftNode node("n1");
  node.BecomeFollower(5);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(5u, node.term());
  EXPECT_TRUE(node.voted_for().empty());
  EXPECT_EQ(0u, node.vote_count());

  // Election timeout triggers transition.
  node.OnElectionTimeout();

  EXPECT_EQ(RaftRole::Candidate, node.role());
  EXPECT_EQ(6u, node.term());
  EXPECT_EQ("n1", node.voted_for());
  EXPECT_EQ(1u, node.vote_count());
}

// Candidate wins election (single-node self-vote) → Leader.
// Verified: role, term, leader_term.
TEST_F(RaftNodeTest, CandidateToLeader) {
  RaftNode node("n1");
  node.BecomeFollower(0);
  node.StartElection();

  EXPECT_EQ(RaftRole::Leader, node.role());
  EXPECT_EQ(1u, node.term());
  EXPECT_EQ(1u, node.leader_term());
  EXPECT_EQ("n1", node.voted_for());
}

// Leader receives heartbeat with higher term → Follower.
// Verified: role, term updated, voted_for cleared, vote_count reset.
TEST_F(RaftNodeTest, LeaderStepDown) {
  RaftNode node("n1");
  node.BecomeCandidate();
  node.BecomeLeader();
  EXPECT_EQ(RaftRole::Leader, node.role());
  EXPECT_EQ(1u, node.term());
  EXPECT_EQ(1u, node.leader_term());

  // Receive a heartbeat from a leader with higher term.
  HeartbeatRequest req{5, "n2"};
  HeartbeatResponse rsp = node.OnHeartbeat(req);

  EXPECT_TRUE(rsp.success);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(5u, node.term());
  EXPECT_EQ(0u, node.leader_term());
  EXPECT_TRUE(node.voted_for().empty());
  EXPECT_EQ(0u, node.vote_count());
}

// ---------------------------------------------------------------------------
// Cluster tests: 3-node cluster communicating through Transport abstraction.
// ---------------------------------------------------------------------------

// Verifies full election cycle through Transport:
//   1. All 3 nodes start as Follower
//   2. N1.StartElection() → N1 becomes Leader, N2/N3 become Followers
//   3. Heartbeats maintain leadership stability
TEST_F(RaftNodeTest, ThreeNodeClusterElectionAndHeartbeat) {
  LocalTransport transport;
  RaftNode n1("N1"), n2("N2"), n3("N3");

  transport.RegisterNode("N1", &n1);
  transport.RegisterNode("N2", &n2);
  transport.RegisterNode("N3", &n3);
  n1.SetTransport(&transport);
  n2.SetTransport(&transport);
  n3.SetTransport(&transport);
  n1.AddPeer("N2");
  n1.AddPeer("N3");

  // All start as Followers
  EXPECT_EQ(RaftRole::Follower, n1.role());
  EXPECT_EQ(RaftRole::Follower, n2.role());
  EXPECT_EQ(RaftRole::Follower, n3.role());

  // N1 starts election → sends VoteRequests to N2/N3 via Transport
  ElectionResult result = n1.StartElection();

  // 3 nodes: majority = 2. Self(1) + N2(1) + N3(1) = 3 ≥ 2.
  EXPECT_EQ(3u, result.votes_received);
  EXPECT_EQ(0u, result.votes_rejected);
  EXPECT_EQ(RaftRole::Leader, n1.role());
  EXPECT_EQ(1u, n1.term());
  EXPECT_EQ(1u, n1.leader_term());

  // N2 and N3 should have BecomeFollower after receiving VoteRequest
  EXPECT_EQ(RaftRole::Follower, n2.role());
  EXPECT_EQ(1u, n2.term());
  EXPECT_EQ(RaftRole::Follower, n3.role());
  EXPECT_EQ(1u, n3.term());

  // Heartbeats from N1 keep followers stable
  HeartbeatRequest hb{n1.term(), n1.node_id()};
  HeartbeatResponse rsp2 = n2.OnHeartbeat(hb);
  HeartbeatResponse rsp3 = n3.OnHeartbeat(hb);

  EXPECT_TRUE(rsp2.success);
  EXPECT_TRUE(rsp3.success);
  EXPECT_EQ(RaftRole::Follower, n2.role());
  EXPECT_EQ(RaftRole::Follower, n3.role());
  EXPECT_EQ(RaftRole::Leader, n1.role());
}

// Verifies log replication through Transport:
//   1. Leader appends entries
//   2. ReplicateLog sends AppendEntries via Transport to followers
//   3. Followers receive and store entries
//   4. CommitIndex advances on leader
//   5. State machine applies committed entries
TEST_F(RaftNodeTest, ThreeNodeClusterLogReplication) {
  LocalTransport transport;
  CommandLog leader_storage, f1_storage, f2_storage;
  TestStateMachine sm;

  RaftNode leader("L1"), follower1("F1"), follower2("F2");
  leader.SetLogStorage(&leader_storage);
  leader.SetStateMachine(&sm);
  follower1.SetLogStorage(&f1_storage);
  follower2.SetLogStorage(&f2_storage);

  transport.RegisterNode("L1", &leader);
  transport.RegisterNode("F1", &follower1);
  transport.RegisterNode("F2", &follower2);
  leader.SetTransport(&transport);
  follower1.SetTransport(&transport);
  follower2.SetTransport(&transport);
  leader.AddPeer("F1");
  leader.AddPeer("F2");

  // Elect L1 as leader
  ElectionResult election = leader.StartElection();
  ASSERT_EQ(RaftRole::Leader, leader.role());
  ASSERT_EQ(3u, election.votes_received);

  // Append entries and replicate via Transport
  leader_storage.Append(LogEntry{1, 1, "SET a 1"});
  leader_storage.Append(LogEntry{1, 2, "SET b 2"});

  leader.ReplicateLog();

  // Followers received entries via Transport->SendAppendEntries
  EXPECT_EQ(2u, f1_storage.LogSize());
  EXPECT_EQ(2u, f2_storage.LogSize());
  EXPECT_EQ("SET a 1", f1_storage.Get(1).command);
  EXPECT_EQ("SET b 2", f1_storage.Get(2).command);
  EXPECT_EQ("SET a 1", f2_storage.Get(1).command);
  EXPECT_EQ("SET b 2", f2_storage.Get(2).command);

  // CommitIndex advanced (majority = 2, all 3 nodes have entries)
  EXPECT_EQ(2u, leader.commit_index());
  EXPECT_EQ(2u, leader.last_applied());
  ASSERT_EQ(2u, sm.applied.size());
  EXPECT_EQ("SET a 1", sm.applied[0].command);
  EXPECT_EQ("SET b 2", sm.applied[1].command);
}

// Verifies leader re-election via Transport after the original leader steps down:
//   1. Elect N1 as leader (term 1)
//   2. N1 steps down after receiving OnHeartbeat with higher term (5)
//   3. N2 starts election (term 2), gets vote from N3 but rejection from N1 (term 5)
//   4. N2 reaches majority (self + N3 = 2 ≥ 2) and becomes Leader(2)
//   5. N2 replicates log entry — N3 accepts, N1 rejects (higher term)
//   6. Majority (N2 + N3) reached → commit_index advances
TEST_F(RaftNodeTest, ThreeNodeClusterLeaderTransition) {
  LocalTransport transport;
  CommandLog l1_storage, l2_storage, l3_storage;
  TestStateMachine sm2, sm3;

  RaftNode n1("N1"), n2("N2"), n3("N3");
  n1.SetLogStorage(&l1_storage);
  n2.SetLogStorage(&l2_storage);
  n2.SetStateMachine(&sm2);
  n3.SetLogStorage(&l3_storage);
  n3.SetStateMachine(&sm3);

  transport.RegisterNode("N1", &n1);
  transport.RegisterNode("N2", &n2);
  transport.RegisterNode("N3", &n3);
  n1.SetTransport(&transport);
  n2.SetTransport(&transport);
  n3.SetTransport(&transport);
  n1.AddPeer("N2");
  n1.AddPeer("N3");
  n2.AddPeer("N1");
  n2.AddPeer("N3");
  n3.AddPeer("N1");
  n3.AddPeer("N2");

  // Step 1: Elect N1 as leader
  ElectionResult e1 = n1.StartElection();
  ASSERT_EQ(RaftRole::Leader, n1.role());
  EXPECT_EQ(3u, e1.votes_received);
  EXPECT_EQ(1u, n1.term());
  EXPECT_EQ(1u, n2.term());
  EXPECT_EQ(1u, n3.term());

  // Step 2: N1 steps down on higher term heartbeat (term 5)
  n1.OnHeartbeat(HeartbeatRequest{5, "N2"});
  EXPECT_EQ(RaftRole::Follower, n1.role());
  EXPECT_EQ(5u, n1.term());

  // Step 3: N2 starts election (term 2)
  // N1 (term 5) rejects, N3 (term 1) grants
  ElectionResult e2 = n2.StartElection();
  EXPECT_EQ(RaftRole::Leader, n2.role());
  EXPECT_EQ(2u, n2.term());  // N2's own term
  EXPECT_EQ(5u, n1.term());  // N1 unchanged (higher term)
  EXPECT_EQ(2u, n3.term());  // N3 updated by vote request
  EXPECT_EQ(2u, e2.votes_received);  // self + N3
  EXPECT_EQ(1u, e2.votes_rejected);  // N1

  // N1 and N3 are followers
  EXPECT_EQ(RaftRole::Follower, n1.role());
  EXPECT_EQ(RaftRole::Follower, n3.role());

  // Step 5: New leader replicates log entry
  // N3 accepts, N1 rejects (higher term 5 > 2)
  l2_storage.Append(LogEntry{2, 1, "SET x 1"});
  n2.ReplicateLog();

  // N3 received the entry via transport
  EXPECT_EQ(1u, l3_storage.LogSize());
  EXPECT_EQ("SET x 1", l3_storage.Get(1).command);

  // N1 rejected (higher term)
  EXPECT_EQ(0u, l1_storage.LogSize());

  // Majority (N2 + N3 = 2/3) reached → commit_index advances
  EXPECT_EQ(1u, n2.commit_index());
  EXPECT_EQ(1u, n2.last_applied());
}

}  // namespace dfly
