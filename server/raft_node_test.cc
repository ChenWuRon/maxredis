// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_node.h"

#include <gmock/gmock.h>

#include <string>
#include <vector>

#include "base/gtest.h"
#include "server/raft/command_log.h"
#include "server/raft/log_storage.h"
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
  CommandLog leader_storage, follower_storage;
  RaftNode leader("L1"), follower("F1");
  leader.SetLogStorage(&leader_storage);
  follower.SetLogStorage(&follower_storage);
  leader.AddPeer(&follower);

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
  CommandLog leader_storage, follower_storage;
  RaftNode leader("L1"), follower("F1");
  leader.SetLogStorage(&leader_storage);
  follower.SetLogStorage(&follower_storage);
  leader.AddPeer(&follower);

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
  leader.AddPeer(&follower);

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
  CommandLog leader_storage, f1_storage, f2_storage;
  TestStateMachine sm;

  RaftNode leader("L1"), follower1("F1"), follower2("F2");
  leader.SetLogStorage(&leader_storage);
  leader.SetStateMachine(&sm);
  follower1.SetLogStorage(&f1_storage);
  follower2.SetLogStorage(&f2_storage);
  leader.AddPeer(&follower1);
  leader.AddPeer(&follower2);

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
  leader.AddPeer(&p1);
  leader.AddPeer(&p2);
  leader.AddPeer(&p3);
  leader.AddPeer(&p4);

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
  MockLogStorage mock_storage;
  CommandLog f1_storage, f2_storage;

  RaftNode node("N1"), f1("F1"), f2("F2");
  node.SetLogStorage(&mock_storage);
  f1.SetLogStorage(&f1_storage);
  f2.SetLogStorage(&f2_storage);
  node.AddPeer(&f1);
  node.AddPeer(&f2);

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

}  // namespace dfly
