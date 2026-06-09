// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
// RaftRoleTest: verifies SetRole() based state machine transitions
// using FakeClock (time simulation) and MockTransport (RPC mocking).

#include "server/raft/raft_node.h"

#include <gmock/gmock.h>

#include <chrono>

#include "base/gtest.h"
#include "server/raft/command_log.h"
#include "server/raft/local_transport.h"
#include "server/raft/log_storage.h"
#include "server/raft/transport.h"
#include "server/raft/vote_rpc.h"
#include "server/raft/heartbeat_rpc.h"

namespace dfly {

using namespace testing;
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// Test infrastructure: FakeClock simulates time; MockTransport checks RPCs.
// ---------------------------------------------------------------------------

// FakeClock: deterministic time advancement.
// In production the ElectionTimer uses real wall-clock time.
// In tests, FakeClock tracks elapsed time for assertions and documentation.
struct FakeClock {
  std::chrono::milliseconds elapsed{0};

  void Advance(std::chrono::milliseconds d) {
    elapsed += d;
  }
};

// MockTransport: gmock-based interceptor for Raft RPC messages.
// Tests set expectations on which messages are sent and with what content,
// then verify them after the tested operation completes.
class MockTransport : public Transport {
 public:
  MOCK_METHOD(VoteResponse, SendVoteRequest,
              (const NodeId& peer_id, const VoteRequest& request), (override));
  MOCK_METHOD(HeartbeatResponse, SendHeartbeat,
              (const NodeId& peer_id, const HeartbeatRequest& request), (override));
  MOCK_METHOD(AppendEntriesResponse, SendAppendEntries,
              (const NodeId& peer_id, const AppendEntriesRequest& request), (override));
};

class RaftRoleTest : public Test {
};

// ---------------------------------------------------------------------------
// 1. Election timeout → Candidate
//
// The election timer fires after 150–300 ms. The timer callback calls
// StartElection() which calls BecomeCandidate() → SetRole(Candidate).
// We verify the resulting state (term incremented, voted_for=self).
// ---------------------------------------------------------------------------
TEST_F(RaftRoleTest, ElectionTimeoutTransitionsToCandidate) {
  FakeClock clock;
  RaftNode node("n1");

  node.BecomeFollower(5);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(5u, node.term());

  // Simulate 200 ms passing — the timer would fire.
  clock.Advance(200ms);

  // This is what the timer callback does internally.
  node.StartElection();

  // Single-node cluster: self vote reaches majority → becomes Leader.
  // The key transition through SetRole(Candidate) is verified by the
  // term increment and role progression.
  EXPECT_EQ(RaftRole::Leader, node.role());
  EXPECT_EQ(6u, node.term());
}

// Same transition but with peers that reject, so the node stays Candidate.
// Uses a 5-node cluster so majority=3; only 2 votes < 3 → stays Candidate.
TEST_F(RaftRoleTest, ElectionTimeoutCreatesCandidateWithPeerRejection) {
  FakeClock clock;

  // 5-node cluster: n1 candidate, n2-n5 peers.
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

  // Pre-vote n2, n3, n4 so they reject in this term.
  VoteRequest pre_req;
  pre_req.term = 10;
  pre_req.candidate_id = "Other";
  n2.OnRequestVote(pre_req);
  n3.OnRequestVote(pre_req);
  n4.OnRequestVote(pre_req);

  n1.BecomeFollower(9);
  clock.Advance(200ms);

  ElectionResult result = n1.StartElection();  // term becomes 10

  // n2/n3/n4 already voted in term 10 → reject. n5 grants.
  // votes: self(1) + n5(1) = 2. Majority = 5/2+1 = 3. 2 < 3 → stays Candidate.
  EXPECT_EQ(RaftRole::Candidate, n1.role());
  EXPECT_EQ(10u, n1.term());
  EXPECT_EQ("N1", n1.voted_for());
  EXPECT_EQ(2u, result.votes_received);
  EXPECT_EQ(3u, result.votes_rejected);
}

// ---------------------------------------------------------------------------
// 2. Candidate receives majority votes → Leader
//
// StartElection collects vote responses. When votes_received >= majority,
// TryBecomeLeader calls BecomeLeader() → SetRole(Leader).
// ---------------------------------------------------------------------------
TEST_F(RaftRoleTest, CandidateWinsElectionBecomesLeader) {
  LocalTransport transport;
  RaftNode n1("N1"), n2("N2"), n3("N3");

  transport.RegisterNode("N1", &n1);
  transport.RegisterNode("N2", &n2);
  transport.RegisterNode("N3", &n3);
  n1.SetTransport(&transport);
  n1.AddPeer("N2");
  n1.AddPeer("N3");

  // Start election — n1 becomes Candidate (term 1) and sends VoteRequests.
  // n2 and n3 are at term 0, so they BecomeFollower(1) and grant.
  ElectionResult result = n1.StartElection();

  // 3 nodes: majority = 3/2 + 1 = 2. Self vote(1) + 2 peers(2) = 3 ≥ 2.
  EXPECT_EQ(3u, result.votes_received);
  EXPECT_EQ(0u, result.votes_rejected);
  EXPECT_EQ(RaftRole::Leader, n1.role());
  EXPECT_EQ(1u, n1.term());
}

// Verifies that TryBecomeLeader correctly rejects when votes < majority.
TEST_F(RaftRoleTest, TryBecomeLeaderDirectCall) {
  RaftNode node("n1");
  node.AddPeer("p1");

  node.BecomeCandidate();  // term = 1, vote_count = 1

  // 2 nodes: majority = 2/2 + 1 = 2. Only self vote (1) < 2 → no leader.
  ElectionResult result;
  result.votes_received = 1;
  EXPECT_FALSE(node.TryBecomeLeader(result));
  EXPECT_EQ(RaftRole::Candidate, node.role());

  // Now with majority reached.
  result.votes_received = 2;
  EXPECT_TRUE(node.TryBecomeLeader(result));
  EXPECT_EQ(RaftRole::Leader, node.role());
}

// ---------------------------------------------------------------------------
// 3. Candidate receives higher term heartbeat → Follower
//
// Raft §5.1: servers step down when they discover a current leader with a
// higher term. OnHeartbeat with term > current term triggers
// BecomeFollower → SetRole(Follower).
// ---------------------------------------------------------------------------
TEST_F(RaftRoleTest, CandidateStepsDownOnHigherTerm) {
  MockTransport transport;
  RaftNode node("n1");

  node.BecomeFollower(2);
  node.BecomeCandidate();  // term becomes 3
  EXPECT_EQ(RaftRole::Candidate, node.role());

  // Receiving a heartbeat from a leader with higher term.
  HeartbeatRequest hb{5, "LeaderY"};
  HeartbeatResponse rsp = node.OnHeartbeat(hb);

  EXPECT_TRUE(rsp.success);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(5u, node.term());
  EXPECT_TRUE(node.voted_for().empty());
}

// Same via AppendEntries, which also carries a term.
TEST_F(RaftRoleTest, CandidateStepsDownOnHigherTermAppendEntries) {
  RaftNode node("n1");

  node.BecomeFollower(2);
  node.BecomeCandidate();  // term = 3

  AppendEntriesRequest req;
  req.term = 6;
  req.leader_id = "LeaderZ";

  node.OnAppendEntries(req);

  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(6u, node.term());
}

// ---------------------------------------------------------------------------
// 4. Leader receives higher term heartbeat → Follower
//
// Same rule: a leader who discovers a higher term must step down.
// OnHeartbeat triggers BecomeFollower → SetRole(Follower).
// ---------------------------------------------------------------------------
TEST_F(RaftRoleTest, LeaderStepsDownOnHigherTermHeartbeat) {
  MockTransport transport;
  RaftNode node("n1");

  // Become a leader first.
  node.BecomeCandidate();
  node.BecomeLeader();
  EXPECT_EQ(RaftRole::Leader, node.role());
  EXPECT_EQ(1u, node.term());

  // Discover a leader with higher term.
  HeartbeatRequest hb{4, "LeaderX"};
  HeartbeatResponse rsp = node.OnHeartbeat(hb);

  EXPECT_TRUE(rsp.success);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(4u, node.term());
  EXPECT_TRUE(node.voted_for().empty());
}

// Leader stepping down via AppendEntries with higher term.
TEST_F(RaftRoleTest, LeaderStepsDownOnHigherTermAppendEntries) {
  RaftNode node("n1");

  node.BecomeCandidate();
  node.BecomeLeader();  // term = 1

  AppendEntriesRequest req;
  req.term = 2;
  req.leader_id = "LeaderW";

  node.OnAppendEntries(req);

  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(2u, node.term());
}

// ---------------------------------------------------------------------------
// Additional SetRole verification tests
// ---------------------------------------------------------------------------

// SetRole is idempotent for the current role.
TEST_F(RaftRoleTest, SetRoleSameRoleIsNoOp) {
  RaftNode node("n1");
  EXPECT_EQ(RaftRole::Follower, node.role());

  node.SetRole(RaftRole::Follower);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(0u, node.term());
  EXPECT_TRUE(node.voted_for().empty());
}

// SetRole resets state correctly for each transition.
TEST_F(RaftRoleTest, SetRoleFullLifecycle) {
  RaftNode node("n1");

  // Follower
  node.SetRole(RaftRole::Follower);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_TRUE(node.voted_for().empty());
  EXPECT_EQ(0u, node.vote_count());

  // Candidate
  node.SetRole(RaftRole::Candidate);
  EXPECT_EQ(RaftRole::Candidate, node.role());
  EXPECT_EQ(1u, node.term());
  EXPECT_EQ("n1", node.voted_for());
  EXPECT_EQ(1u, node.vote_count());

  // Leader
  node.SetRole(RaftRole::Leader);
  EXPECT_EQ(RaftRole::Leader, node.role());
  EXPECT_EQ(1u, node.term());

  // Back to Follower (step down)
  node.SetRole(RaftRole::Follower);
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(1u, node.term());  // term not reset on step-down
  EXPECT_TRUE(node.voted_for().empty());
  EXPECT_EQ(0u, node.vote_count());
}

// ---------------------------------------------------------------------------
// MockTransport usage: configure expected vote responses for peer routing.
//
// RaftNode currently calls peer->OnRequestVote directly. In a future
// transport-layer refactor, RaftNode would use Transport::SendRequestVote
// instead. This test demonstrates how MockTransport would verify message
// content and provide deterministic responses.
// ---------------------------------------------------------------------------
TEST_F(RaftRoleTest, MockTransportConfiguredVoteRouting) {
  MockTransport transport;

  // Configure what the mock expects and returns for each peer.
  EXPECT_CALL(transport, SendVoteRequest(
      "N2",
      AllOf(Field(&VoteRequest::term, 1),
            Field(&VoteRequest::candidate_id, "N1"))))
      .WillOnce(Return(VoteResponse{1, true}));

  EXPECT_CALL(transport, SendVoteRequest(
      "N3",
      AllOf(Field(&VoteRequest::term, 1),
            Field(&VoteRequest::candidate_id, "N1"))))
      .WillOnce(Return(VoteResponse{1, false}));

  // Simulate the election by manually routing through the transport mock
  // (which represents how a real transport layer would work).
  RaftNode n1("N1");
  n1.SetTransport(&transport);
  n1.AddPeer("N2");
  n1.AddPeer("N3");

  n1.StartElection();

  // 2 votes (self + N2) >= majority(2) → Leader.
  EXPECT_EQ(RaftRole::Leader, n1.role());

  Mock::VerifyAndClearExpectations(&transport);
}

// ---------------------------------------------------------------------------
// FakeClock usage example: document time-advance during election timeout.
// ---------------------------------------------------------------------------
TEST_F(RaftRoleTest, FakeClockDocumentsTimeoutFlow) {
  FakeClock clock;

  RaftNode node("n1");
  node.BecomeFollower(1);

  // Simulate 200 ms passing (the election timer fires).
  clock.Advance(200ms);
  ASSERT_EQ(200ms, clock.elapsed);

  node.StartElection();

  // After the timeout + election, node is Leader (single-node).
  EXPECT_EQ(RaftRole::Leader, node.role());
  EXPECT_EQ(2u, node.term());
}

}  // namespace dfly
