// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_node.h"

#include "base/logging.h"

namespace dfly {

RaftNode::RaftNode(NodeId node_id) : node_id_(std::move(node_id)) {
}

void RaftNode::BecomeFollower(Term term) {
  DCHECK_GE(term, term_);
  term_ = term;
  role_ = RaftRole::Follower;
  leader_term_ = 0;
  voted_for_.clear();
  vote_count_ = 0;
}

void RaftNode::OnElectionTimeout() {
  if (role_ != RaftRole::Follower)
    return;
  BecomeCandidate();
}

void RaftNode::BecomeCandidate() {
  term_++;
  role_ = RaftRole::Candidate;
  voted_for_ = node_id_;
  vote_count_ = 1;
}

void RaftNode::BecomeLeader() {
  DCHECK_EQ(role_, RaftRole::Candidate);
  role_ = RaftRole::Leader;
  leader_term_ = term_;
}

VoteResponse RaftNode::OnRequestVote(const VoteRequest& request) {
  // Rule 1: Stale term — reject.
  if (request.term < term_) {
    return {term_, false};
  }

  // Rule 2: Higher term — update local state and become follower.
  if (request.term > term_) {
    BecomeFollower(request.term);
  }

  // After rule 2, term_ == request.term.

  // Rule 3: Already voted for another candidate in this term.
  if (!voted_for_.empty() && voted_for_ != request.candidate_id) {
    return {term_, false};
  }

  // Rule 4: Grant vote.
  voted_for_ = request.candidate_id;
  return {term_, true};
}

void RaftNode::AddPeer(RaftNode* peer) {
  peers_.push_back(peer);
}

ElectionResult RaftNode::StartElection() {
  BecomeCandidate();

  VoteRequest request;
  request.term = term_;
  request.candidate_id = node_id_;
  request.last_log_index = 0;
  request.last_log_term = 0;

  ElectionResult result;
  result.votes_received = vote_count_;  // self vote

  for (RaftNode* peer : peers_) {
    VoteResponse rsp = peer->OnRequestVote(request);
    if (rsp.vote_granted) {
      result.votes_received++;
    } else {
      result.votes_rejected++;
    }
  }

  vote_count_ = result.votes_received;
  TryBecomeLeader(result);
  return result;
}

bool RaftNode::TryBecomeLeader(const ElectionResult& result) {
  size_t total_nodes = peers_.size() + 1;  // self + peers
  size_t majority = total_nodes / 2 + 1;

  if (result.votes_received >= majority) {
    BecomeLeader();
    return true;
  }
  return false;
}

HeartbeatResponse RaftNode::OnHeartbeat(const HeartbeatRequest& request) {
  // Rule 1: Stale term — reject.
  if (request.term < term_) {
    return {term_, false};
  }

  // Rule 2: Valid heartbeat — update state and become follower.
  if (request.term >= term_) {
    BecomeFollower(request.term);
  }

  return {term_, true};
}

void RaftNode::SendHeartbeatToPeers() {
  HeartbeatRequest req{term_, node_id_};
  for (RaftNode* peer : peers_) {
    peer->OnHeartbeat(req);
  }
}

void RaftNode::StartHeartbeat(uint32_t interval_ms) {
  heartbeat_interval_ms_ = interval_ms;
  if (heartbeat_fiber_.IsJoinable())
    return;
  shutdown_.store(false, std::memory_order_release);
  heartbeat_fiber_ = util::fb2::Fiber("heartbeat", [this] { HeartbeatLoop(); });
}

void RaftNode::StopHeartbeat() {
  shutdown_.store(true, std::memory_order_release);
  if (heartbeat_fiber_.IsJoinable())
    heartbeat_fiber_.Join();
}

void RaftNode::HeartbeatLoop() {
  while (!shutdown_.load(std::memory_order_acquire) && role_ == RaftRole::Leader) {
    SendHeartbeatToPeers();
    util::ThisFiber::SleepFor(std::chrono::milliseconds(heartbeat_interval_ms_));
  }
}

}  // namespace dfly
