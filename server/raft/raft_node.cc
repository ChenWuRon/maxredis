// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_node.h"

#include <algorithm>

#include "base/logging.h"
#include "server/state_machine/state_machine.h"

namespace dfly {

RaftNode::RaftNode(NodeId node_id) : node_id_(std::move(node_id)) {
}

RaftNode::~RaftNode() {
  StopHeartbeat();
  election_timer_.Stop();
}

void RaftNode::SetRole(RaftRole new_role) {
  RaftRole old_role = role_;
  role_ = new_role;

  VLOG(1) << "RaftNode " << node_id_ << ": " << old_role << " -> " << new_role;

  switch (new_role) {
    case RaftRole::Follower:
      leader_term_ = 0;
      voted_for_.clear();
      vote_count_ = 0;
      if (!election_started_) {
        election_started_ = true;
        election_timer_.Start([this] { StartElection(); });
      }
      election_timer_.Reset();
      StopHeartbeat();
      break;
    case RaftRole::Candidate:
      term_++;
      voted_for_ = node_id_;
      vote_count_ = 1;
      election_timer_.Deactivate();
      StopHeartbeat();
      break;
    case RaftRole::Leader:
      leader_term_ = term_;
      election_timer_.Deactivate();
      StopHeartbeat();
      StartHeartbeat(heartbeat_interval_ms_);
      break;
  }
}

void RaftNode::BecomeFollower(Term term) {
  DCHECK_GE(term, term_);
  term_ = term;
  SetRole(RaftRole::Follower);
}

void RaftNode::BecomeCandidate() {
  SetRole(RaftRole::Candidate);
}

void RaftNode::BecomeLeader() {
  DCHECK_EQ(role_, RaftRole::Candidate);
  SetRole(RaftRole::Leader);
}

void RaftNode::OnElectionTimeout() {
  if (role_ != RaftRole::Follower)
    return;
  BecomeCandidate();
}

VoteResponse RaftNode::OnRequestVote(const VoteRequest& request) {
  if (request.term < term_) {
    return {term_, false};
  }

  if (request.term > term_) {
    BecomeFollower(request.term);
  }

  if (!voted_for_.empty() && voted_for_ != request.candidate_id) {
    return {term_, false};
  }

  Term local_last_term = log_storage_ ? log_storage_->LastTerm() : 0;
  LogIndex local_last_index = log_storage_ ? log_storage_->LastIndex() : 0;

  if (request.last_log_term < local_last_term) {
    return {term_, false};
  }
  if (request.last_log_term == local_last_term && request.last_log_index < local_last_index) {
    return {term_, false};
  }

  voted_for_ = request.candidate_id;
  return {term_, true};
}

ElectionResult RaftNode::StartElection() {
  BecomeCandidate();

  VoteRequest request;
  request.term = term_;
  request.candidate_id = node_id_;
  request.last_log_index = log_storage_ ? log_storage_->LastIndex() : 0;
  request.last_log_term = log_storage_ ? log_storage_->LastTerm() : 0;

  ElectionResult result;
  result.votes_received = vote_count_;

  for (const auto& peer_id : peer_manager_.GetPeerIds()) {
    DCHECK(transport_) << "Transport not set for multi-node operation";
    VoteResponse rsp = transport_->SendVoteRequest(peer_id, request);
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
  size_t total_nodes = peer_manager_.ClusterSize();
  size_t majority = total_nodes / 2 + 1;

  if (result.votes_received >= majority) {
    BecomeLeader();
    return true;
  }
  return false;
}

HeartbeatResponse RaftNode::OnHeartbeat(const HeartbeatRequest& request) {
  if (request.term < term_) {
    return {term_, false};
  }

  if (request.term >= term_) {
    BecomeFollower(request.term);
  }

  election_timer_.Reset();

  return {term_, true};
}

void RaftNode::SendHeartbeatToPeers() {
  HeartbeatRequest req{term_, node_id_};
  for (const auto& peer_id : peer_manager_.GetPeerIds()) {
    DCHECK(transport_) << "Transport not set for multi-node operation";
    transport_->SendHeartbeat(peer_id, req);
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

AppendEntriesResponse RaftNode::OnAppendEntries(const AppendEntriesRequest& req) {
  if (req.term < term_) {
    LogIndex my_last = log_storage_ ? log_storage_->LastIndex() : 0;
    return {term_, false, my_last};
  }

  if (req.term >= term_) {
    BecomeFollower(req.term);
  }

  if (log_storage_) {
    if (req.prev_log_index > log_storage_->LastIndex()) {
      return {term_, false, log_storage_->LastIndex()};
    }
    if (req.prev_log_index > 0 &&
        log_storage_->Get(req.prev_log_index).term != req.prev_log_term) {
      return {term_, false, req.prev_log_index - 1};
    }

    for (const auto& entry : req.entries) {
      if (entry.index <= log_storage_->LastIndex()) {
        if (log_storage_->Get(entry.index).term != entry.term) {
          log_storage_->TruncateFrom(entry.index - 1);
          log_storage_->Append(entry);
        }
      } else if (entry.index == log_storage_->LastIndex() + 1) {
        log_storage_->Append(entry);
      }
    }
  }

  LogIndex my_last = log_storage_ ? log_storage_->LastIndex() : 0;

  if (req.leader_commit > commit_index_) {
    commit_index_ = std::min(req.leader_commit, my_last);
    ApplyCommittedLogs();
  }

  return {term_, true, my_last};
}

ApplyResult RaftNode::ReplicateLog() {
  if (!log_storage_)
    return {};

  if (peer_manager_.PeerCount() == 0) {
    if (commit_index_ < log_storage_->LastIndex()) {
      commit_index_ = log_storage_->LastIndex();
    }
    return ApplyCommittedLogs();
  }

  AppendEntriesRequest req;
  req.term = term_;
  req.leader_id = node_id_;
  req.leader_commit = commit_index_;

  size_t log_size = log_storage_->LogSize();
  if (log_size > 0) {
    req.entries = log_storage_->GetRange(1);
  }

  auto ids = peer_manager_.GetPeerIds();
  peer_last_log_index_.resize(ids.size());
  for (size_t i = 0; i < ids.size(); i++) {
    DCHECK(transport_) << "Transport not set for multi-node operation";
    AppendEntriesResponse rsp = transport_->SendAppendEntries(ids[i], req);
    peer_last_log_index_[i] = rsp.last_log_index;
  }

  AdvanceCommitIndex();
  return ApplyCommittedLogs();
}

void RaftNode::AdvanceCommitIndex() {
  if (!log_storage_)
    return;

  std::vector<LogIndex> indexes;
  indexes.push_back(log_storage_->LastIndex());
  for (auto idx : peer_last_log_index_) {
    indexes.push_back(idx);
  }

  if (indexes.empty())
    return;

  std::sort(indexes.rbegin(), indexes.rend());
  size_t total = peer_manager_.ClusterSize();
  size_t majority = total / 2 + 1;

  if (majority - 1 >= indexes.size())
    return;

  LogIndex candidate = indexes[majority - 1];

  if (candidate > commit_index_) {
    commit_index_ = candidate;
  }
}

ApplyResult RaftNode::ApplyCommittedLogs() {
  ApplyResult result;
  if (!state_machine_ || !log_storage_)
    return result;

  while (last_applied_ < commit_index_ && last_applied_ < log_storage_->LastIndex()) {
    last_applied_++;
    const LogEntry& entry = log_storage_->Get(last_applied_);
    result = state_machine_->ApplyLogEntry(entry);
  }
  return result;
}

}  // namespace dfly
