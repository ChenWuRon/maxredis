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

  // Rule 4: Election restriction (§5.4.1).
  // Candidate's log must be at least as up-to-date as receiver's log.
  // If no log storage is set, treat the local log as empty.
  Term local_last_term = log_storage_ ? log_storage_->LastTerm() : 0;
  LogIndex local_last_index = log_storage_ ? log_storage_->LastIndex() : 0;

  if (request.last_log_term < local_last_term) {
    return {term_, false};
  }
  if (request.last_log_term == local_last_term && request.last_log_index < local_last_index) {
    return {term_, false};
  }

  // Rule 5: Grant vote.
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
  request.last_log_index = log_storage_ ? log_storage_->LastIndex() : 0;
  request.last_log_term = log_storage_ ? log_storage_->LastTerm() : 0;

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

AppendEntriesResponse RaftNode::OnAppendEntries(const AppendEntriesRequest& req) {
  // Rule 1: Stale term — reject.
  if (req.term < term_) {
    LogIndex my_last = log_storage_ ? log_storage_->LastIndex() : 0;
    return {term_, false, my_last};
  }

  // Rule 2: Valid heartbeat — become follower (resets election timer).
  if (req.term >= term_) {
    BecomeFollower(req.term);
  }

  // Rule 3: Check prev_log_index / prev_log_term match.
  if (log_storage_) {
    if (req.prev_log_index > log_storage_->LastIndex()) {
      return {term_, false, log_storage_->LastIndex()};
    }
    if (req.prev_log_index > 0 &&
        log_storage_->Get(req.prev_log_index).term != req.prev_log_term) {
      return {term_, false, req.prev_log_index - 1};
    }

    // Rule 4: Append entries with index matching.
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

  // Follower applies entries up to leader_commit.
  if (req.leader_commit > commit_index_) {
    commit_index_ = std::min(req.leader_commit, my_last);
    ApplyCommittedLogs();
  }

  return {term_, true, my_last};
}

void RaftNode::ReplicateLog() {
  if (!log_storage_ || peers_.empty())
    return;

  AppendEntriesRequest req;
  req.term = term_;
  req.leader_id = node_id_;
  req.leader_commit = commit_index_;

  // Send all entries from the beginning.
  size_t log_size = log_storage_->LogSize();
  if (log_size > 0) {
    req.entries = log_storage_->GetRange(1);
  }

  peer_last_log_index_.resize(peers_.size());
  for (size_t i = 0; i < peers_.size(); i++) {
    AppendEntriesResponse rsp = peers_[i]->OnAppendEntries(req);
    peer_last_log_index_[i] = rsp.last_log_index;
  }

  AdvanceCommitIndex();
  ApplyCommittedLogs();
}

void RaftNode::AdvanceCommitIndex() {
  if (!log_storage_)
    return;

  std::vector<LogIndex> indexes;
  indexes.push_back(log_storage_->LastIndex());
  for (auto idx : peer_last_log_index_) {
    indexes.push_back(idx);
  }

  std::sort(indexes.rbegin(), indexes.rend());
  size_t total = peers_.size() + 1;
  size_t majority = total / 2 + 1;
  LogIndex candidate = indexes[majority - 1];

  if (candidate > commit_index_) {
    commit_index_ = candidate;
  }
}

void RaftNode::ApplyCommittedLogs() {
  if (!state_machine_ || !log_storage_)
    return;

  while (last_applied_ < commit_index_ && last_applied_ < log_storage_->LastIndex()) {
    last_applied_++;
    const LogEntry& entry = log_storage_->Get(last_applied_);
    state_machine_->ApplyLogEntry(entry);
  }
}

}  // namespace dfly
