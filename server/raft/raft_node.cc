// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_node.h"

#include <algorithm>

#include "base/logging.h"
#include "server/raft/apply_progress.h"
#include "server/raft/replicated_command.h"
#include "server/raft/snapshot_loader.h"
#include "server/state_machine/state_machine.h"

namespace dfly {

RaftNode::RaftNode(NodeId node_id) : node_id_(std::move(node_id)) {
}

void RaftNode::SetStoragePath(std::string path) {
  // path is the directory for Raft metadata files (e.g. "data/raft").
  // Ensure trailing slash for consistent path construction.
  if (!path.empty() && path.back() != '/')
    path += '/';

  storage_ = RaftStorage(path + "meta.json");
  storage_.Load();

  apply_progress_ = ApplyProgress(path + "apply.meta");
  apply_progress_.Load();
  last_applied_ = apply_progress_.LastApplied();

  // Recover from snapshot if one exists.
  // This must happen after state_machine_ is set.
  if (state_machine_) {
    SnapshotLoader loader(path);
    LoadedSnapshot loaded;
    if (loader.Load(&loaded) == SnapshotLoadStatus::OK) {
      LOG(INFO) << node_id_ << " RecoverFromSnapshot: index=" << loaded.meta.index
                << " term=" << loaded.meta.term;
      if (state_machine_->LoadSnapshot(loaded.bin_path)) {
        last_applied_ = std::max(last_applied_, loaded.meta.index);
        last_snapshot_index_ = loaded.meta.index;
        last_snapshot_term_ = loaded.meta.term;
        apply_progress_.Update(last_applied_);
      }
    }
  }

  VLOG(1) << node_id_ << " SetStoragePath: last_applied=" << last_applied_;
}

RaftNode::~RaftNode() {
  StopHeartbeat();
  election_timer_.Stop();
}

std::vector<NodeId> RaftNode::GetPeerIds() const {
  if (config_state_ == ConfigState::kJoint) {
    std::unordered_set<NodeId> all;
    all.insert(joint_config_.old_config.voters.begin(),
               joint_config_.old_config.voters.end());
    all.insert(joint_config_.new_config.voters.begin(),
               joint_config_.new_config.voters.end());
    return {all.begin(), all.end()};
  }
  return {cluster_config_.voters.begin(), cluster_config_.voters.end()};
}

bool RaftNode::BeginConfigChange(ClusterConfig target) {
  if (role_ != RaftRole::Leader || !log_storage_)
    return false;

  if (config_state_ == ConfigState::kJoint) {
    // Step 2: finalize — append the second config entry
    if (target.voters != joint_config_.new_config.voters ||
        target.learners != joint_config_.new_config.learners ||
        target.version != joint_config_.new_config.version) {
      return false;
    }
    ConfigChangeCommand cmd{target};
    log_storage_->Append(LogEntry(term(), 0, cmd.Serialize()));
    VLOG(1) << node_id_ << " BeginConfigChange step 2: append final config version="
            << target.version;
    return true;
  }

  // Step 1: store joint config and append entry (state changes when entry is applied)
  joint_config_.old_config = cluster_config_;
  joint_config_.new_config = target;

  ConfigChangeCommand cmd{target};
  log_storage_->Append(LogEntry(term(), 0, cmd.Serialize()));

  VLOG(1) << node_id_ << " BeginConfigChange step 1: append joint config, target version="
          << target.version;
  return true;
}

void RaftNode::SetRole(RaftRole new_role) {
  RaftRole old_role = role_;
  role_ = new_role;

  VLOG(1) << "RaftNode " << node_id_ << " term=" << storage_.current_term()
          << ": " << old_role << " -> " << new_role;

  switch (new_role) {
    case RaftRole::Follower:
      leader_term_ = 0;
      storage_.set_voted_for("");
      vote_count_ = 0;
      if (!election_started_) {
        election_started_ = true;
        election_timer_.Start([this] { StartElection(); });
      }
      election_timer_.Reset();
      StopHeartbeat();
      break;
    case RaftRole::Candidate:
      storage_.set_current_term(storage_.current_term() + 1);
      storage_.set_voted_for(node_id_);
      vote_count_ = 1;
      election_timer_.Deactivate();
      StopHeartbeat();
      break;
    case RaftRole::Leader:
      leader_term_ = storage_.current_term();
      election_timer_.Deactivate();
      StopHeartbeat();
      StartHeartbeat(heartbeat_interval_ms_);
      break;
  }
}

void RaftNode::BecomeFollower(Term term) {
  DCHECK_GE(term, storage_.current_term());
  storage_.set_current_term(term);
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
  Term cur_term = storage_.current_term();
  if (request.term < cur_term) {
    VLOG(1) << node_id_ << " rejects VoteRequest from " << request.candidate_id
            << ": stale term " << request.term << " < " << cur_term;
    return {cur_term, false};
  }

  if (request.term > cur_term) {
    BecomeFollower(request.term);
    cur_term = storage_.current_term();
  }

  const NodeId& voted = storage_.voted_for();
  if (!voted.empty() && voted != request.candidate_id) {
    VLOG(1) << node_id_ << " rejects VoteRequest from " << request.candidate_id
            << ": already voted for " << voted;
    return {storage_.current_term(), false};
  }

  Term local_last_term = log_storage_ ? log_storage_->LastTerm() : 0;
  LogIndex local_last_index = log_storage_ ? log_storage_->LastIndex() : 0;

  if (request.last_log_term < local_last_term) {
    VLOG(1) << node_id_ << " rejects VoteRequest from " << request.candidate_id
            << ": log term " << request.last_log_term << " < " << local_last_term;
    return {storage_.current_term(), false};
  }
  if (request.last_log_term == local_last_term && request.last_log_index < local_last_index) {
    VLOG(1) << node_id_ << " rejects VoteRequest from " << request.candidate_id
            << ": log index " << request.last_log_index << " < " << local_last_index;
    return {storage_.current_term(), false};
  }

  storage_.set_voted_for(request.candidate_id);
  VLOG(1) << node_id_ << " grants VoteRequest to " << request.candidate_id
          << " term=" << storage_.current_term();
  return {storage_.current_term(), true};
}

ElectionResult RaftNode::StartElection() {
  BecomeCandidate();

  VoteRequest request;
  request.term = storage_.current_term();
  request.candidate_id = node_id_;
  request.last_log_index = log_storage_ ? log_storage_->LastIndex() : 0;
  request.last_log_term = log_storage_ ? log_storage_->LastTerm() : 0;

  VLOG(1) << node_id_ << " starts election term=" << storage_.current_term()
          << " last_log=" << request.last_log_index << "/" << request.last_log_term;

  ElectionResult result;
  result.votes_received = vote_count_;

  if (config_state_ == ConfigState::kJoint) {
    old_config_votes_ = (joint_config_.old_config.voters.empty() ||
                         joint_config_.old_config.voters.count(node_id_) > 0) ? 1 : 0;
    new_config_votes_ = (joint_config_.new_config.voters.empty() ||
                         joint_config_.new_config.voters.count(node_id_) > 0) ? 1 : 0;
  }

  for (const auto& peer_id : GetPeerIds()) {
    DCHECK(transport_) << "Transport not set for multi-node operation";
    VoteResponse rsp = transport_->SendVoteRequest(peer_id, request);
    if (rsp.vote_granted) {
      result.votes_received++;
      if (config_state_ == ConfigState::kJoint) {
        if (joint_config_.old_config.voters.count(peer_id) > 0)
          old_config_votes_++;
        if (joint_config_.new_config.voters.count(peer_id) > 0)
          new_config_votes_++;
      }
      VLOG(1) << node_id_ << " received VoteGranted from " << peer_id;
    } else {
      result.votes_rejected++;
      VLOG(1) << node_id_ << " received VoteRejected from " << peer_id
              << " (peer term=" << rsp.term << ")";
    }
  }

  vote_count_ = result.votes_received;
  TryBecomeLeader(result);
  return result;
}

bool RaftNode::TryBecomeLeader(const ElectionResult& result) {
  if (config_state_ == ConfigState::kJoint) {
    size_t old_total = joint_config_.old_config.voters.size() + 1;
    size_t new_total = joint_config_.new_config.voters.size() + 1;
    size_t old_majority = old_total / 2 + 1;
    size_t new_majority = new_total / 2 + 1;

    VLOG(1) << node_id_ << " TryBecomeLeader (joint): old_votes=" << old_config_votes_
            << "/" << old_majority << " new_votes=" << new_config_votes_
            << "/" << new_majority;

    if (old_config_votes_ >= old_majority && new_config_votes_ >= new_majority) {
      VLOG(1) << node_id_ << " election won (joint) term=" << storage_.current_term();
      BecomeLeader();
      return true;
    }
    VLOG(1) << node_id_ << " election not won (joint): " << result.votes_received;
    return false;
  }

  size_t total_nodes = cluster_config_.voters.size() + 1;
  size_t majority = total_nodes / 2 + 1;

  VLOG(1) << node_id_ << " TryBecomeLeader: votes=" << result.votes_received
          << " majority=" << majority << " total=" << total_nodes;

  if (result.votes_received >= majority) {
    VLOG(1) << node_id_ << " election won term=" << storage_.current_term();
    BecomeLeader();
    return true;
  }
  VLOG(1) << node_id_ << " election not won: " << result.votes_received << "/" << majority;
  return false;
}

HeartbeatResponse RaftNode::OnHeartbeat(const HeartbeatRequest& request) {
  Term cur_term = storage_.current_term();
  if (request.term < cur_term) {
    VLOG(2) << node_id_ << " rejects Heartbeat from " << request.leader_id
            << ": stale term " << request.term << " < " << cur_term;
    return {cur_term, false};
  }

  if (request.term >= cur_term) {
    VLOG(1) << node_id_ << " accepts Heartbeat from leader " << request.leader_id
            << " term=" << request.term;
    BecomeFollower(request.term);
  }

  election_timer_.Reset();

  return {storage_.current_term(), true};
}

void RaftNode::SendHeartbeatToPeers() {
  HeartbeatRequest req{storage_.current_term(), node_id_};
  for (const auto& peer_id : GetPeerIds()) {
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
  Term cur_term = storage_.current_term();
  if (req.term < cur_term) {
    VLOG(2) << node_id_ << " rejects AppendEntries from " << req.leader_id
            << ": stale term " << req.term << " < " << cur_term;
    LogIndex my_last = log_storage_ ? log_storage_->LastIndex() : 0;
    return {cur_term, false, my_last};
  }

  if (req.term >= cur_term) {
    VLOG(1) << node_id_ << " accepts AppendEntries from leader " << req.leader_id
            << " term=" << req.term << " entries=" << req.entries.size();
    BecomeFollower(req.term);
  }

  if (log_storage_) {
    if (req.prev_log_index > log_storage_->LastIndex()) {
      VLOG(2) << node_id_ << " rejects AppendEntries: gap at prev_log=" << req.prev_log_index;
      return {storage_.current_term(), false, log_storage_->LastIndex()};
    }
    if (req.prev_log_index > 0) {
      const LogEntry* prev = log_storage_->Get(req.prev_log_index);
      if (!prev || prev->term != req.prev_log_term) {
        VLOG(2) << node_id_ << " rejects AppendEntries: conflict at " << req.prev_log_index;
        return {storage_.current_term(), false, req.prev_log_index - 1};
      }
    }

    for (const auto& entry : req.entries) {
      if (entry.index <= log_storage_->LastIndex()) {
        const LogEntry* existing = log_storage_->Get(entry.index);
        if (!existing || existing->term != entry.term) {
          VLOG(1) << node_id_ << " truncate from " << (entry.index - 1);
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
    VLOG(1) << node_id_ << " commit_index " << commit_index_
            << " -> " << std::min(req.leader_commit, my_last) << " (from leader)";
    commit_index_ = std::min(req.leader_commit, my_last);
    ApplyCommittedLogs();
  }

  return {storage_.current_term(), true, my_last};
}

ApplyResult RaftNode::ReplicateLog() {
  if (!log_storage_)
    return {};

  size_t log_size = log_storage_->LogSize();
  auto peer_ids = GetPeerIds();
  VLOG(1) << node_id_ << " ReplicateLog: " << log_size << " entries, "
          << peer_ids.size() << " peers"
          << (IsJointConsensus() ? " (joint)" : "");

  if (peer_ids.empty()) {
    if (commit_index_ < log_storage_->LastIndex()) {
      commit_index_ = log_storage_->LastIndex();
      VLOG(1) << node_id_ << " fast commit: commit_index=" << commit_index_;
    }
    return ApplyCommittedLogs();
  }

  AppendEntriesRequest req;
  req.term = storage_.current_term();
  req.leader_id = node_id_;
  req.leader_commit = commit_index_;

  if (log_size > 0) {
    req.entries = log_storage_->GetRange(1);
  }

  peer_last_log_index_.resize(peer_ids.size());
  for (size_t i = 0; i < peer_ids.size(); i++) {
    DCHECK(transport_) << "Transport not set for multi-node operation";
    AppendEntriesResponse rsp = transport_->SendAppendEntries(peer_ids[i], req);
    peer_last_log_index_[i] = rsp.last_log_index;
  }

  AdvanceCommitIndex();
  return ApplyCommittedLogs();
}

void RaftNode::AdvanceCommitIndex() {
  if (!log_storage_)
    return;

  if (config_state_ == ConfigState::kJoint) {
    AdvanceCommitIndexJoint();
    return;
  }

  std::vector<LogIndex> indexes;
  indexes.push_back(log_storage_->LastIndex());
  for (auto idx : peer_last_log_index_) {
    indexes.push_back(idx);
  }

  if (indexes.empty())
    return;

  std::sort(indexes.rbegin(), indexes.rend());
  size_t total = cluster_config_.voters.size() + 1;
  size_t majority = total / 2 + 1;

  if (majority - 1 >= indexes.size())
    return;

  LogIndex candidate = indexes[majority - 1];

  if (candidate > commit_index_) {
    VLOG(1) << node_id_ << " commit_index " << commit_index_ << " -> " << candidate;
    commit_index_ = candidate;
  }
}

void RaftNode::AdvanceCommitIndexJoint() {
  auto peer_ids = GetPeerIds();

  auto calc_config_commit = [&](const ClusterConfig& config) -> LogIndex {
    std::vector<LogIndex> indexes;
    indexes.push_back(log_storage_->LastIndex());

    for (size_t i = 0; i < peer_ids.size() && i < peer_last_log_index_.size(); i++) {
      if (config.voters.count(peer_ids[i]) > 0) {
        indexes.push_back(peer_last_log_index_[i]);
      }
    }

    if (indexes.empty())
      return 0;

    std::sort(indexes.rbegin(), indexes.rend());
    size_t total = config.voters.size() + 1;
    size_t majority = total / 2 + 1;

    if (majority - 1 >= indexes.size())
      return 0;

    return indexes[majority - 1];
  };

  LogIndex old_commit = calc_config_commit(joint_config_.old_config);
  LogIndex new_commit = calc_config_commit(joint_config_.new_config);
  LogIndex candidate = std::min(old_commit, new_commit);

  if (candidate > commit_index_) {
    VLOG(1) << node_id_ << " commit_index " << commit_index_ << " -> " << candidate
            << " (joint old=" << old_commit << " new=" << new_commit << ")";
    commit_index_ = candidate;
  }
}

void RaftNode::ReplayUnappliedLogs() {
  if (!log_storage_ || !state_machine_)
    return;
  LogIndex last = log_storage_->LastIndex();
  if (last_applied_ >= last) {
    VLOG(1) << node_id_ << " ReplayUnappliedLogs: nothing to replay (last_applied="
            << last_applied_ << " last_index=" << last << ")";
    return;
  }
  VLOG(1) << node_id_ << " ReplayUnappliedLogs: last_applied=" << last_applied_
          << " last_index=" << last;
  commit_index_ = last;
  ApplyCommittedLogs();
}

ApplyResult RaftNode::ApplyCommittedLogs() {
  ApplyResult result;
  if (!state_machine_ || !log_storage_)
    return result;

  constexpr size_t kBatchSize = 128;
  LogIndex prev = last_applied_;

  while (last_applied_ < commit_index_ && last_applied_ < log_storage_->LastIndex()) {
    LogIndex start = last_applied_ + 1;
    size_t limit = std::min<size_t>(kBatchSize, commit_index_ - last_applied_);

    auto entries = log_storage_->GetRange(start, limit);
    if (entries.empty())
      break;

    for (const auto& entry : entries) {
      VLOG(1) << node_id_ << " apply[" << entry.index << "] term=" << entry.term
              << " cmd=" << entry.command;
      if (entry.command.find("CONFIG_CHANGE") == 0) {
        ConfigChangeCommand cmd = ConfigChangeCommand::Deserialize(entry.command);
        if (config_state_ == ConfigState::kJoint) {
          // Step 2: finalize — transition to stable with new config
          cluster_config_ = cmd.target;
          joint_config_ = JointConfig{};
          config_state_ = ConfigState::kStable;
          peer_manager_.SetConfig(&cluster_config_);
          VLOG(1) << node_id_ << " config change step 2: entering Stable, config version="
                  << cluster_config_.version << " voters=" << cluster_config_.voters.size();
        } else {
          // Step 1: enter joint consensus
          joint_config_.old_config = cluster_config_;
          joint_config_.new_config = cmd.target;
          config_state_ = ConfigState::kJoint;
          VLOG(1) << node_id_ << " config change step 1: entering Joint, old voters="
                  << joint_config_.old_config.voters.size()
                  << " new voters=" << joint_config_.new_config.voters.size();
        }
        last_applied_ = entry.index;
        continue;
      }
      result = state_machine_->ApplyLogEntry(entry);
      last_applied_ = entry.index;
    }

    apply_progress_.Update(last_applied_);
  }

  return result;
}

}  // namespace dfly
