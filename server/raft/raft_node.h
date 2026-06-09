// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <unordered_set>
#include <vector>

#include "server/raft/append_entries_rpc.h"
#include "server/raft/apply_progress.h"
#include "server/raft/election_timer.h"
#include "server/raft/heartbeat_rpc.h"
#include "server/raft/log_storage.h"
#include "server/raft/peer_manager.h"
#include "server/raft/raft_storage.h"
#include "server/raft/raft_types.h"
#include "server/raft/transport.h"
#include "server/raft/vote_rpc.h"
#include "server/state_machine/state_machine.h"
#include "util/fibers/fibers.h"

namespace dfly {

class RaftNode {
 public:
  explicit RaftNode(NodeId node_id = "");
  ~RaftNode();

  // Set the persistence path for Raft metadata.
  // This also loads existing state from disk if available.
  void SetStoragePath(std::string path);

  RaftStorage& storage() {
    return storage_;
  }

  const RaftStorage& storage() const {
    return storage_;
  }

  const NodeId& node_id() const {
    return node_id_;
  }

  RaftRole role() const {
    return role_;
  }

  Term term() const {
    return storage_.current_term();
  }

  const NodeId& voted_for() const {
    return storage_.voted_for();
  }

  uint32_t vote_count() const {
    return vote_count_;
  }

  // Unified role transition — all role changes must go through this.
  // Handles logging, timer management, and heartbeat lifecycle.
  void SetRole(RaftRole new_role);

  void SetTransport(Transport* transport) {
    transport_ = transport;
  }

  const ClusterConfig& cluster_config() const {
    return cluster_config_;
  }

  ConfigState config_state() const {
    return config_state_;
  }

  void SetConfigState(ConfigState state) {
    config_state_ = state;
  }

  void SetClusterConfig(ClusterConfig config) {
    cluster_config_ = std::move(config);
    peer_manager_.SetConfig(&cluster_config_);
  }

  void AddPeer(const NodeId& id) {
    cluster_config_.voters.insert(id);
  }

  bool RemovePeer(const NodeId& id) {
    return cluster_config_.voters.erase(id) > 0;
  }

  const PeerManager& peer_manager() const {
    return peer_manager_;
  }

  void BecomeFollower(Term term);
  void BecomeCandidate();
  void BecomeLeader();

  // Called when the election timer fires.
  // Transitions Follower → Candidate if still in Follower state.
  void OnElectionTimeout();

  // Processes an incoming VoteRequest according to Raft rules.
  VoteResponse OnRequestVote(const VoteRequest& request);

  // Transitions to Candidate, sends VoteRequest to all peers,
  // collects responses, and returns the tally.
  ElectionResult StartElection();

  // Checks if votes_received >= majority (N/2+1).
  // If so, calls BecomeLeader() and returns true.
  bool TryBecomeLeader(const ElectionResult& result);

  Term leader_term() const {
    return leader_term_;
  }

  const ElectionTimer& election_timer() const {
    return election_timer_;
  }

  // Follower-side: processes an incoming Heartbeat from the leader.
  HeartbeatResponse OnHeartbeat(const HeartbeatRequest& request);

  // Leader-side: sends Heartbeat to all peers immediately.
  void SendHeartbeatToPeers();

  // Leader-side: starts a fiber that sends heartbeats periodically.
  void StartHeartbeat(uint32_t interval_ms);

  // Stops the heartbeat fiber.
  void StopHeartbeat();

  // Associates a log storage for log operations.
  void SetLogStorage(ILogStorage* storage) {
    log_storage_ = storage;
  }

  // Associates a state machine for applying committed entries.
  void SetStateMachine(IStateMachine* sm) {
    state_machine_ = sm;
  }

  LogIndex commit_index() const {
    return commit_index_;
  }

  LogIndex last_applied() const {
    return last_applied_;
  }

  LogIndex last_snapshot_index() const {
    return last_snapshot_index_;
  }

  Term last_snapshot_term() const {
    return last_snapshot_term_;
  }

  // Follower-side: processes an incoming AppendEntries request.
  AppendEntriesResponse OnAppendEntries(const AppendEntriesRequest& req);

  // Leader-side: sends all log entries to every peer.
  // Returns the ApplyResult of the last committed entry.
  ApplyResult ReplicateLog();

  // Advances commit_index when a majority of peers have replicated an entry.
  void AdvanceCommitIndex();

  // Applies entries from last_applied+1 up to commit_index.
  // Returns the ApplyResult of the last entry applied.
  ApplyResult ApplyCommittedLogs();

  // Replays unapplied log entries after recovery.
  // Sets commit_index_ = LastIndex() and applies everything after last_applied_.
  // Safe to call even without log_storage_ or state_machine_ (no-op).
  void ReplayUnappliedLogs();

  ApplyProgress& apply_progress() {
    return apply_progress_;
  }

  const ApplyProgress& apply_progress() const {
    return apply_progress_;
  }

 private:
  void HeartbeatLoop();

  ClusterConfig cluster_config_;
  ConfigState config_state_ = ConfigState::kStable;
  RaftStorage storage_;
  ApplyProgress apply_progress_;
  NodeId node_id_;
  RaftRole role_ = RaftRole::Follower;
  Term leader_term_ = 0;
  uint32_t vote_count_ = 0;

  Transport* transport_ = nullptr;
  PeerManager peer_manager_{&cluster_config_};

  ILogStorage* log_storage_ = nullptr;
  IStateMachine* state_machine_ = nullptr;
  LogIndex commit_index_ = 0;
  LogIndex last_applied_ = 0;
  LogIndex last_snapshot_index_ = 0;
  Term last_snapshot_term_ = 0;
  std::vector<LogIndex> peer_last_log_index_;
  std::atomic<bool> shutdown_{false};
  util::fb2::Fiber heartbeat_fiber_;
  uint32_t heartbeat_interval_ms_ = 50;
  ElectionTimer election_timer_;
  bool election_started_ = false;
};

}  // namespace dfly
