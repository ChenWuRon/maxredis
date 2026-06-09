// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
// STRICT REQUIREMENTS
// Implement Raft Joint Consensus exactly as described in
// Raft paper section: "Membership Changes"
//
// DO NOT implement one-step membership change.
// DO NOT directly replace old configuration with new configuration.
//
// The implementation MUST use:
//   Stable → Joint → Stable
// state transitions.
//
// Commit in Joint state MUST require:
//   majority(old_config) AND majority(new_config)
// simultaneously.
//

#pragma once

#include <atomic>
#include <unordered_set>
#include <vector>

#include "server/raft/append_entries_rpc.h"
#include "server/raft/apply_progress.h"
#include "server/raft/election_timer.h"
#include "server/raft/heartbeat_rpc.h"
#include "server/raft/install_snapshot_rpc.h"
#include "server/raft/log_storage.h"
#include "server/raft/peer_manager.h"
#include "server/raft/raft_storage.h"
#include "server/raft/raft_types.h"
#include "server/raft/read_index_rpc.h"
#include "server/raft/snapshot_receiver.h"
#include "server/raft/snapshot_sender.h"
#include "server/raft/timeout_now_rpc.h"
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

  // --- Joint Consensus ---

  // Initiates a membership change via joint consensus.
  // Must be called on the Leader. Appends a CONFIG_CHANGE log entry.
  // When the entry is committed, state transitions occur:
  //   Stable -> Joint (step 1) -> Stable with target (step 2).
  // The second call (with the same target) finalizes the transition.
  bool BeginConfigChange(ClusterConfig target);

  const JointConfig& joint_config() const {
    return joint_config_;
  }

  bool IsJointConsensus() const {
    return config_state_ == ConfigState::kJoint;
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

  // Processes an incoming ReadIndex request from the leader.
  // Returns success=false if the request's term is stale.
  ReadIndexResponse OnReadIndex(const ReadIndexRequest& request);

  // Processes an incoming TimeoutNow request from the current leader.
  // Triggers an immediate election (must be called on follower).
  TimeoutNowResponse OnTimeoutNow(const TimeoutNowRequest& request);

  // Initiates graceful leader transfer to |target|.
  // Returns true if transfer was initiated, false otherwise.
  // Must be called on the leader.
  // |target| must be a current voter (peer) in the cluster.
  bool StartTransfer(const NodeId& target);

  // Returns true if |target| can safely become the next leader
  // (i.e., match_index >= last_index).
  bool IsTransferReady(const NodeId& target) const;

  // Cancels any ongoing transfer. Called automatically on timeout or role change.
  void CancelTransfer();

  const LeaderTransferContext& transfer_context() const {
    return transfer_ctx_;
  }

  // Leader-side: implements the ReadIndex protocol.
  // 1. Records commit_index_ as candidate read index.
  // 2. Sends ReadIndex RPC to all peers.
  // 3. Waits for quorum (majority success responses).
  // 4. Waits for last_applied_ >= read_index.
  // 5. Returns the confirmed read index.
  // Uses leader lease optimization if enabled.
  LogIndex ReadIndex();

  // Blocks until last_applied_ >= target.
  void WaitForApplied(LogIndex target);

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

  void SetSnapshotReceiver(SnapshotReceiver* receiver) {
    snapshot_receiver_ = receiver;
  }

  void SetSnapshotDir(std::string dir) {
    snapshot_dir_ = std::move(dir);
  }

  const std::string& snapshot_dir() const {
    return snapshot_dir_;
  }

  // Returns true if a follower at |next_index| should receive a snapshot
  // instead of AppendEntries.
  static bool ShouldInstallSnapshot(LogIndex next_index, LogIndex snapshot_index) {
    return snapshot_index > 0 && next_index <= snapshot_index;
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

  uint64_t leader_lease_expire() const {
    return leader_lease_expire_;
  }

  // Test helper: forces commit_index to bypass replication.
  void ForceCommitIndex(LogIndex ci) {
    commit_index_ = ci;
  }

  LogIndex last_snapshot_index() const {
    return last_snapshot_index_;
  }

  Term last_snapshot_term() const {
    return last_snapshot_term_;
  }

  // Follower-side: processes an incoming AppendEntries request.
  AppendEntriesResponse OnAppendEntries(const AppendEntriesRequest& req);

  // Follower-side: processes an incoming InstallSnapshot request.
  InstallSnapshotResponse OnInstallSnapshot(const InstallSnapshotRequest& req);

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

  std::vector<NodeId> GetPeerIds() const;
  void AdvanceCommitIndexJoint();
  uint64_t NowMs() const;
  void ExtendLeaderLease();

  // Leader lease: allows skipping ReadIndex quorum within lease period.
  uint64_t leader_lease_expire_ = 0;
  uint64_t lease_ms_ = 100;  // default lease duration
  uint64_t next_read_index_request_id_ = 0;

  // Leader transfer state.
  LeaderTransferContext transfer_ctx_;
  uint64_t transfer_timeout_ms_ = 3000;  // 3 second default
  void CheckTransferTimeout();
  void SendTimeoutNowToTarget();

  JointConfig joint_config_;
  ClusterConfig cluster_config_;
  ConfigState config_state_ = ConfigState::kStable;
  RaftStorage storage_;
  ApplyProgress apply_progress_;
  NodeId node_id_;
  RaftRole role_ = RaftRole::Follower;
  Term leader_term_ = 0;
  uint32_t vote_count_ = 0;
  uint32_t old_config_votes_ = 0;
  uint32_t new_config_votes_ = 0;

  Transport* transport_ = nullptr;
  PeerManager peer_manager_{&cluster_config_};

  ILogStorage* log_storage_ = nullptr;
  IStateMachine* state_machine_ = nullptr;
  SnapshotReceiver* snapshot_receiver_ = nullptr;
  LogIndex commit_index_ = 0;
  LogIndex last_applied_ = 0;
  LogIndex last_snapshot_index_ = 0;
  Term last_snapshot_term_ = 0;
  std::string snapshot_dir_;
  std::vector<NodeId> last_peer_ids_;
  std::vector<LogIndex> peer_last_log_index_;
  std::atomic<bool> shutdown_{false};
  util::fb2::Fiber heartbeat_fiber_;
  uint32_t heartbeat_interval_ms_ = 50;
  ElectionTimer election_timer_;
  bool election_started_ = false;
};

}  // namespace dfly
