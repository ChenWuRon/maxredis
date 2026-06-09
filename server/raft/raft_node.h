// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <vector>

#include "server/raft/append_entries_rpc.h"
#include "server/raft/election_timer.h"
#include "server/raft/heartbeat_rpc.h"
#include "server/raft/log_storage.h"
#include "server/raft/peer_manager.h"
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

  const NodeId& node_id() const {
    return node_id_;
  }

  RaftRole role() const {
    return role_;
  }

  Term term() const {
    return term_;
  }

  const NodeId& voted_for() const {
    return voted_for_;
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

  void AddPeer(const NodeId& id) {
    peer_manager_.AddPeer(id);
  }

  bool RemovePeer(const NodeId& id) {
    return peer_manager_.RemovePeer(id);
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

 private:
  void HeartbeatLoop();

  NodeId node_id_;
  RaftRole role_ = RaftRole::Follower;
  Term term_ = 0;
  Term leader_term_ = 0;
  NodeId voted_for_;
  uint32_t vote_count_ = 0;

  Transport* transport_ = nullptr;
  PeerManager peer_manager_;

  ILogStorage* log_storage_ = nullptr;
  IStateMachine* state_machine_ = nullptr;
  LogIndex commit_index_ = 0;
  LogIndex last_applied_ = 0;
  std::vector<LogIndex> peer_last_log_index_;
  std::atomic<bool> shutdown_{false};
  util::fb2::Fiber heartbeat_fiber_;
  uint32_t heartbeat_interval_ms_ = 50;
  ElectionTimer election_timer_;
  bool election_started_ = false;
};

}  // namespace dfly
