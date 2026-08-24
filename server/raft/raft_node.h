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
// CONCURRENCY MODEL
// -----------------
// RaftNode state is shared across proactor threads (connection fibers, the
// heartbeat fiber, the election timer fiber and the snapshot driver). All
// block-able state transitions and log mutations are serialized through
// mutex_ (util::fb2::Mutex — a FIBER-friendly mutex that parks the calling
// fiber instead of the OS thread, so same-thread fibers cannot deadlock the
// proactor).
//
// Two rules prevent deadlocks and data races:
//   1. Every public method that reads or mutates consensus state locks
//      mutex_. Internal helpers use the *Locked() forms; public methods
//      never call one another while holding the lock.
//   2. Transport RPCs are NEVER issued while holding mutex_ — each RPC phase
//      captures the request + peer list under the lock, sends outside the
//      lock, then re-acquires the lock to process responses. Holding the
//      lock across a synchronous in-process RPC would create an AB-BA
//      deadlock between two nodes replicating to each other.
//
// Persistent hard state (term, voted_for, apply progress, WAL) is only ever
// touched under mutex_, which makes every write path crash-safe and
// race-free.

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
#include "util/fibers/synchronization.h"

namespace dfly {

class RaftSnapshotManager;

// Per-call synchronization for the group-commit path. SubmitEntry appends the
// entry under the consensus lock, then either becomes the batch coordinator
// (doing one Persist + one ReplicateLog + one apply sweep for the whole batch)
// or joins the in-flight batch and waits here. The coordinator fulfills `done`.
struct RaftWaiter {
  LogIndex idx = 0;
  util::fb2::Mutex mu;
  util::fb2::CondVar cv;
  ApplyResult res;
  bool done = false;
};

class RaftNode {
 public:
  explicit RaftNode(NodeId node_id = "");
  ~RaftNode();

  GroupId group_id() const {
    return group_id_;
  }

  void set_group_id(GroupId gid) {
    group_id_ = gid;
  }

  void SetNodeId(NodeId id);

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

  RaftRole role() const;
  Term term() const;
  const NodeId voted_for() const;
  uint32_t vote_count() const;

  // Unified role transition — all role changes must go through this.
  // Handles logging, timer management, and heartbeat lifecycle.
  void SetRole(RaftRole new_role);

  void SetTransport(Transport* transport) {
    transport_ = transport;
  }

  ClusterConfig cluster_config() const;
  ConfigState config_state() const;
  void SetConfigState(ConfigState state);
  void SetClusterConfig(ClusterConfig config);
  void AddPeer(const NodeId& id);
  bool RemovePeer(const NodeId& id);

  // --- Joint Consensus ---

  bool BeginConfigChange(ClusterConfig target);

  JointConfig joint_config() const;
  bool IsJointConsensus() const;

  const PeerManager& peer_manager() const {
    return peer_manager_;
  }

  void BecomeFollower(Term term);
  void BecomeCandidate();
  void BecomeLeader();

  // Bootstraps a single-node cluster (no peers) directly into Leader state.
  // Safe only when this node is the sole voter: an election with no peers
  // trivially wins majority (1/1). Used to make writes and linearizable reads
  // functional in single-node deployments without waiting for the election
  // timer. Returns true if the node became (or already was) Leader.
  bool BootstrapSingleNode();

  // Called when the election timer fires.
  void OnElectionTimeout();

  // Processes an incoming VoteRequest according to Raft rules.
  VoteResponse OnRequestVote(const VoteRequest& request);

  // Transitions to Candidate, sends VoteRequest to all peers,
  // collects responses, and returns the tally.
  ElectionResult StartElection();

  // Checks if votes_received >= majority (N/2+1).
  bool TryBecomeLeader(const ElectionResult& result);

  Term leader_term() const;

  const ElectionTimer& election_timer() const {
    return election_timer_;
  }

  HeartbeatResponse OnHeartbeat(const HeartbeatRequest& request);

  ReadIndexResponse OnReadIndex(const ReadIndexRequest& request);

  TimeoutNowResponse OnTimeoutNow(const TimeoutNowRequest& request);

  bool StartTransfer(const NodeId& target);
  bool IsTransferReady(const NodeId& target) const;
  void CancelTransfer();

  const LeaderTransferContext transfer_context() const;

  // Leader-side: implements the ReadIndex protocol with leader-lease fast path.
  // Return value 0 means "leadership could not be confirmed".
  LogIndex ReadIndex();

  // Blocks until last_applied_ >= target. Locks internally; sleeps without
  // the lock so replication/heartbeats can make progress.
  void WaitForApplied(LogIndex target);

  // Leader-side: sends Heartbeat to all peers immediately.
  void SendHeartbeatToPeers();

  // Leader-side: starts a fiber that sends heartbeats periodically.
  void StartHeartbeat(uint32_t interval_ms);

  // Requests the heartbeat fiber to stop. The fiber self-terminates at the
  // next scheduling point; JoinHeartbeat() must be called WITHOUT holding
  // mutex_ (e.g. from ~RaftNode) to reclaim it.
  void StopHeartbeat();

  // Joins the heartbeat fiber if it is still running.
  // Must NOT be called while holding mutex_.
  void JoinHeartbeat();

  // Gracefully stops all background consensus activity (heartbeat fiber and
  // election timer). Idempotent; safe to call before destruction (e.g. when
  // tearing down peers while a transport still holds references to this node).
  void Shutdown();

  // --- RPC lifetime guard (used by transports) ---------------------------
  // Acquires an in-flight RPC reference. Returns false if the node is
  // shutting down — the caller must NOT invoke any handler.
  bool TryAcquireRpcRef() noexcept {
    uint32_t refs = rpc_refs_.load(std::memory_order_acquire);
    while (true) {
      if (!rpc_alive_.load(std::memory_order_acquire) || refs == 0)
        return false;
      if (rpc_refs_.compare_exchange_weak(refs, refs + 1,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire))
        return true;
    }
  }

  void ReleaseRpcRef() noexcept {
    rpc_refs_.fetch_sub(1, std::memory_order_acq_rel);
  }

  void SetLogStorage(ILogStorage* storage) {
    log_storage_ = storage;
  }

  void SetSnapshotReceiver(SnapshotReceiver* receiver) {
    snapshot_receiver_ = receiver;
  }

  // Associates the snapshot driver. The driver invokes
  // CreateSnapshotIfNeeded() on the consensus thread to keep log compactions
  // serialized with appends.
  void SetSnapshotManager(RaftSnapshotManager* mgr) {
    snapshot_manager_ = mgr;
  }

  void SetSnapshotDir(std::string dir);

  const std::string& snapshot_dir() const {
    return snapshot_dir_;
  }

  // Returns true if a follower at |next_index| should receive a snapshot
  // instead of AppendEntries.
  static bool ShouldInstallSnapshot(LogIndex next_index, LogIndex snapshot_index) {
    return snapshot_index > 0 && next_index <= snapshot_index;
  }

  void SetStateMachine(IStateMachine* sm) {
    state_machine_ = sm;
  }

  LogIndex commit_index() const;
  LogIndex last_applied() const;

  uint64_t leader_lease_expire() const;

  // Test helper: forces commit_index to bypass replication.
  void ForceCommitIndex(LogIndex ci);

  LogIndex last_snapshot_index() const;
  Term last_snapshot_term() const;

  AppendEntriesResponse OnAppendEntries(const AppendEntriesRequest& req);

  InstallSnapshotResponse OnInstallSnapshot(const InstallSnapshotRequest& req);

  // Leader-side: replicates the log to all peers. Sends RPCs outside the
  // consensus lock. Returns the ApplyResult of the last committed entry.
  // Replicates the log tail to peers.
  // low_latency=true (client write path): healthy-first ordering + stops as
  //   soon as a quorum has ACKed — a straggler/partitioned peer must not sit
  //   on the write's critical path; it catches up via the heartbeat-triggered
  //   call below.
  // low_latency=false (heartbeat/transfer catch-up path): pushes to EVERY
  //   peer that is behind (next_index <= last_index) and reachable (ACKed the
  //   last heartbeat round). No early break: an early quorum break here is
  //   what left lagging followers permanently stale — the peers that ACK
  //   first are already caught up, so the quorum condition excludes nobody.
  ApplyResult ReplicateLog(bool low_latency = false);

  // Group-commit coordinator: flushes the WAL once (group fsync), drives a
  // single replication round for the whole in-flight batch, applies committed
  // entries, then fulfills every waiter in group_waiters_ (including the
  // caller). Runs WITHOUT holding group_mu_ during the expensive replication so
  // it cannot deadlock with the consensus lock or heartbeat fiber.
  void CoordinatorCommit();

  // Advances commit_index when a majority of peers have replicated an entry.
  // Applies the Figure 8 guard (§5.4.2): only entries from the CURRENT term
  // may advance commit_index directly.
  void AdvanceCommitIndex();

  // Applies entries from last_applied+1 up to commit_index.
  ApplyResult ApplyCommittedLogs();

  // Replays unapplied log entries after recovery.
  // NOTE: does NOT advance commit_index — committing is exclusively the
  // leader's job (via AppendEntries leader_commit). Bumping commit_index to
  // LastIndex() on recovery would apply entries that were never committed,
  // violating State Machine Safety.
  void ReplayUnappliedLogs();

  // Startup hook: declares that the state machine has NO restored state
  // (no snapshot was loaded), so replay must start from the beginning of the
  // WAL. Resets both the in-memory and the on-disk apply progress.
  // Safe: re-applying the local WAL is idempotent for the supported command
  // set, and a follower in a multi-node cluster still never self-commits
  // (ReplayUnappliedLogs refuses without a majority-confirmed commit index).
  void ResetApplyProgress(LogIndex to);

  // Batches apply.meta fsyncs: 0 = flush after every apply batch (default,
  // safe for any WAL policy); >0 = flush at most once per interval, always
  // AFTER flushing the WAL first — apply.meta can never become durable ahead
  // of the log records it references, so recovery can only replay idempotent
  // entries, never skip committed ones.
  void SetApplyMetaFlushInterval(uint32_t interval_ms);

  ApplyProgress& apply_progress() {
    return apply_progress_;
  }

  const ApplyProgress& apply_progress() const {
    return apply_progress_;
  }

  // Client write path: appends |entry| to the log (term is taken from the
  // current leader state) and replicates it. Only valid on the Leader.
  ApplyResult SubmitEntry(LogEntry entry);

  // Runs a snapshot-creation pass under the consensus lock. Called by the
  // snapshot driver fiber; keeps WAL compaction serialized with appends.
  bool CreateSnapshotIfNeeded();

 private:
  // ---- Locked helpers (callers must hold mutex_) ----

  void SetRoleLocked(RaftRole new_role);
  void BecomeFollowerLocked(Term term);
  void BecomeCandidateLocked();
  // Campaign in a fresh term (election-timer retry after a lost round).
  void BecomeCandidateNewTermLocked();
  void TryBecomeLeaderLocked();
  void BecomeLeaderLocked();
  void BecomeLeaderInitPeersLocked();

  // Releases the base RPC ref and blocks (with a bounded wait) until all
  // in-flight RPC handlers have returned. Called from Shutdown() so member
  // destruction never races an active transport dispatch.
  void WaitForRpcDrain();

  std::vector<NodeId> GetPeerIdsLocked() const;
  void AdvanceCommitIndexLocked();
  void AdvanceCommitIndexJointLocked();
  ApplyResult ApplyCommittedLogsLocked();
  ApplyResult ReplicateLogLocked();
  void MaybeAutoFinalizeJointLocked();

  // Renews the leader lease. Must be called under mutex_ and ONLY after a
  // majority of the current config has confirmed the leader (heartbeat ACK
  // or ReadIndex quorum).
  void ExtendLeaderLeaseLocked();

  // Steps down to Follower when the leader lost its quorum (CheckQuorum).
  // Keeps voted_for intact (same term — no vote reset).
  void StepDownLocked();

  void CheckTransferTimeoutLocked();
  void SendTimeoutNowToTarget();
  void CancelTransferLocked();
  bool IsTransferReadyLocked(const NodeId& target) const;

  // One heartbeat round: capture (locked) → send (unlocked) → process
  // (locked). Returns true when the loop must stop (role change / epoch).
  bool HeartbeatTickImpl(bool is_loop, uint64_t epoch);

  uint64_t NowMs() const;

  void HeartbeatLoop();

  // Per-peer replication progress (parallel to last_peer_ids_).
  void ResizePeerArraysLocked(size_t n);

  // ---- members ----

  // Fiber-friendly consensus lock. All consensus state below is guarded by
  // this mutex.
  mutable util::fb2::Mutex mutex_;

  // Leader lease: allows skipping ReadIndex quorum within lease period.
  // Only renewed on majority ACK; monotonic time source.
  uint64_t leader_lease_expire_ = 0;
  uint64_t lease_ms_ = 100;  // default lease duration
  // Last time a majority of the config acknowledged this node as leader.
  uint64_t last_majority_ack_ms_ = 0;
  // Step down if no majority ACK within this window (≈ 2 election timeouts).
  uint64_t check_quorum_ms_ = 600;
  // Batched apply.meta flush (see SetApplyMetaFlushInterval).
  uint32_t apply_meta_flush_interval_ms_ = 0;
  uint64_t last_apply_meta_flush_ms_ = 0;

  uint64_t next_read_index_request_id_ = 0;

  // Leader transfer state.
  LeaderTransferContext transfer_ctx_;
  uint64_t transfer_timeout_ms_ = 3000;  // 3 second default

  GroupId group_id_ = 0;
  JointConfig joint_config_;
  ClusterConfig cluster_config_;
  ConfigState config_state_ = ConfigState::kStable;
  // Index of the joint (step 1) config entry, used to auto-append the
  // step 2 (finalize) entry once it is committed.
  LogIndex joint_entry_index_ = 0;
  bool joint_finalize_appended_ = false;
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

  // --- Group commit (P1-b) ---
  // Separate lock from mutex_ to avoid holding the consensus lock across fsync /
  // replication. Lock order is always: (briefly mutex_) -> group_mu_ -> waiter.mu.
  util::fb2::Mutex group_mu_;
  bool group_active_ = false;
  std::vector<RaftWaiter*> group_waiters_;

  ILogStorage* log_storage_ = nullptr;
  IStateMachine* state_machine_ = nullptr;
  SnapshotReceiver* snapshot_receiver_ = nullptr;
  RaftSnapshotManager* snapshot_manager_ = nullptr;
  LogIndex commit_index_ = 0;
  LogIndex last_applied_ = 0;
  LogIndex last_snapshot_index_ = 0;
  Term last_snapshot_term_ = 0;
  std::string snapshot_dir_;
  std::vector<NodeId> last_peer_ids_;
  std::vector<LogIndex> peer_next_index_;
  std::vector<LogIndex> peer_last_log_index_;
  // Whether each peer ACKed the most recent heartbeat round. ReplicateLog
  // sends to healthy peers first so a partitioned peer never sits on the
  // critical path of a majority write.
  std::vector<bool> peer_hb_ok_;
  // Node-level shutdown flag: set ONLY in Shutdown(). Once set, no background
  // consensus activity (election timer, heartbeat) may (re)start. Distinct
  // from heartbeat_stop_ — StopHeartbeat() is part of every role transition
  // and must not flip the node into "shutdown" mode.
  std::atomic<bool> shutdown_{false};
  // Stops the current heartbeat loop. Set by StopHeartbeat(), cleared by
  // StartHeartbeat(). The loop exits when this is set OR the node shuts down.
  std::atomic<bool> heartbeat_stop_{true};
  // Bumped on every StartHeartbeat so stale heartbeat fibers (from a
  // previous leadership) self-terminate at their next scheduling point.
  std::atomic<uint64_t> heartbeat_epoch_{0};

  // RPC lifetime guard: transport-dispatched RPC handlers may run
  // concurrently with the node's destruction (e.g. another node's heartbeat
  // fiber calling into a node that is being torn down). rpc_refs_ counts
  // in-flight handler invocations; the base ref (1) is released by
  // WaitForRpcDrain() which then waits for the count to drop to zero so
  // member destruction can never race an active handler.
  std::atomic<uint32_t> rpc_refs_{1};
  std::atomic<bool> rpc_alive_{true};
  // Set when WaitForRpcDrain has dropped the base ref — makes the drain
  // (and hence Shutdown) idempotent.
  std::atomic<bool> drain_started_{false};
  util::fb2::Fiber heartbeat_fiber_;
  uint32_t heartbeat_interval_ms_ = 50;
  ElectionTimer election_timer_;
  bool election_started_ = false;
};

}  // namespace dfly