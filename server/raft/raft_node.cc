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
// CONCURRENCY: see raft_node.h — all consensus state is guarded by mutex_
// (a fiber-friendly mutex). Transport RPCs are issued only OUTSIDE the lock.

#include "server/raft/raft_node.h"

#include <algorithm>
#include <chrono>
#include <mutex>

#include "base/logging.h"
#include "server/raft/apply_progress.h"
#include "server/raft/replicated_command.h"
#include "server/raft/snapshot_loader.h"
#include "server/raft/snapshot_manager.h"
#include "server/state_machine/state_machine.h"

namespace dfly {

namespace {

// Max entries carried in a single AppendEntries RPC.
constexpr size_t kMaxAppendBatch = 128;

// Upper bound on how long WaitForApplied may wait before giving up
// (guards against an infinite hang when the node was partitioned away).
constexpr uint64_t kWaitForAppliedTimeoutMs = 5000;

}  // namespace

RaftNode::RaftNode(NodeId node_id) : node_id_(std::move(node_id)) {
}

RaftNode::~RaftNode() {
  Shutdown();
}

void RaftNode::Shutdown() {
  // Definitive: set shutdown_ FIRST (under the lock) so any RPC that arrives
  // afterwards cannot flip the role and restart the election timer / heartbeat
  // (a restarted timer would RPC to peers that may already be destroyed).
  {
    std::lock_guard guard(mutex_);
    shutdown_.store(true, std::memory_order_release);
  }
  // From this point no NEW transport RPC may be dispatched to this node.
  rpc_alive_.store(false, std::memory_order_release);
  if (transport_) {
    transport_->UnregisterNode(group_id_, node_id_);
    // Clear the back-pointer: Shutdown is idempotent and the transport may
    // be destroyed before this node (member destruction order), so a second
    // call must not dereference a stale pointer.
    transport_ = nullptr;
  }
  StopHeartbeat();
  JoinHeartbeat();
  election_timer_.Stop();
  // Wait for RPCs that were already in flight when we flipped rpc_alive_ (a
  // peer's heartbeat/election fiber may be inside one of our handlers). They
  // observe shutdown_ and return promptly; blocking here guarantees the
  // mutex_ and all members below are destroyed without concurrent access.
  WaitForRpcDrain();
}

void RaftNode::WaitForRpcDrain() {
  // Drop the base ref exactly ONCE: Shutdown() is idempotent (called both by
  // ~RaftNode and by Service::Shutdown/RaftEngine teardown) and a second
  // fetch_sub would underflow the counter, making the drain spin on
  // 0xFFFFFFFF until the 5s timeout (and double-wait on every shutdown).
  if (drain_started_.exchange(true, std::memory_order_acq_rel))
    return;
  // In-flight handlers hold the remaining count.
  rpc_refs_.fetch_sub(1, std::memory_order_acq_rel);
  constexpr uint32_t kMaxWaitMs = 5000;
  uint32_t waited = 0;
  while (rpc_refs_.load(std::memory_order_acquire) != 0) {
    if (waited >= kMaxWaitMs) {
      LOG(WARNING) << node_id_ << ": RPC drain timed out after " << waited
                   << "ms with " << rpc_refs_.load(std::memory_order_relaxed)
                   << " in-flight RPC(s)";
      break;
    }
    util::ThisFiber::SleepFor(std::chrono::milliseconds(1));
    ++waited;
  }
}

void RaftNode::SetNodeId(NodeId id) {
  std::lock_guard guard(mutex_);
  node_id_ = std::move(id);
}

void RaftNode::SetStoragePath(std::string path) {
  std::lock_guard guard(mutex_);

  // path is the directory for Raft metadata files (e.g. "data/raft").
  // Ensure trailing slash for consistent path construction.
  if (!path.empty() && path.back() != '/')
    path += '/';

  snapshot_dir_ = path;
  storage_ = RaftStorage(path + "meta.json");
  storage_.Load();

  // Restore a possibly in-flight joint membership change so that step 2 can
  // be resumed (the joint config is hard state).
  config_state_ = storage_.config_state();
  joint_config_ = storage_.joint_config();
  if (config_state_ == ConfigState::kJoint) {
    LOG(INFO) << node_id_ << " recovered in-flight joint consensus (old voters="
              << joint_config_.old_config.voters.size()
              << " new voters=" << joint_config_.new_config.voters.size() << ")";
  }

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

        // Restore the snapshot anchor so that GetTerm(last_snapshot_index_)
        // returns the correct term for AppendEntries consistency checks.
        if (log_storage_) {
          log_storage_->SetSnapshotAnchor(last_snapshot_index_, last_snapshot_term_);
          // Drop any WAL entries covered by the snapshot from memory and
          // from the segment index (their disk records are still valid but
          // redundant; ScanSegment prunes them on next Open via the anchor).
          log_storage_->PruneCompacted();
        }
      }
    }
  }
  // Note: apply.meta (and the KV layer's own snapshot/AOF restore, wired in
  // main_service) is authoritative for last_applied_. ReplayUnappliedLogs()
  // only replays entries strictly after last_applied_, so a node that
  // restored its state machine from the KV persistence layer never re-applies
  // entries that were already applied before the restart.

  // commit_index_ is a volatile field in Raft: it is re-established by the
  // elected leader via AppendEntries. Initialize it to last_applied_ so we
  // never re-apply entries below it, and never self-commit beyond it.
  commit_index_ = last_applied_;

  VLOG(1) << node_id_ << " SetStoragePath: last_applied=" << last_applied_;
}

std::vector<NodeId> RaftNode::GetPeerIdsLocked() const {
  if (config_state_ == ConfigState::kJoint) {
    std::unordered_set<NodeId> all;
    all.insert(joint_config_.old_config.voters.begin(),
               joint_config_.old_config.voters.end());
    all.insert(joint_config_.new_config.voters.begin(),
               joint_config_.new_config.voters.end());
    all.erase(node_id_);
    return {all.begin(), all.end()};
  }
  auto& voters = cluster_config_.voters;
  std::vector<NodeId> result;
  result.reserve(voters.size());
  for (const auto& v : voters) {
    if (v != node_id_)
      result.push_back(v);
  }
  return result;
}

void RaftNode::ResizePeerArraysLocked(size_t n) {
  peer_next_index_.resize(n);
  peer_last_log_index_.resize(n);
  peer_hb_ok_.resize(n, /*healthy=*/true);
  for (size_t i = 0; i < n; i++) {
    if (peer_next_index_[i] == 0) {
      // Standard Raft: a freshly known peer starts at LastIndex+1.
      peer_next_index_[i] = log_storage_ ? log_storage_->LastIndex() + 1 : 1;
    }
  }
}

bool RaftNode::BeginConfigChange(ClusterConfig target) {
  std::lock_guard guard(mutex_);
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
    log_storage_->Append(LogEntry(storage_.current_term(), 0, cmd.Serialize()));
    joint_finalize_appended_ = true;
    VLOG(1) << node_id_ << " BeginConfigChange step 2: append final config version="
            << target.version;
    return true;
  }

  // Step 1: store joint config and append entry (state changes when entry is applied)
  joint_config_.old_config = cluster_config_;
  joint_config_.new_config = target;

  ConfigChangeCommand cmd{target};
  log_storage_->Append(LogEntry(storage_.current_term(), 0, cmd.Serialize()));

  VLOG(1) << node_id_ << " BeginConfigChange step 1: append joint config, target version="
          << target.version;
  return true;
}

void RaftNode::SetRole(RaftRole new_role) {
  std::lock_guard guard(mutex_);
  if (new_role == role_)
    return;  // idempotent
  switch (new_role) {
    case RaftRole::Follower:
      // Public API (test-facing). Unlike BecomeFollowerLocked, this clears
      // the current-term vote to match legacy SetRole semantics.
      SetRoleLocked(RaftRole::Follower);
      storage_.SetState(storage_.current_term(), "");
      break;
    case RaftRole::Candidate:
      // Atomic (term+1, self-vote) persistence.
      if (role_ != RaftRole::Candidate)
        BecomeCandidateLocked();
      break;
    case RaftRole::Leader:
      // Raw API: promote through the candidate path so the term and the
      // self-vote are consistent (the DCHECK requires a Candidate).
      if (role_ != RaftRole::Candidate)
        BecomeCandidateLocked();
      BecomeLeaderLocked();
      break;
  }
}

void RaftNode::SetRoleLocked(RaftRole new_role) {
  RaftRole old_role = role_;
  role_ = new_role;

  VLOG(1) << "RaftNode " << node_id_ << " term=" << storage_.current_term()
          << ": " << old_role << " -> " << new_role;

  // After Shutdown() no background consensus activity may (re)start: a role
  // change triggered by a late RPC must not resurrect the election timer or
  // the heartbeat fiber.
  const bool shutdown = shutdown_.load(std::memory_order_acquire);

  switch (new_role) {
    case RaftRole::Follower:
      leader_term_ = 0;
      vote_count_ = 0;
      CancelTransferLocked();  // cancel any ongoing transfer on step-down
      if (!shutdown) {
        if (!election_started_) {
          election_started_ = true;
          election_timer_.Start([this] { StartElection(); });
        }
        election_timer_.Reset();
      } else {
        election_timer_.Stop();
      }
      StopHeartbeat();
      break;
    case RaftRole::Candidate:
      // NOTE: term increment + self-vote are persisted as ONE atomic write by
      // BecomeCandidateLocked, not here.
      vote_count_ = 1;
      CancelTransferLocked();
      election_timer_.Deactivate();
      StopHeartbeat();
      break;
    case RaftRole::Leader:
      leader_term_ = storage_.current_term();
      election_timer_.Deactivate();
      CancelTransferLocked();  // clear any stale transfer context
      StopHeartbeat();
      if (!shutdown)
        StartHeartbeat(heartbeat_interval_ms_);
      break;
  }
}

void RaftNode::BecomeFollower(Term term) {
  std::lock_guard guard(mutex_);
  BecomeFollowerLocked(term);
}

void RaftNode::BecomeFollowerLocked(Term term) {
  DCHECK_GE(term, storage_.current_term());
  if (term > storage_.current_term()) {
    // New term: persist (term, voted_for="") in a SINGLE atomic fsync so a
    // crash can never observe a new term with a stale vote (Election Safety).
    storage_.SetState(term, "");
  }
  // Same-term step-down keeps voted_for intact: clearing it could let this
  // node grant a second vote within the same term.
  SetRoleLocked(RaftRole::Follower);
}

void RaftNode::BecomeCandidate() {
  std::lock_guard guard(mutex_);
  BecomeCandidateLocked();
}

void RaftNode::BecomeCandidateLocked() {
  if (role_ == RaftRole::Candidate)
    return;  // already campaigning in the current term
  // Atomic (term+1, vote=self) persistence — one fsync, no crash window
  // between "term durable" and "vote durable".
  storage_.SetState(storage_.current_term() + 1, node_id_);
  SetRoleLocked(RaftRole::Candidate);
}

// Re-enters campaigning in a FRESH term. Used when the election timer fires
// for a candidate that lost the previous round (Raft §5.2: a candidate that
// times out starts a NEW election with an incremented term). Re-running in
// the same term could never win — the peers already rejected us for it.
void RaftNode::BecomeCandidateNewTermLocked() {
  storage_.SetState(storage_.current_term() + 1, node_id_);
  SetRoleLocked(RaftRole::Candidate);
}

void RaftNode::BecomeLeader() {
  std::lock_guard guard(mutex_);
  BecomeLeaderLocked();
}

void RaftNode::BecomeLeaderInitPeersLocked() {
  auto peers = GetPeerIdsLocked();
  last_peer_ids_ = peers;
  peer_next_index_.assign(peers.size(), 0);
  peer_last_log_index_.assign(peers.size(), 0);
  if (log_storage_) {
    LogIndex init_next = log_storage_->LastIndex() + 1;
    std::fill(peer_next_index_.begin(), peer_next_index_.end(), init_next);
  }
}

void RaftNode::BecomeLeaderLocked() {
  DCHECK_EQ(role_, RaftRole::Candidate);
  SetRoleLocked(RaftRole::Leader);
  BecomeLeaderInitPeersLocked();
  // A fresh leader has no confirmed majority contact yet.
  last_majority_ack_ms_ = NowMs();
  // Replicate immediately so the leader's entries reach followers fast.
}

bool RaftNode::BootstrapSingleNode() {
  std::lock_guard guard(mutex_);
  if (role_ == RaftRole::Leader)
    return true;

  // Only valid when there are no other voters — otherwise this would bypass
  // the normal election and could create a split brain.
  if (!GetPeerIdsLocked().empty()) {
    LOG(WARNING) << node_id_ << " BootstrapSingleNode refused: cluster has peers";
    return false;
  }

  BecomeCandidateLocked();  // term += 1, vote for self
  BecomeLeaderLocked();     // majority(1/1) is trivially satisfied
  VLOG(1) << node_id_ << " bootstrapped as single-node Leader, term="
          << storage_.current_term();
  return true;
}

void RaftNode::OnElectionTimeout() {
  std::lock_guard guard(mutex_);
  if (shutdown_.load(std::memory_order_acquire))
    return;
  if (role_ != RaftRole::Follower)
    return;
  BecomeCandidateLocked();
}

VoteResponse RaftNode::OnRequestVote(const VoteRequest& request) {
  std::lock_guard guard(mutex_);
  Term cur_term = storage_.current_term();
  if (request.term < cur_term) {
    VLOG(1) << node_id_ << " rejects VoteRequest from " << request.candidate_id
            << ": stale term " << request.term << " < " << cur_term;
    return {group_id_, cur_term, false};
  }

  if (request.term > cur_term) {
    BecomeFollowerLocked(request.term);
    cur_term = storage_.current_term();
  }

  const NodeId& voted = storage_.voted_for();
  if (!voted.empty() && voted != request.candidate_id) {
    VLOG(1) << node_id_ << " rejects VoteRequest from " << request.candidate_id
            << ": already voted for " << voted;
    return {group_id_, storage_.current_term(), false};
  }

  Term local_last_term = log_storage_ ? log_storage_->LastTerm() : 0;
  LogIndex local_last_index = log_storage_ ? log_storage_->LastIndex() : 0;

  if (request.last_log_term < local_last_term) {
    VLOG(1) << node_id_ << " rejects VoteRequest from " << request.candidate_id
            << ": log term " << request.last_log_term << " < " << local_last_term;
    return {group_id_, storage_.current_term(), false};
  }
  if (request.last_log_term == local_last_term && request.last_log_index < local_last_index) {
    VLOG(1) << node_id_ << " rejects VoteRequest from " << request.candidate_id
            << ": log index " << request.last_log_index << " < " << local_last_index;
    return {group_id_, storage_.current_term(), false};
  }

  // The vote decision is fsynced (single field; the term is already durable)
  // BEFORE the caller sees our grant.
  storage_.set_voted_for(request.candidate_id);
  VLOG(1) << node_id_ << " grants VoteRequest to " << request.candidate_id
          << " term=" << storage_.current_term();
  return {group_id_, storage_.current_term(), true};
}

ElectionResult RaftNode::StartElection() {
  // Phase 1 (locked): transition to candidate and capture the request.
  std::vector<NodeId> peers;
  VoteRequest req;
  {
    std::lock_guard guard(mutex_);
    if (shutdown_.load(std::memory_order_acquire))
      return {};  // node is being torn down — never start an election
    if (role_ == RaftRole::Follower) {
      BecomeCandidateLocked();
    } else if (role_ == RaftRole::Candidate) {
      // Election-timer retry (Raft §5.2): campaign in a FRESH term. A
      // re-run in the current term could never win — the peers rejected
      // it already.
      BecomeCandidateNewTermLocked();
    }
    if (role_ != RaftRole::Candidate) {
      return {};  // we are a leader already or have been deposed
    }

    req.group_id = group_id_;
    req.term = storage_.current_term();
    req.candidate_id = node_id_;
    req.last_log_index = log_storage_ ? log_storage_->LastIndex() : 0;
    req.last_log_term = log_storage_ ? log_storage_->LastTerm() : 0;
    peers = GetPeerIdsLocked();

    VLOG(1) << node_id_ << " starts election term=" << storage_.current_term()
            << " last_log=" << req.last_log_index << "/" << req.last_log_term;
  }

  // Phase 2: vote RPCs WITHOUT holding the consensus lock. Sequential sends
  // with a short vote RPC timeout (500ms, set in the transport) keep each
  // election round bounded even when a peer is partitioned.
  ElectionResult result;
  result.votes_received = 1;
  std::vector<NodeId> granters;
  Term max_peer_term = req.term;
  for (const auto& peer_id : peers) {
    if (!transport_) {
      LOG(DFATAL) << "Transport not set for multi-node operation";
      break;
    }
    VoteResponse rsp = transport_->SendVoteRequest(peer_id, req);
    if (rsp.vote_granted) {
      result.votes_received++;
      granters.push_back(peer_id);
      VLOG(1) << node_id_ << " received VoteGranted from " << peer_id;
    } else {
      result.votes_rejected++;
      VLOG(1) << node_id_ << " received VoteRejected from " << peer_id
              << " (peer term=" << rsp.term << ")";
    }
    if (rsp.term > max_peer_term)
      max_peer_term = rsp.term;
  }

  // Phase 3 (locked): process the tally.
  {
    std::lock_guard guard(mutex_);
    if (max_peer_term > storage_.current_term()) {
      BecomeFollowerLocked(max_peer_term);
      return result;
    }
    // Only count the votes if we are still campaigning in the same term.
    if (role_ != RaftRole::Candidate || storage_.current_term() != req.term)
      return result;

    vote_count_ = result.votes_received;

    if (config_state_ == ConfigState::kJoint) {
      old_config_votes_ = (joint_config_.old_config.voters.empty() ||
                           joint_config_.old_config.voters.count(node_id_) > 0) ? 1 : 0;
      new_config_votes_ = (joint_config_.new_config.voters.empty() ||
                           joint_config_.new_config.voters.count(node_id_) > 0) ? 1 : 0;
      for (const auto& g : granters) {
        if (joint_config_.old_config.voters.count(g) > 0)
          old_config_votes_++;
        if (joint_config_.new_config.voters.count(g) > 0)
          new_config_votes_++;
      }
    }
    TryBecomeLeaderLocked();

    // Raft §5.2: a candidate that loses the election steps back and waits
    // for a fresh randomized timeout before campaigning in a NEW term.
    // Stepping to Follower (rather than staying Candidate) is what breaks
    // split-vote lockstep: a Follower GRANTS a higher-term vote request,
    // while a Candidate rejects it — two candidates in sync would reject
    // each other forever. voted_for is kept (same-term step-down), which
    // also preserves Election Safety against same-term re-requests.
    if (role_ == RaftRole::Candidate &&
        !shutdown_.load(std::memory_order_acquire)) {
      VLOG(1) << node_id_ << " election round lost (votes=" << result.votes_received
              << "), stepping back to Follower for a fresh timer";
      SetRoleLocked(RaftRole::Follower);  // Reset()s the election timer
    }
  }
  return result;
}

void RaftNode::TryBecomeLeaderLocked() {
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
      BecomeLeaderLocked();
      return;
    }
    VLOG(1) << node_id_ << " election not won (joint): " << vote_count_;
    return;
  }

  size_t total_nodes = cluster_config_.voters.size() + 1;
  size_t majority = total_nodes / 2 + 1;

  VLOG(1) << node_id_ << " TryBecomeLeader: votes=" << vote_count_
          << " majority=" << majority << " total=" << total_nodes;

  if (vote_count_ >= majority) {
    VLOG(1) << node_id_ << " election won term=" << storage_.current_term();
    BecomeLeaderLocked();
    return;
  }
  VLOG(1) << node_id_ << " election not won: " << vote_count_ << "/" << majority;
}

bool RaftNode::TryBecomeLeader(const ElectionResult& result) {
  std::lock_guard guard(mutex_);
  vote_count_ = result.votes_received;
  TryBecomeLeaderLocked();
  return role_ == RaftRole::Leader;
}

HeartbeatResponse RaftNode::OnHeartbeat(const HeartbeatRequest& request) {
  std::lock_guard guard(mutex_);
  Term cur_term = storage_.current_term();
  if (request.term < cur_term) {
    VLOG(2) << node_id_ << " rejects Heartbeat from " << request.leader_id
            << ": stale term " << request.term << " < " << cur_term;
    return {group_id_, cur_term, false,
            log_storage_ ? log_storage_->LastIndex() : 0};
  }

  if (request.term > cur_term || role_ != RaftRole::Leader) {
    VLOG(1) << node_id_ << " accepts Heartbeat from leader " << request.leader_id
            << " term=" << request.term;
    BecomeFollowerLocked(request.term);
  } else {
    // Same term and we are the leader (split-brain recovery): stay.
    election_timer_.Reset();
    return {group_id_, storage_.current_term(), true,
            log_storage_ ? log_storage_->LastIndex() : 0};
  }

  election_timer_.Reset();

  // A heartbeat is an empty AppendEntries: advance commit_index from the
  // leader's and apply. Without this, entries replicated by AppendEntries
  // stay unapplied on the follower until the next write. State Machine
  // Safety holds: leader_commit only ever references entries the leader has
  // committed, and the min() with our own last index prevents applying
  // entries we never stored.
  if (request.leader_commit > commit_index_ && log_storage_) {
    LogIndex my_last = log_storage_->LastIndex();
    if (request.leader_commit > my_last) {
      VLOG(1) << node_id_ << " heartbeat: leader_commit " << request.leader_commit
              << " beyond our log (" << my_last << ") — ignoring";
    } else {
      commit_index_ = request.leader_commit;
      ApplyCommittedLogsLocked();
    }
  }

  // Report our log length so the leader can detect we missed entries while
  // partitioned (it then pushes them via AppendEntries) and can advance its
  // commit index from the ACK.
  return {group_id_, storage_.current_term(), true,
          log_storage_ ? log_storage_->LastIndex() : 0};
}

ReadIndexResponse RaftNode::OnReadIndex(const ReadIndexRequest& request) {
  std::lock_guard guard(mutex_);
  ReadIndexResponse resp;
  resp.term = storage_.current_term();
  resp.success = (request.term >= storage_.current_term() &&
                  role_ != RaftRole::Candidate);
  resp.commit_index = commit_index_;
  VLOG(2) << node_id_ << " OnReadIndex from " << request.leader_id
          << ": success=" << resp.success << " commit_index=" << resp.commit_index;
  return resp;
}

TimeoutNowResponse RaftNode::OnTimeoutNow(const TimeoutNowRequest& request) {
  bool start_election = false;
  {
    std::lock_guard guard(mutex_);
    if (request.term < storage_.current_term()) {
      VLOG(1) << node_id_ << " rejects TimeoutNow from " << request.leader_id
              << ": stale term " << request.term;
      return {group_id_, storage_.current_term(), false};
    }
    if (request.term > storage_.current_term())
      BecomeFollowerLocked(request.term);

    VLOG(1) << node_id_ << " TimeoutNow from leader " << request.leader_id
            << " term=" << request.term << " — immediate election";
    if (role_ != RaftRole::Leader)
      start_election = true;
  }

  // Kick off an immediate election OUTSIDE the lock.
  if (start_election)
    StartElection();
  return {group_id_, request.term, true};
}

// Returns true if the tick must stop the heartbeat loop (role change or
// epoch change).
bool RaftNode::HeartbeatTickImpl(bool is_loop, uint64_t epoch) {
  // --- Phase 1 (locked): capture peers and current term. ---
  std::vector<NodeId> peers;
  HeartbeatRequest req;
  {
    std::lock_guard guard(mutex_);
    if (is_loop && heartbeat_epoch_.load(std::memory_order_acquire) != epoch)
      return true;
    if (role_ != RaftRole::Leader)
      return true;
    req.group_id = group_id_;
    req.term = storage_.current_term();
    req.leader_id = node_id_;
    req.leader_commit = commit_index_;
    peers = GetPeerIdsLocked();
  }

  // --- Phase 2: RPCs WITHOUT the lock. ---
  // Sequential sends are safe BECAUSE the heartbeat RPC timeout (150ms) is
  // far below the follower election timeout (300-600ms): a partitioned peer
  // delays the round by at most ~150ms, so healthy followers still receive
  // a heartbeat every ~200ms and never start spurious elections (the old
  // 1s RPC timeout made a single dead peer starve everyone into election
  // livelock).
  size_t ack_count = 1;  // self
  Term max_peer_term = req.term;
  // (peer_id -> follower last_log_index) for successful same-term ACKs.
  std::vector<std::pair<NodeId, LogIndex>> rsp_last_log;
  for (const auto& peer_id : peers) {
    if (!transport_)
      break;
    HeartbeatResponse rsp = transport_->SendHeartbeat(peer_id, req);
    if (rsp.success && rsp.term == req.term) {
      ack_count++;
      rsp_last_log.emplace_back(peer_id, rsp.last_log_index);
    }
    if (rsp.term > max_peer_term)
      max_peer_term = rsp.term;
  }

  // --- Phase 3 (locked): process ACKs, lease, quorum and transfer. ---
  bool send_timeout_now = false;
  bool need_replicate = false;
  {
    std::lock_guard guard(mutex_);
    if (is_loop && heartbeat_epoch_.load(std::memory_order_acquire) != epoch)
      return true;
    if (role_ != RaftRole::Leader)
      return true;

    if (max_peer_term > storage_.current_term()) {
      VLOG(1) << node_id_ << " heartbeat: peer has higher term " << max_peer_term
              << ", stepping down";
      BecomeFollowerLocked(max_peer_term);
      return true;
    }

    // Update match progress from heartbeat ACKs (etcd MsgHeartbeatResp):
    // lets commit advance between writes and detects followers that are
    // behind (e.g. a restarted node that missed entries) so we can push
    // them via AppendEntries. The arrays are refreshed from the current
    // peer set first; ids are matched by value so ordering drift is safe.
    {
      auto peer_ids = GetPeerIdsLocked();
      ResizePeerArraysLocked(peer_ids.size());
      last_peer_ids_ = peer_ids;
      // Fresh round: everyone is presumed unreachable until proven otherwise;
      // ACKed peers are marked healthy below.
      std::fill(peer_hb_ok_.begin(), peer_hb_ok_.end(), false);
      LogIndex my_last = log_storage_ ? log_storage_->LastIndex() : 0;
      for (const auto& [peer_id, last_log] : rsp_last_log) {
        auto it = std::find(last_peer_ids_.begin(), last_peer_ids_.end(), peer_id);
        if (it == last_peer_ids_.end())
          continue;
        size_t i = it - last_peer_ids_.begin();
        peer_hb_ok_[i] = true;
        peer_last_log_index_[i] = last_log;
        if (last_log < my_last)
          need_replicate = true;  // follower behind: push missing entries
      }
      AdvanceCommitIndexLocked();
      // NOTE: no state-machine apply here. The leader applies on the write
      // path (ReplicateLog) and via WaitForApplied for linearizable reads;
      // applying inside the heartbeat fiber can block it on the shard
      // threads (RunBriefInParallel waits across proactors), freezing
      // heartbeats for the whole cluster and triggering election churn.
    }

    size_t majority;
    if (config_state_ == ConfigState::kJoint) {
      size_t old_total = joint_config_.old_config.voters.size() + 1;
      size_t new_total = joint_config_.new_config.voters.size() + 1;
      majority = std::max(old_total / 2 + 1, new_total / 2 + 1);
    } else {
      majority = cluster_config_.voters.size() / 2 + 1;
    }

    uint64_t now = NowMs();
    if (ack_count >= majority) {
      last_majority_ack_ms_ = now;
      // C2 fix: the lease is renewed ONLY on a real majority ACK, so a
      // partitioned leader can never serve stale linearizable reads.
      ExtendLeaderLeaseLocked();
    } else if (now - last_majority_ack_ms_ > check_quorum_ms_) {
      LOG(WARNING) << node_id_ << " CheckQuorum: lost majority for "
                   << (now - last_majority_ack_ms_) << "ms, stepping down";
      StepDownLocked();
      return true;
    }

    // Leader transfer lifecycle.
    if (transfer_ctx_.IsActive()) {
      CheckTransferTimeoutLocked();
      if (transfer_ctx_.state == TransferState::kWaitingCatchUp &&
          role_ == RaftRole::Leader) {
        if (IsTransferReadyLocked(transfer_ctx_.target)) {
          VLOG(1) << node_id_ << " " << transfer_ctx_.target
                  << " is now caught up, sending TimeoutNow";
          transfer_ctx_.state = TransferState::kWaitingElection;
          send_timeout_now = true;
        } else {
          need_replicate = true;  // push entries to the target
        }
      }
    }
  }

  if (send_timeout_now)
    SendTimeoutNowToTarget();
  if (need_replicate)
    ReplicateLog(/*low_latency=*/false);  // heartbeat catch-up: push to ALL behind peers
  return false;
}

void RaftNode::HeartbeatLoop() {
  uint64_t epoch = heartbeat_epoch_.load(std::memory_order_acquire);
  while (!heartbeat_stop_.load(std::memory_order_acquire) &&
         !shutdown_.load(std::memory_order_acquire)) {
    bool stop = HeartbeatTickImpl(true, epoch);
    if (stop)
      break;
    util::ThisFiber::SleepFor(std::chrono::milliseconds(heartbeat_interval_ms_));
  }
  VLOG(2) << node_id_ << " heartbeat fiber exiting";
}

void RaftNode::SendHeartbeatToPeers() {
  HeartbeatTickImpl(false, 0);
}

void RaftNode::StartHeartbeat(uint32_t interval_ms) {
  heartbeat_interval_ms_ = interval_ms;
  heartbeat_stop_.store(false, std::memory_order_release);
  heartbeat_epoch_.fetch_add(1, std::memory_order_acq_rel);
  if (heartbeat_fiber_.IsJoinable()) {
    // A previous leader's fiber may still be winding down. Detach it — it
    // self-terminates on the next epoch/shutdown check. (Join would deadlock
    // here: this is called with mutex_ held.)
    heartbeat_fiber_.Detach();
  }
  heartbeat_fiber_ = util::fb2::Fiber("heartbeat", [this] { HeartbeatLoop(); });
}

void RaftNode::StopHeartbeat() {
  heartbeat_stop_.store(true, std::memory_order_release);
}

void RaftNode::JoinHeartbeat() {
  if (heartbeat_fiber_.IsJoinable())
    heartbeat_fiber_.Join();
}

LogIndex RaftNode::ReadIndex() {
  LogIndex read_index = 0;
  uint64_t current_term = 0;
  uint64_t request_id = 0;
  bool slow_path = false;
  std::vector<NodeId> peers;

  // --- Phase 1 (locked): fast path via the leader lease. ---
  {
    std::lock_guard guard(mutex_);
    if (role_ != RaftRole::Leader) {
      LOG(WARNING) << node_id_ << " ReadIndex: not leader, role=" << role_;
      return 0;
    }
    if (NowMs() < leader_lease_expire_) {
      // Fast path: lease valid → no quorum RPCs, zero RTT overhead.
      read_index = commit_index_;
      VLOG(2) << node_id_ << " ReadIndex fast path: lease valid, read_index=" << read_index;
    } else {
      slow_path = true;
      current_term = storage_.current_term();
      request_id = ++next_read_index_request_id_;
      peers = GetPeerIdsLocked();
    }
  }

  if (slow_path) {
    // --- Phase 2: ReadIndex RPCs WITHOUT the lock. ---
    size_t success_count = 1;  // Self counts as success
    Term max_peer_term = current_term;
    ReadIndexRequest req;
    req.group_id = group_id_;
    req.term = current_term;
    req.leader_id = node_id_;
    req.request_id = request_id;
    for (const auto& peer_id : peers) {
      if (!transport_)
        break;
      ReadIndexResponse resp = transport_->SendReadIndex(peer_id, req);
      if (resp.success && resp.term == current_term) {
        success_count++;
      }
      if (resp.term > max_peer_term)
        max_peer_term = resp.term;
    }

    // --- Phase 3 (locked): confirm quorum. ---
    std::lock_guard guard(mutex_);
    if (max_peer_term > storage_.current_term()) {
      VLOG(1) << node_id_ << " ReadIndex: peer has higher term " << max_peer_term;
      BecomeFollowerLocked(max_peer_term);
      return 0;
    }
    if (role_ != RaftRole::Leader || storage_.current_term() != current_term)
      return 0;

    size_t total = cluster_config_.voters.size() + 1;
    size_t majority = total / 2 + 1;
    if (success_count < majority) {
      VLOG(1) << node_id_ << " ReadIndex: only " << success_count
              << "/" << majority << " acks, cannot confirm leadership";
      return 0;
    }
    read_index = commit_index_;
    VLOG(2) << node_id_ << " ReadIndex: quorum confirmed (" << success_count
            << "/" << majority << "), read_index=" << read_index;

    // Lease is renewed ONLY on a confirmed quorum (fast-path lease is
    // therefore always backed by a real majority contact).
    last_majority_ack_ms_ = NowMs();
    ExtendLeaderLeaseLocked();
  }

  if (read_index > 0)
    WaitForApplied(read_index);
  return read_index;
}

void RaftNode::WaitForApplied(LogIndex target) {
  uint64_t deadline_ms = NowMs() + kWaitForAppliedTimeoutMs;
  while (true) {
    {
      std::lock_guard guard(mutex_);
      ApplyCommittedLogsLocked();
      if (last_applied_ >= target)
        return;
    }
    // Yield WITHOUT the lock so the heartbeat fiber can replicate and advance
    // commit_index_ (or a new leader can push us entries via AppendEntries).
    if (NowMs() > deadline_ms) {
      LOG(WARNING) << node_id_ << " WaitForApplied: timed out waiting for index "
                   << target << " (last_applied=" << last_applied_ << ")";
      return;
    }
    util::ThisFiber::SleepFor(std::chrono::milliseconds(1));
  }
}

void RaftNode::ExtendLeaderLeaseLocked() {
  leader_lease_expire_ = NowMs() + lease_ms_;
}

void RaftNode::StepDownLocked() {
  if (role_ != RaftRole::Leader)
    return;
  LOG(WARNING) << node_id_ << " stepping down: leader lease/quorum lost";
  BecomeFollowerLocked(storage_.current_term());  // same term → vote preserved
}

uint64_t RaftNode::NowMs() const {
  // Monotonic clock: immune to wall-clock jumps (NTP) which could otherwise
  // extend or shorten the leader lease arbitrarily.
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

bool RaftNode::StartTransfer(const NodeId& target) {
  bool initiated = false;
  {
    std::lock_guard guard(mutex_);
    if (role_ != RaftRole::Leader) {
      LOG(WARNING) << node_id_ << " StartTransfer: not leader";
      return false;
    }
    if (transfer_ctx_.IsActive()) {
      LOG(WARNING) << node_id_ << " StartTransfer: transfer already in progress to "
                   << transfer_ctx_.target;
      return false;
    }

    auto peers = GetPeerIdsLocked();
    bool found = false;
    for (const auto& p : peers) {
      if (p == target) {
        found = true;
        break;
      }
    }
    if (!found) {
      LOG(WARNING) << node_id_ << " StartTransfer: " << target << " is not a peer";
      return false;
    }

    VLOG(1) << node_id_ << " StartTransfer: initiating transfer to " << target;
    transfer_ctx_.target = target;
    transfer_ctx_.state = TransferState::kRequested;
    transfer_ctx_.start_ms = NowMs();
    initiated = true;
  }

  // Push entries to the target (RPCs happen outside the lock).
  ReplicateLog(/*low_latency=*/false);  // transfer catch-up: target must be fully caught up

  bool send_now = false;
  {
    std::lock_guard guard(mutex_);
    if (role_ != RaftRole::Leader || !transfer_ctx_.IsActive())
      return false;
    if (IsTransferReadyLocked(target)) {
      VLOG(1) << node_id_ << " StartTransfer: " << target << " is ready, sending TimeoutNow";
      transfer_ctx_.state = TransferState::kWaitingElection;
      send_now = true;
    } else {
      VLOG(1) << node_id_ << " StartTransfer: waiting for " << target << " to catch up";
      transfer_ctx_.state = TransferState::kWaitingCatchUp;
    }
  }
  if (send_now)
    SendTimeoutNowToTarget();
  return true;
}

bool RaftNode::IsTransferReady(const NodeId& target) const {
  std::lock_guard guard(mutex_);
  return IsTransferReadyLocked(target);
}

bool RaftNode::IsTransferReadyLocked(const NodeId& target) const {
  if (!log_storage_)
    return false;

  LogIndex last_index = log_storage_->LastIndex();
  for (size_t i = 0; i < last_peer_ids_.size() && i < peer_last_log_index_.size(); i++) {
    if (last_peer_ids_[i] == target) {
      return peer_last_log_index_[i] >= last_index;
    }
  }
  return false;
}

void RaftNode::CancelTransfer() {
  std::lock_guard guard(mutex_);
  CancelTransferLocked();
}

void RaftNode::CancelTransferLocked() {
  if (!transfer_ctx_.IsActive())
    return;
  VLOG(1) << node_id_ << " CancelTransfer: cancelling transfer to " << transfer_ctx_.target;
  transfer_ctx_.Reset();
}

void RaftNode::CheckTransferTimeoutLocked() {
  if (!transfer_ctx_.IsActive())
    return;
  if (NowMs() - transfer_ctx_.start_ms >= transfer_timeout_ms_) {
    VLOG(1) << node_id_ << " Transfer timeout to " << transfer_ctx_.target
            << " after " << transfer_timeout_ms_ << "ms — cancelling";
    CancelTransferLocked();
  }
}

void RaftNode::SendTimeoutNowToTarget() {
  TimeoutNowRequest req;
  {
    std::lock_guard guard(mutex_);
    if (!transport_ || transfer_ctx_.target.empty() || role_ != RaftRole::Leader)
      return;
    req.group_id = group_id_;
    req.term = storage_.current_term();
    req.leader_id = node_id_;
  }

  VLOG(1) << node_id_ << " Sending TimeoutNow to " << req.leader_id;
  TimeoutNowResponse resp = transport_->SendTimeoutNow(transfer_ctx_.target, req);

  std::lock_guard guard(mutex_);
  if (role_ != RaftRole::Leader)
    return;
  if (!resp.accepted) {
    VLOG(1) << node_id_ << " TimeoutNow rejected by " << transfer_ctx_.target;
    CancelTransferLocked();
  }
}

AppendEntriesResponse RaftNode::OnAppendEntries(const AppendEntriesRequest& req) {
  std::lock_guard guard(mutex_);
  Term cur_term = storage_.current_term();
  if (req.term < cur_term) {
    VLOG(2) << node_id_ << " rejects AppendEntries from " << req.leader_id
            << ": stale term " << req.term << " < " << cur_term;
    LogIndex my_last = log_storage_ ? log_storage_->LastIndex() : 0;
    return {group_id_, cur_term, false, my_last};
  }

  if (req.term > cur_term) {
    VLOG(1) << node_id_ << " accepts AppendEntries from leader " << req.leader_id
            << " term=" << req.term << " entries=" << req.entries.size();
    BecomeFollowerLocked(req.term);
  } else if (role_ != RaftRole::Leader) {
    // Same-term message from the legitimate leader.
    election_timer_.Reset();
  }

  if (!log_storage_)
    return {group_id_, storage_.current_term(), true, 0};

  LogIndex my_last = log_storage_->LastIndex();

  // Log consistency check against prev_log_index/term. GetTerm() covers both
  // live entries and the snapshot anchor (compacted prefix).
  if (req.prev_log_index > my_last) {
    VLOG(2) << node_id_ << " rejects AppendEntries: gap at prev_log=" << req.prev_log_index;
    return {group_id_, storage_.current_term(), false, my_last};
  }
  if (req.prev_log_index > 0 &&
      log_storage_->GetTerm(req.prev_log_index) != req.prev_log_term) {
    VLOG(2) << node_id_ << " rejects AppendEntries: conflict at " << req.prev_log_index;
    return {group_id_, storage_.current_term(), false, req.prev_log_index - 1};
  }

  // Conflict resolution: truncate at the FIRST conflicting entry and delete
  // everything after it, then append. (Fixes the bug where the un-sent tail
  // survived truncation.)
  size_t max_entries = std::min(req.entries.size(), kMaxAppendBatch);
  for (size_t i = 0; i < max_entries; i++) {
    const LogEntry& entry = req.entries[i];
    if (entry.index <= log_storage_->LastIndex()) {
      const LogEntry* existing = log_storage_->Get(entry.index);
      if (existing && existing->term == entry.term)
        continue;  // identical entry — keep going
      VLOG(1) << node_id_ << " truncate from " << (entry.index - 1);
      log_storage_->TruncateFrom(entry.index - 1);
      log_storage_->Append(entry);
    } else if (entry.index == log_storage_->LastIndex() + 1) {
      log_storage_->Append(entry);
    } else {
      // Gap — cannot happen after the prev_log consistency check.
      return {group_id_, storage_.current_term(), false, log_storage_->LastIndex()};
    }
  }

  // Truncate any local tail the leader did not send: those entries are
  // uncommitted junk from a previous leadership and must not survive.
  // Only applies when the request CARRIES entries — an empty AppendEntries is
  // a progress/heartbeat message and must never truncate the follower's log.
  if (!req.entries.empty()) {
    LogIndex leader_tail = req.entries.back().index;
    if (log_storage_->LastIndex() > leader_tail)
      log_storage_->TruncateFrom(leader_tail);
  }

  my_last = log_storage_->LastIndex();

  if (req.leader_commit > commit_index_) {
    VLOG(1) << node_id_ << " commit_index " << commit_index_
            << " -> " << std::min(req.leader_commit, my_last) << " (from leader)";
    commit_index_ = std::min(req.leader_commit, my_last);
    ApplyCommittedLogsLocked();
  }

  return {group_id_, storage_.current_term(), true, my_last};
}

InstallSnapshotResponse RaftNode::OnInstallSnapshot(const InstallSnapshotRequest& req) {
  std::lock_guard guard(mutex_);
  Term cur_term = storage_.current_term();
  if (req.term < cur_term) {
    VLOG(2) << node_id_ << " rejects InstallSnapshot from " << req.leader_id
            << ": stale term " << req.term << " < " << cur_term;
    return {group_id_, cur_term, false};
  }

  if (req.term > cur_term) {
    VLOG(1) << node_id_ << " accepts InstallSnapshot from leader " << req.leader_id
            << " index=" << req.last_included_index;
    BecomeFollowerLocked(req.term);
  }

  if (!snapshot_receiver_) {
    LOG(WARNING) << node_id_ << " no SnapshotReceiver installed";
    return {group_id_, storage_.current_term(), false};
  }

  InstallSnapshotResponse rsp = snapshot_receiver_->HandleChunk(req);

  if (rsp.success && req.done) {
    VLOG(1) << node_id_ << " snapshot complete: index=" << req.last_included_index
            << " term=" << req.last_included_term;

    if (state_machine_) {
      if (!state_machine_->LoadSnapshot(snapshot_receiver_->bin_path())) {
        LOG(WARNING) << node_id_ << " failed to load snapshot from "
                     << snapshot_receiver_->bin_path();
        rsp.success = false;
        return rsp;
      }
    }

    last_applied_ = req.last_included_index;
    commit_index_ = std::max(commit_index_, req.last_included_index);
    last_snapshot_index_ = req.last_included_index;
    last_snapshot_term_ = req.last_included_term;
    apply_progress_.Update(last_applied_);

    if (log_storage_) {
      log_storage_->Clear();
      log_storage_->SetSnapshotAnchor(last_snapshot_index_, last_snapshot_term_);
    }

    VLOG(1) << node_id_ << " state restored: last_applied=" << last_applied_
            << " commit_index=" << commit_index_
            << " snapshot_index=" << last_snapshot_index_;
  }

  return rsp;
}

ApplyResult RaftNode::SubmitEntry(LogEntry entry) {
  {
    std::lock_guard guard(mutex_);
    if (!log_storage_)
      return {ApplyOp::ERROR, 0};
    if (role_ != RaftRole::Leader) {
      VLOG(1) << "SubmitEntry rejected: not leader (role=" << role_ << ")";
      return {ApplyOp::ERROR, 0};
    }
    entry.term = storage_.current_term();
    entry.index = 0;  // the log assigns the index
    log_storage_->Append(std::move(entry));
  }
  return ReplicateLog(/*low_latency=*/true);  // client write: quorum suffices
}

ApplyResult RaftNode::ReplicateLog(bool low_latency) {
  struct PeerReq {
    NodeId id;
    size_t idx = 0;  // position in the peer arrays (peer_next_index_ etc.)
    LogIndex next = 0;
    LogEntry prev;
    bool need_snapshot = false;
    LogIndex snapshot_index = 0;
    Term snapshot_term = 0;
    std::vector<LogEntry> entries;
  };

  ApplyResult result;

  // Bounded retry loop: after a rejection, nextIndex has been backed off, so
  // retry immediately in the same call — converges followers fast instead of
  // waiting for the next heartbeat. Cap prevents pathological spinning.
  for (int round = 0; round < 8; round++) {
    // --- Phase 1 (locked): capture peers + build requests. ---
    Term current_term = 0;
    size_t majority = 1;  // self
    LogIndex my_last = 0;
    std::vector<PeerReq> preq;
    bool retry_needed = false;
    {
      std::lock_guard guard(mutex_);
      if (!log_storage_)
        return result;
      if (role_ != RaftRole::Leader)
        return result;
      current_term = storage_.current_term();
      auto peer_ids = GetPeerIdsLocked();
      ResizePeerArraysLocked(peer_ids.size());
      last_peer_ids_ = peer_ids;
      majority = cluster_config_.voters.size() / 2 + 1;
      my_last = log_storage_->LastIndex();

      // Send to peers that ACKed the last heartbeat round FIRST: a write
      // needs only a MAJORITY of responses, so a partitioned peer must
      // never sit on the critical path of the healthy majority.
      std::vector<size_t> order(peer_ids.size());
      for (size_t i = 0; i < order.size(); i++)
        order[i] = i;
      std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        return peer_hb_ok_[a] && !peer_hb_ok_[b];
      });

      for (size_t i : order) {
        PeerReq r;
        r.id = peer_ids[i];
        r.idx = i;
        r.next = peer_next_index_[i];
        if (r.next <= 1)
          r.next = 1;

        if (ShouldInstallSnapshot(r.next, last_snapshot_index_)) {
          r.need_snapshot = true;
          r.snapshot_index = last_snapshot_index_;
          r.snapshot_term = last_snapshot_term_;
        } else {
          if (r.next > 1) {
            const LogEntry* prev = log_storage_->Get(r.next - 1);
            if (prev) {
              r.prev = *prev;
            } else {
              // r.next-1 may be covered by the snapshot anchor (compacted).
              r.prev.index = r.next - 1;
              r.prev.term = log_storage_->GetTerm(r.next - 1);
            }
          }
          // Copy the entry range under the lock: the caller reads it outside
          // the lock, so it must not alias the mutable in-memory log.
          r.entries = log_storage_->GetRange(r.next, kMaxAppendBatch);
        }
        preq.push_back(std::move(r));
      }
    }

    if (preq.empty()) {
      // Single-node leader: no peers, but majority(1/1) is trivially
      // satisfied — commit and apply immediately.
      std::lock_guard guard(mutex_);
      if (role_ != RaftRole::Leader)
        return result;
      AdvanceCommitIndexLocked();
      MaybeAutoFinalizeJointLocked();
      return ApplyCommittedLogsLocked();
    }

    // --- Phase 2: RPCs WITHOUT the lock. ---
    // low_latency (client write path): healthy-first order, break as soon as
    // the responses received (plus self) satisfy the quorum — a frozen peer
    // costs nothing on the write path.
    // Catch-up path: send to EVERY reachable, behind peer — no early break
    // (see the declaration comment).
    std::vector<LogIndex> rsp_match(preq.size(), 0);
    std::vector<bool> rsp_ok(preq.size(), false);
    std::vector<bool> rsp_attempted(preq.size(), false);
    std::vector<Term> rsp_term(preq.size(), 0);
    size_t successes = 1;  // self
    for (size_t i = 0; i < preq.size(); i++) {
      if (!transport_)
        break;
      const PeerReq& r = preq[i];
      if (low_latency) {
        if (successes >= majority)
          break;
      } else {
        // Catch-up: skip peers that are unreachable (no heartbeat ACK last
        // round — a dead peer would block the heartbeat fiber for the full
        // RPC timeout and starve healthy followers' heartbeats) or already
        // caught up (nothing to push). Skipped peers must NOT be marked
        // failed in phase 3 (their nextIndex must stay put).
        if (!peer_hb_ok_[r.idx] || r.next > my_last)
          continue;
      }
      rsp_attempted[i] = true;
      if (r.need_snapshot) {
        std::string snapshot_path = snapshot_dir_ + "snapshot.bin";
        SnapshotSender sender(snapshot_path, transport_);
        bool ok = sender.SendSnapshot(r.id, group_id_, current_term, node_id_,
                                      r.snapshot_index, r.snapshot_term);
        if (ok) {
          rsp_match[i] = r.snapshot_index;
          rsp_ok[i] = true;
          successes++;
        }
        continue;
      }

      AppendEntriesRequest req;
      req.group_id = group_id_;
      req.term = current_term;
      req.leader_id = node_id_;
      req.leader_commit = commit_index_;  // captured under the lock in phase 1
      req.prev_log_index = r.prev.index;
      req.prev_log_term = r.prev.term;
      req.entries = r.entries;

      AppendEntriesResponse resp = transport_->SendAppendEntries(r.id, req);
      rsp_term[i] = resp.term;
      if (resp.success) {
        rsp_ok[i] = true;
        rsp_match[i] = r.prev.index + r.entries.size();
        successes++;
      } else {
        rsp_match[i] = 0;
      }
      if (!resp.success && resp.last_log_index < r.next - 1)
        rsp_match[i] = resp.last_log_index;  // backoff hint
    }

    // --- Phase 3 (locked): process responses, advance commit, apply. ---
    {
      std::lock_guard guard(mutex_);
      Term cur = storage_.current_term();
      Term max_term = current_term;
      for (Term t : rsp_term)
        max_term = std::max(max_term, t);
      if (max_term > cur) {
        VLOG(1) << node_id_ << " ReplicateLog: peer has higher term " << max_term;
        BecomeFollowerLocked(max_term);
        return result;
      }
      if (role_ != RaftRole::Leader || storage_.current_term() != current_term)
        return result;

      for (size_t i = 0; i < preq.size(); i++) {
        if (!rsp_attempted[i])
          continue;  // skipped (caught up / unreachable) — state unchanged
        if (rsp_ok[i]) {
          peer_last_log_index_[preq[i].idx] = rsp_match[i];
          peer_next_index_[preq[i].idx] = rsp_match[i] + 1;
        } else {
          // nextIndex backoff: drop to min(next-1, follower_last+1) so
          // conflicts converge exponentially instead of resending everything.
          LogIndex hint = rsp_match[i] + 1;
          LogIndex backoff = std::min(peer_next_index_[preq[i].idx] - 1, hint);
          if (backoff < 1)
            backoff = 1;
          peer_next_index_[preq[i].idx] = backoff;
          retry_needed = true;
          if (backoff <= last_snapshot_index_)
            peer_last_log_index_[preq[i].idx] = last_snapshot_index_;  // force snapshot path
        }
      }

      if (role_ == RaftRole::Leader) {
        AdvanceCommitIndexLocked();
        MaybeAutoFinalizeJointLocked();
      }
      result = ApplyCommittedLogsLocked();
    }

    if (!retry_needed)
      break;
  }
  return result;
}

void RaftNode::AdvanceCommitIndex() {
  std::lock_guard guard(mutex_);
  if (role_ != RaftRole::Leader)
    return;
  AdvanceCommitIndexLocked();
}

void RaftNode::AdvanceCommitIndexLocked() {
  if (!log_storage_)
    return;

  if (config_state_ == ConfigState::kJoint) {
    AdvanceCommitIndexJointLocked();
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

  // Raft §5.4.2 (Figure 8): a leader may only advance commit_index by counting
  // replicas for an entry from its CURRENT term. Entries from prior terms are
  // committed indirectly, once a current-term entry above them is committed
  // (Log Matching Property). Committing a prior-term entry purely by replica
  // count is unsafe: it can still be overwritten by a future leader.
  if (candidate > commit_index_) {
    Term candidate_term = log_storage_->GetTerm(candidate);
    if (candidate_term != 0 && candidate_term < storage_.current_term()) {
      VLOG(1) << node_id_ << " commit_index NOT advanced to " << candidate
              << ": entry term " << candidate_term << " < current term "
              << storage_.current_term() << " (Figure 8 safety)";
      return;
    }
    VLOG(1) << node_id_ << " commit_index " << commit_index_ << " -> " << candidate;
    commit_index_ = candidate;
  }
}

void RaftNode::AdvanceCommitIndexJointLocked() {
  auto calc_config_commit = [&](const ClusterConfig& config) -> LogIndex {
    std::vector<LogIndex> indexes;
    indexes.push_back(log_storage_->LastIndex());

    for (size_t i = 0; i < last_peer_ids_.size() && i < peer_last_log_index_.size(); i++) {
      if (config.voters.count(last_peer_ids_[i]) > 0) {
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
    Term candidate_term = log_storage_->GetTerm(candidate);
    if (candidate_term != 0 && candidate_term < storage_.current_term()) {
      VLOG(1) << node_id_ << " commit_index NOT advanced to " << candidate
              << " (joint): entry term " << candidate_term << " < current term "
              << storage_.current_term() << " (Figure 8 safety)";
      return;
    }
    VLOG(1) << node_id_ << " commit_index " << commit_index_ << " -> " << candidate
            << " (joint old=" << old_commit << " new=" << new_commit << ")";
    commit_index_ = candidate;
  }
}

void RaftNode::ReplayUnappliedLogs() {
  std::lock_guard guard(mutex_);
  if (!log_storage_ || !state_machine_)
    return;
  LogIndex last = log_storage_->LastIndex();
  if (last_applied_ >= last) {
    VLOG(1) << node_id_ << " ReplayUnappliedLogs: nothing to replay (last_applied="
            << last_applied_ << " last_index=" << last << ")";
    return;
  }
  // H3 fix: a restarting node must NOT self-commit as a Follower in a
  // multi-node cluster — only the elected leader may advance commit_index.
  // A single-node cluster (no peers) is the exception: this node is the only
  // voter, so replaying up to the last log index is the leader-equivalent
  // recovery path (BootstrapSingleNode makes the same assumption).
  if (role_ != RaftRole::Leader && !GetPeerIdsLocked().empty()) {
    VLOG(1) << node_id_ << " ReplayUnappliedLogs: not leader, "
            << (last - last_applied_)
            << " entries pending — waiting for the leader to decide";
    return;
  }
  commit_index_ = last;
  ApplyCommittedLogsLocked();
}

void RaftNode::ResetApplyProgress(LogIndex to) {
  std::lock_guard guard(mutex_);
  last_applied_ = to;
  apply_progress_.Reset(to);
}

void RaftNode::SetApplyMetaFlushInterval(uint32_t interval_ms) {
  std::lock_guard guard(mutex_);
  apply_meta_flush_interval_ms_ = interval_ms;
}

ApplyResult RaftNode::ApplyCommittedLogs() {
  std::lock_guard guard(mutex_);
  return ApplyCommittedLogsLocked();
}

ApplyResult RaftNode::ApplyCommittedLogsLocked() {
  ApplyResult result;
  if (!state_machine_ || !log_storage_)
    return result;

  constexpr size_t kBatchSize = 128;

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
          joint_entry_index_ = 0;
          joint_finalize_appended_ = false;
          config_state_ = ConfigState::kStable;
          peer_manager_.SetConfig(&cluster_config_);
          storage_.SetJointConfigState(config_state_, JointConfig{});
          VLOG(1) << node_id_ << " config change step 2: entering Stable, config version="
                  << cluster_config_.version << " voters=" << cluster_config_.voters.size();
        } else {
          // Step 1: enter joint consensus
          joint_config_.old_config = cluster_config_;
          joint_config_.new_config = cmd.target;
          joint_entry_index_ = entry.index;
          joint_finalize_appended_ = false;
          config_state_ = ConfigState::kJoint;
          storage_.SetJointConfigState(config_state_, joint_config_);
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

    // Apply-progress durability. In-memory value always tracks last_applied_;
    // the disk flush is either per batch (default) or batched per interval.
    // With batching, the WAL is flushed FIRST so apply.meta can never become
    // durable ahead of the records it references (a stale apply.meta only
    // causes idempotent re-apply after recovery — never a skipped commit).
    apply_progress_.UpdateMemoryOnly(last_applied_);
    uint64_t now = NowMs();
    if (apply_meta_flush_interval_ms_ == 0 ||
        now - last_apply_meta_flush_ms_ >= apply_meta_flush_interval_ms_) {
      if (apply_meta_flush_interval_ms_ > 0 && log_storage_)
        log_storage_->Flush();
      apply_progress_.Flush();
      last_apply_meta_flush_ms_ = now;
    }
  }

  return result;
}

void RaftNode::MaybeAutoFinalizeJointLocked() {
  // When the joint (step 1) config entry is committed, the leader appends the
  // step 2 (finalize) entry automatically, as etcd does.
  if (config_state_ != ConfigState::kJoint || role_ != RaftRole::Leader ||
      !log_storage_ || joint_finalize_appended_)
    return;
  if (joint_entry_index_ == 0 || joint_entry_index_ > commit_index_)
    return;
  if (log_storage_->GetTerm(joint_entry_index_) != storage_.current_term())
    return;

  VLOG(1) << node_id_ << " auto-finalizing joint consensus, target version="
          << joint_config_.new_config.version;
  ConfigChangeCommand cmd{joint_config_.new_config};
  log_storage_->Append(LogEntry(storage_.current_term(), 0, cmd.Serialize()));
  joint_finalize_appended_ = true;
}

bool RaftNode::CreateSnapshotIfNeeded() {
  std::lock_guard guard(mutex_);
  if (!snapshot_manager_)
    return false;
  // Pass last_applied_ as the snapshot bound: the snapshot may only cover
  // entries already applied to the state machine, never the uncommitted log
  // tail. The manager records the bound in snapshot.meta and compacts the
  // WAL exactly up to it.
  bool ok = snapshot_manager_->ScheduleCreateIfNeeded(last_applied_);
  if (ok) {
    // Mirror the new snapshot bound into the node so AppendEntries correctly
    // prefers InstallSnapshot for lagging followers.
    const SnapshotMeta& m = snapshot_manager_->meta();
    if (m.index > last_snapshot_index_) {
      last_snapshot_index_ = m.index;
      last_snapshot_term_ = m.term;
    }
  }
  return ok;
}

// ---- Value getters (locked) ----

RaftRole RaftNode::role() const {
  std::lock_guard guard(mutex_);
  return role_;
}

Term RaftNode::term() const {
  std::lock_guard guard(mutex_);
  return storage_.current_term();
}

const NodeId RaftNode::voted_for() const {
  std::lock_guard guard(mutex_);
  return storage_.voted_for();
}

uint32_t RaftNode::vote_count() const {
  std::lock_guard guard(mutex_);
  return vote_count_;
}

ClusterConfig RaftNode::cluster_config() const {
  std::lock_guard guard(mutex_);
  return cluster_config_;
}

ConfigState RaftNode::config_state() const {
  std::lock_guard guard(mutex_);
  return config_state_;
}

void RaftNode::SetConfigState(ConfigState state) {
  std::lock_guard guard(mutex_);
  config_state_ = state;
}

void RaftNode::SetClusterConfig(ClusterConfig config) {
  std::lock_guard guard(mutex_);
  cluster_config_ = std::move(config);
  peer_manager_.SetConfig(&cluster_config_);
}

void RaftNode::AddPeer(const NodeId& id) {
  std::lock_guard guard(mutex_);
  cluster_config_.voters.insert(id);
}

bool RaftNode::RemovePeer(const NodeId& id) {
  std::lock_guard guard(mutex_);
  return cluster_config_.voters.erase(id) > 0;
}

JointConfig RaftNode::joint_config() const {
  std::lock_guard guard(mutex_);
  return joint_config_;
}

bool RaftNode::IsJointConsensus() const {
  std::lock_guard guard(mutex_);
  return config_state_ == ConfigState::kJoint;
}

Term RaftNode::leader_term() const {
  std::lock_guard guard(mutex_);
  return leader_term_;
}

LogIndex RaftNode::commit_index() const {
  std::lock_guard guard(mutex_);
  return commit_index_;
}

LogIndex RaftNode::last_applied() const {
  std::lock_guard guard(mutex_);
  return last_applied_;
}

uint64_t RaftNode::leader_lease_expire() const {
  std::lock_guard guard(mutex_);
  return leader_lease_expire_;
}

void RaftNode::ForceCommitIndex(LogIndex ci) {
  std::lock_guard guard(mutex_);
  commit_index_ = ci;
}

LogIndex RaftNode::last_snapshot_index() const {
  std::lock_guard guard(mutex_);
  return last_snapshot_index_;
}

Term RaftNode::last_snapshot_term() const {
  std::lock_guard guard(mutex_);
  return last_snapshot_term_;
}

void RaftNode::SetSnapshotDir(std::string dir) {
  std::lock_guard guard(mutex_);
  snapshot_dir_ = std::move(dir);
}

const LeaderTransferContext RaftNode::transfer_context() const {
  std::lock_guard guard(mutex_);
  return transfer_ctx_;
}

}  // namespace dfly