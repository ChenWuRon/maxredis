## Goal
Implement the complete Raft consensus protocol: Joint Consensus, InstallSnapshot, Log Compaction, Linearizable Reads (ReadIndex), Graceful Leader Transfer (TimeoutNow), and Multi-Raft architecture. Each feature has comprehensive tests covering normal operation, edge cases, failure modes, and integration scenarios.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        RaftEngine                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────┐ │
│  │  RaftGroup    │  │  RaftGroup    │  │  RaftGroup    │  │  ...    │ │
│  │  (GroupId=0) │  │  (GroupId=1) │  │  (GroupId=2) │  │         │ │
│  │              │  │              │  │              │  │         │ │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │  │         │ │
│  │ │RaftNode  │ │  │ │RaftNode  │ │  │ │RaftNode  │ │  │         │ │
│  │ │ +RaftNode│ │  │ │ +RaftNode│ │  │ │ +RaftNode│ │  │         │ │
│  │ │ +PeerMgr │ │  │ │ +PeerMgr │ │  │ │ +PeerMgr │ │  │         │ │
│  │ │ +RaftSnap│ │  │ │ +RaftSnap│ │  │ │ +RaftSnap│ │  │         │ │
│  │ │  Manager │ │  │ │  Manager │ │  │ │  Manager │ │  │         │ │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │  │         │ │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │  │         │ │
│  │ │CmdLog    │ │  │ │CmdLog    │ │  │ │CmdLog    │ │  │         │ │
│  │ │(ILogStor)│ │  │ │(ILogStor)│ │  │ │(ILogStor)│ │  │         │ │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │  │         │ │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │  │         │ │
│  │ │StateMach │ │  │ │StateMach │ │  │ │StateMach │ │  │         │ │
│  │ │(IStateMa)│ │  │ │(IStateMa)│ │  │ │(IStateMa)│ │  │         │ │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │  │         │ │
│  │              │  │              │  │              │  │         │ │
│  │ Storage:     │  │ Storage:     │  │ Storage:     │  │         │ │
│  │ raft/group_0/│  │ raft/group_1/│  │ raft/group_2/│  │         │ │
│  │  wal/        │  │  wal/        │  │  wal/        │  │         │ │
│  │  snapshot/   │  │  snapshot/   │  │  snapshot/   │  │         │ │
│  └──────────────┘  └──────────────┘  └──────────────┘  └─────────┘ │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                 RaftGroupManager                              │  │
│  │  Create(GroupId, nodes) → new RaftGroup                      │  │
│  │  Get(GroupId) → RaftGroup*                                   │  │
│  │  Remove(GroupId) → Shutdown + erase                          │  │
│  │  RecoverFromDisk(base_path) → scan raft/group_*/ dirs        │  │
│  │  LeaderCount(), GroupCount()                                 │  │
│  └───────────────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    ShardRouter                                │  │
│  │  HashSlot(key) % num_groups → GroupId                        │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │               LocalTransport (in-process)                     │  │
│  │  Routes by (GroupId, NodeId) → GroupNodeKey                   │  │
│  │  Supports: Vote, Heartbeat, AppendEntries,                    │  │
│  │            InstallSnapshot, ReadIndex, TimeoutNow             │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## Raft State Machine (per node, per group)

```
RaftNode (per peer per group):
├── RaftRole: Follower / Candidate / Leader
├── persistent state on disk:
│   ├── current_term
│   └── voted_for
├── volatile state:
│   ├── commit_index_, last_applied_
│   ├── last_snapshot_index_, last_snapshot_term_
│   ├── leader_lease_expire_ (for linearizable reads)
│   └── TransferState + LeaderTransferContext
├── heartbeat fiber (periodic AppendEntries / election timeout)
├── PeerManager → per-peer state:
│   ├── next_index, match_index, peer_last_log_index_
│   ├── voted_for_me, granted (for elections)
│   └── SetConfig() for membership changes
├── Leader-only:
│   ├── ReplicateLog() → sends AppendEntries or InstallSnapshot
│   ├── ReadIndex() → quorum confirmation + leader lease
│   ├── StartTransfer() → catch-up + TimeoutNow RPC
│   └── CompactLogs() → safe compaction point + snapshot compaction
└── Event handlers:
    ├── OnVoteRequest / OnVoteResponse
    ├── OnHeartbeat / OnAppendEntries
    ├── OnInstallSnapshot (chunked via SnapshotReceiver)
    ├── OnReadIndex
    └── OnTimeoutNow (immediate StartElection)
```

## RaftGroup Manager Per-Group State

```
RaftGroup:
├── GroupId group_id_
├── RaftNode node_
├── CommandLog log_storage_ (ILogStorage*)
├── IStateMachine* state_machine_ (KvStateMachine)
├── RaftSnapshotManager snapshot_manager_
├── InitStorage(base_path) → creates:
│   ├── base_path/raft/group_N/wal/    (WAL segments)
│   └── base_path/raft/group_N/snapshot/ (snapshot files)
└── Shutdown() → stops snapshot manager, shuts down node
```

## Protocol Flow

### 1. Leader Election (standard Raft)
```
Follower → ElectionTimeout → Candidate → votes → Leader
```
- **TimeoutNow RPC**: Forces immediate StartElection() on target (no timeout delay).

### 2. Log Replication
```
Leader: Propose→Append to local log→Send AppendEntries→majority→Commit→Apply
Follower: Receive→Append to log→Reply→Advance commit→Apply
```
- **ShouldInstallSnapshot**: If `next_index <= last_snapshot_index_`, use InstallSnapshot instead of AppendEntries.
- **InstallSnapshot**: Chunked transfer (64KB chunks), temporary file `snapshot.recv.tmp`, fsync+rename on done.

### 3. Config Change (Joint Consensus)
```
Stable (C_old) → BeginConfigChange → Append CONFIG_CHANGE entry (kJoint)
  → ApplyCommittedLogs sets ConfigState=kJoint, GetPeerIds() returns C_old ∪ C_new
  → Append CONFIG_CHANGE entry (kStable)
  → ApplyCommittedLogs sets ConfigState=kStable, GetPeerIds() returns C_new
```
- **Dual-majority commit**: `AdvanceCommitIndexJoint()` computes min(majority(C_old), majority(C_new)).

### 4. Linearizable Read (ReadIndex protocol)
```
Client GET(key, kLinearizable)
  → Leader: Record commit_index_ → Send ReadIndex RPC to peers
  → Wait for majority acks (or fast-path via leader lease)
  → WaitForApplied(read_index) → state_machine_->Get(key)
```
- **Leader lease**: `leader_lease_expire_ = NowMs() + 100ms`. Extended on successful heartbeat + ReadIndex quorum.

### 5. Leader Transfer
```
Leader: StartTransfer(target)
  → Validate leader role, no concurrent transfer
  → Force ReplicateLog() once
  → Wait for IsTransferReady(target) → send TimeoutNow RPC
  → HeartbeatLoop: CheckTransferTimeout (3s), retry catch-up
  → CancelTransfer on SetRole() or timeout
```

### 6. Snapshot Management
```
trigger → SnapshotManager::CreateSnapshot():
  ├── SnapshotBarrier::AcquireWrite() → exclusive access to state machine
  ├── state_machine_->SaveSnapshot(path) → binary format
  ├── SnapshotMetaStorage::Save() → JSON metadata
  ├── SnapshotBarrier::ReleaseWrite()
  ├── SetSnapshotAnchor(snapshot_index, snapshot_term)
  └── CompactLogs() → CompactUpTo + CompactSegments
```

### 7. Log Compaction
```
CompactLogs(safe_point):
  compact_to = min(last_snapshot_index_, min(match_index across followers))
  log_storage_->CompactUpTo(compact_to) → removes entries, updates base_index_
  segment_log_storage_->CompactSegments(snapshot_index) → deletes WAL files before anchor segment
```
- **SnapshotAnchor**: Preserved in ILogStorage after compaction for AppendEntries consistency checks.
- **GetTerm(index)**: Returns anchor term for compacted indices.

### 8. Recovery
```
RecoverFromDisk(base_path):
  for each raft/group_N/ directory:
    RaftGroup group(N)
    group.InitStorage(base_path)  → creates directories if missing
    group.node().SetStoragePath() → LoadSnapshot + apply.meta + replay delta WAL
```

## Constraints & Preferences
- Raft paper (extended version) sections 4-6, 8, 10.
- Joint consensus: NO one-step membership change. All transitions: Stable → Joint → Stable.
- InstallSnapshot MUST be chunked (64KB), not single RPC.
- Follower MUST write to temporary file (snapshot.recv.tmp) before replacing active snapshot.bin.
- Snapshot installation MUST update: last_applied, commit_index, last_snapshot_index, last_snapshot_term.
- Compaction MUST NOT break AppendEntries consistency checks.
- Compaction MUST preserve lastIncludedIndex/lastIncludedTerm via SnapshotAnchor.
- Compaction MUST NOT delete segment containing snapshot_index.
- Compaction MUST be follower-aware: do not compact followers still need.
- Linearizable reads MUST NOT serve from local state without quorum confirmation.
- Leader transfer MUST use TimeoutNow RPC (immediate election), NOT rely on election timeout.
- Target follower MUST be fully caught up (match_index >= last_index) before transfer.
- At no point during transfer shall two valid leaders exist in the same term.
- ALL RPCs MUST carry GroupId.
- Groups MUST be independently recoverable, elect and replicate independently.

## Progress
### Done
- ISSUE-P1-001A through ISSUE-P1-001F: Joint Consensus (ClusterConfig, JointConfig, ConfigChangeCommand, dual-majority commit, config change lifecycle, 6 integration tests).
- ISSUE-P1-002A through ISSUE-P1-002E: InstallSnapshot (chunked RPC, SnapshotSender, SnapshotReceiver, state restoration, ShouldInstallSnapshot integration, 5 test suites).
- ISSUE-P1-003A through ISSUE-P1-003G: Log Compaction (FirstIndex, CompactUpTo, SegmentLogStorage::CompactSegments, SnapshotAnchor, auto-compaction in CreateSnapshot, follower-aware safe point, 3 compaction recovery tests).
- ISSUE-P1-004A through ISSUE-P1-004G: Linearizable Reads (ReadConsistency enum, ReadIndex RPC, quorum protocol, WaitForApplied, leader lease fast-path, 9 tests).
- ISSUE-P1-005A through ISSUE-P1-005E: Leader Transfer (TransferState, TimeoutNow RPC, IsTransferReady, StartTransfer, CancelTransfer, CheckTransferTimeout, 7 tests).
- ISSUE-P1-006A through ISSUE-P1-006I: Multi-Raft (GroupId in all RPCs, RaftGroup abstraction, RaftGroupManager, ShardRouter, per-group storage isolation, RecoverFromDisk, 6 integration tests).

### Next Steps
- Wire ShardRouter into RaftEngine for multi-group command submission.
- Add per-group FiberQueue to RaftGroup for serialized operation execution.
- Add stress tests: 1000-group scale, parallel snapshots, kill -9 recovery.
- Surface `--snapshot_log_gap` as CLI flag.
- Wire leader lease timeout as configurable parameter.
- Formalize metrics (replication_lag, apply_latency, election_count).

## Test Dashboard
| Test Target | Tests | Coverage |
|---|---|---|
| raft_role_test | 12 | Role transitions via MockTransport |
| raft_node_test | 87 | Full raft_node integration suite |
| vote_rpc_test | 12 | VoteRequest/Response serde |
| heartbeat_rpc_test | 6 | HeartbeatRequest/Response serde |
| append_entries_rpc_test | 6 | AppendEntriesRequest/Response serde |
| install_snapshot_rpc_test | 8 | InstallSnapshotRequest/Response serde |
| snapshot_sender_test | 8 | 64KB chunked send, multi-chunk, rejection |
| snapshot_receiver_test | 5 | Chunked receive, stale-tmp, rename |
| raft_snapshot_manager_test | 13 | Barrier, auto-snapshot trigger, compaction |
| command_log_test | 13 | CommandLog with base_index_ |
| log_storage_test | 17 | Typed ILogStorage tests |
| segment_log_storage_test | 28 | Segment storage, GetTerm, CompactSegments |
| file_log_storage_test | 16 | File-based storage, GetTerm |
| raft_engine_test | 1 | FastCommitPathSingleNode |
| raft_integration_test | 2 | SetCommandApply, MultipleCommandsApply |
| raft_apply_recovery_test | 25 | Apply progress, persistence, recovery pipeline |
| raft_group_test | 13 | CreateGroup, AccessNode, MultipleGroups |
| raft_multi_group_test | 6 | Independent elections, partition, shard routing |
| **Total** | **270** | **18 test targets, all passing** |

## Key Decisions
- AdvanceCommitIndexJoint uses dual-majority: min(majority(C_old), majority(C_new)).
- SnapshotSender::kChunkSize = 65536 (64KB) per Raft paper section 7.
- SnapshotReceiver writes to snapshot.recv.tmp, renames to snapshot.bin on done=true.
- ShouldInstallSnapshot(next_index, snapshot_index) returns true when snapshot_index > 0 && next_index <= snapshot_index.
- CompactUpTo() uses base_index_ offset approach: O(N) with std::move + resize.
- Compaction safety: compact up to min(last_snapshot_index_, min follower match_index).
- ReadIndex fast-path uses leader lease (100ms default) to skip quorum.
- Leader transfer uses dedicated TimeoutNow RPC (immediate election).
- Transfer cancelled on SetRole() (any role change) and on timeout.
- RaftSnapshotManager renamed from SnapshotManager to avoid collision with persistence SnapshotManager.
- RecoverFromDisk() scans raft/ for group_N/ directories (no separate metadata file).

## Critical Context
- ClusterConfig::voters stores only peer IDs (not self); self counted as +1 in majority.
- ConfigChangeCommand serialization: "CONFIG_CHANGE <version> <voter_count> <voters...> <learner_count> <learners...>"
- GetPeerIds() excludes self in both kStable and kJoint paths.
- LocalTransport routes by (group_id, node_id) via GroupNodeKey = {GroupId, NodeId}.
- LocalTransport::Lookup() returns nullptr for unknown (group_id, node_id) pairs.
- SnapshotAnchor lives in ILogStorage; GetTerm() returns anchor term for compacted indices.
- leader_lease_expire_ uses NowMs(); lease extended on successful heartbeat AND ReadIndex quorum.
- StartTransfer() returns synchronously but handoff completes asynchronously.
- InstallSnapshot state restoration: LoadSnapshot(), updates last_applied_, commit_index_, last_snapshot_index_, last_snapshot_term_, apply_progress_, clears log_storage_.

## Relevant Files
- `server/raft/raft_types.h`: Core types (GroupId, Term, LogIndex, LogEntry, ClusterConfig, JointConfig, ConfigChangeCommand, SnapshotAnchor, ReadConsistency, TransferState, LeaderTransferContext)
- `server/raft/raft_node.h/cc`: RaftNode — all protocol logic
- `server/raft/raft_group.h/cc`: RaftGroup — per-group container
- `server/raft/raft_group_manager.h/cc`: RaftGroupManager — lifecycle manager
- `server/raft/shard_router.h`: ShardRouter — key→group routing
- `server/raft/raft_engine.h/cc`: RaftEngine — integration layer
- `server/raft/transport.h`: Abstract Transport interface
- `server/raft/local_transport.h/cc`: In-process transport (GroupNodeKey routing)
- `server/raft/peer_manager.h/cc`: Peer tracking (next_index, match_index)
- `server/raft/log_storage.h`: ILogStorage interface (FirstIndex, CompactUpTo, GetTerm, SetSnapshotAnchor)
- `server/raft/command_log.h/cc`: CommandLog — vector-backed log with base_index_
- `server/raft/segment_log_storage.h/cc`: SegmentLogStorage — persistent WAL segments
- `server/raft/file_log_storage.h/cc`: FileLogStorage — file-backed log
- `server/raft/manifest.h`: ManifestManager for segment ID tracking
- `server/raft/snapshot_manager.h/cc`: RaftSnapshotManager — auto-snapshot + compaction
- `server/raft/snapshot_sender.h/cc`: SnapshotSender — 64KB chunked snapshot sending
- `server/raft/snapshot_receiver.h/cc`: SnapshotReceiver — tmp → rename snapshot reception
- `server/raft/snapshot_meta.h/cc`: SnapshotMeta / SnapshotMetaStorage
- `server/raft/snapshot_writer.h/cc`: Binary snapshot writer
- `server/raft/snapshot_loader.h/cc`: Binary snapshot loader
- `server/raft/snapshot_barrier.h`: Read-write barrier for consistent snapshots
- `server/raft/apply_progress.h/cc`: ApplyProgress — persist last_applied_
- `server/raft/read_index_rpc.h`: ReadIndexRequest/Response (with GroupId)
- `server/raft/timeout_now_rpc.h`: TimeoutNowRequest/Response (with GroupId)
- `server/raft/vote_rpc.h`: VoteRequest/Response (with GroupId)
- `server/raft/heartbeat_rpc.h`: HeartbeatRequest/Response (with GroupId)
- `server/raft/append_entries_rpc.h`: AppendEntriesRequest/Response (with GroupId)
- `server/raft/install_snapshot_rpc.h`: InstallSnapshotRequest/Response (with GroupId)
- `server/raft/election_timer.h`: ElectionTimer
- `server/raft/raft_storage.h`: RaftStorage — term, voted_for persistence
- `server/state_machine/state_machine.h`: IStateMachine interface
- `server/state_machine/kv_state_machine.h/cc`: KvStateMachine implementation
- `server/raft/raft_node_test.cc`: 87 integration tests
- `server/raft/raft_multi_group_test.cc`: 6 multi-group tests
