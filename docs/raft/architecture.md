# Raft Architecture v3

## Layer Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│  RaftEngine                     (orchestrator)                       │
│  - holds RaftGroup + CommandLog + KvStateMachine + SnapshotManager   │
│  - SubmitCommand() entry point for Redis commands                    │
├──────────────────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌───────────┐  ┌──────────────────┐  ┌─────────────┐ │
│  │ RaftGroup│  │ CommandLog│  │ KvStateMachine   │  │SnapshotMgr  │ │
│  │ (wraps   │  │(ILogStor- │  │(IStateMachine)   │  │(background   │ │
│  │ RaftNode)│  │ age impl) │  │ + Save/Load Snap │  │ fiber)      │ │
│  └────┬─────┘  └───────────┘  └──────────────────┘  └──────┬──────┘ │
│       │                                                     │        │
│       ▼                                                     │        │
│  ┌─────────────────────────────────────────────┐            │        │
│  │  RaftNode              (state machine)       │            │        │
│  │  - role: Follower/Candidate/Leader           │◄───────────┘        │
│  │  - term + vote tracking                      │  uses barrier       │
│  │  - log replication + commit/apply            │                     │
│  │  - ElectionTimer (random 150-300ms)          │                     │
│  │  - HeartbeatLoop (50ms interval)             │                     │
│  │  - last_snapshot_index_ / term_              │                     │
│  │  - SetStoragePath + ReplayUnappliedLogs      │                     │
│  └──────┬──────────────────────┬──────────────┘                     │
│         │                      │                                     │
│         ▼                      ▼                                     │
│  ┌────────────┐       ┌──────────────┐                              │
│  │ PeerManager│       │  Transport*  │                              │
│  │ (NodeId    │       │  (interface) │                              │
│  │  set)      │       │              │                              │
│  └────────────┘       └──────┬───────┘                              │
│                              │                                        │
│                              ▼                                        │
│                    ┌──────────────────┐                              │
│                    │ LocalTransport   │                              │
│                    │ (in-process)     │                              │
│                    │ NodeId→RaftNode* │                              │
│                    └──────────────────┘                              │
└──────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────┐
  │   Persistence & Recovery Stack      │
  ├─────────────────────────────────────┤
  │  SegmentLogStorage (ILogStorage)    │
  │  ├── WalScanner (discovers segs)    │
  │  ├── WalIndex (LogIndex→offset map) │
  │  └── ManifestManager (segment list) │
  │                                     │
  │  WalWriter (append-only log segs)   │
  │                                     │
  │  ApplyProgress (apply.meta JSON)    │
  │                                     │
  │  SnapshotMeta (snapshot.meta JSON)  │
  │  SnapshotWriter (binary export)     │
  │  SnapshotLoader (validate+load)     │
  │  SnapshotBarrier (R/W lock)         │
  │  SnapshotManager (auto trigger)     │
  └─────────────────────────────────────┘
```

## Component Details

### RaftEngine (`server/raft/raft_engine.h/.cc`)

The top-level orchestrator. Exposes `SubmitCommand()` for Redis command processing.

```
RaftEngine
  ├── kv_        : KvStateMachine  — applies committed entries to storage
  ├── group_     : RaftGroup        — wraps the RaftNode
  └── log_       : CommandLog       — append-only Raft log storage
```

**Key flow:**
1. `SubmitCommand()` encodes command → checks Leader role → appends to log → calls `ReplicateLog()` (multi-node) or `FastCommitPath()` (single-node)

### RaftGroup (`server/raft/raft_group.h`)

Lightweight wrapper around `RaftNode` with a `GroupId`. Reserved for future multi-group (sharded Raft) support.

```
RaftGroup
  ├── group_id_  : GroupId
  └── node_      : RaftNode
```

### RaftNode (`server/raft/raft_node.h/.cc`)

Core Raft state machine implementing the complete Raft protocol.

**State machine:**
```
Follower ──OnElectionTimeout()──▶ Candidate ──StartElection()──▶ Leader
    ▲                                                            │
    └───────── OnHeartbeat/OnAppendEntries(higher term) ────────┘
```

**Key methods:**

| Method | Trigger | Action |
|--------|---------|--------|
| `SetRole(new_role)` | Internal | Unified role transition with timer/heartbeat management |
| `OnElectionTimeout()` | ElectionTimer | Follower → Candidate |
| `StartElection()` | Candidate | Increments term, sends VoteRequests via Transport |
| `OnRequestVote(req)` | Received | Raft log-matching vote logic |
| `BecomeLeader()` | Majority won | Starts HeartbeatLoop |
| `SendHeartbeatToPeers()` | HeartbeatLoop | Sends heartbeats via Transport |
| `OnHeartbeat(req)` | Received | Resets election timer, checks term |
| `ReplicateLog()` | Leader command | Sends AppendEntries via Transport, advances commit |
| `OnAppendEntries(req)` | Received | Log validation, append, conflict resolution |
| `AdvanceCommitIndex()` | Log replication | Majority-based commit index calculation |
| `ApplyCommittedLogs()` | Commit advanced | Applies via IStateMachine (batched via GetRange) |

**Recovery methods:**

| Method | Description |
|--------|-------------|
| `SetStoragePath(path)` | Sets persistence dir; loads snapshot + apply_progress from disk |
| `ReplayUnappliedLogs()` | Post-recovery: sets `commit_index_ = LastIndex()`, batch-applies delta via `GetRange` |

### Persistence Stack

#### SegmentLogStorage (`server/raft/segment_log_storage.h/.cc`)

Persistent `ILogStorage` implementation backed by segmented WAL files on disk.

```
SegmentLogStorage : ILogStorage
  ├── dir_              : string
  ├── entries_          : vector<LogEntry>     — 1-indexed in-memory cache
  ├── index_            : WalIndex             — LogIndex→(segment,offset) map
  ├── manifest_         : ManifestManager      — tracks active segment IDs
  ├── last_index_       : LogIndex             — highest index seen
  └── last_term_        : Term                 — term of highest index
```

**Discovery flow:**
1. `Open()` reads the manifest, scans segment files in sorted order
2. `ScanSegment()` iterates each WAL file, validating headers + CRC32
3. `RebuildIndex()` records each valid entry's location and updates `last_index_`/`last_term_`
4. Entries cached in `entries_` vector for O(1) access; `GetRange()` for batched reads

**Segment file format:**
```
segment_00000000.log → segment_00000001.log → ...

segment_%08lu.log:
  [RecordHeader][command_bytes]
  [RecordHeader][command_bytes]
  ...

RecordHeader (24 bytes packed):
  index  : uint64    — LogIndex
  term   : uint64    — Term
  size   : uint32    — command string length
  crc32  : uint32    — CRC32C of command data
```

#### WalWriter (`server/raft/wal_writer.h/.cc`)

Append-only WAL segment writer. Used for both live log segments and test purposes.

```
WalWriter
  ├── Open(path)       — creates/truncates file
  ├── OpenAppend(path) — reopens existing for append (after TruncateFrom)
  ├── Append(entry)    — buffers header + command
  ├── Flush()          — fwrite + fdatasync
  └── Close()          — flush and close
```

Atomic per-entry: buffered writes, Flush does `fwrite` + `fdatasync` for crash safety.

#### WalIndex (`server/raft/wal_index.h`)

Maps `LogIndex → (segment_id, file_offset)` for O(1) random access after recovery.

#### Manifest (`server/raft/manifest.h`)

Tracks the set of active WAL segment IDs. Used by `SegmentLogStorage::DiscoverSegments()`.

#### CRC32 (`server/raft/crc32.h`)

CRC32C checksum for WAL record integrity validation.

### Snapshot Stack

#### SnapshotMeta (`server/raft/snapshot_meta.h/.cc`)

Metadata struct and JSON persistence.

```
SnapshotMeta {
  index        : LogIndex
  term         : Term
  timestamp_ms : uint64
}

SnapshotMetaStorage
  ├── Load()        — reads JSON from snapshot.meta
  ├── Flush()       — atomic write: JSON → .tmp → fsync → rename
  └── SetMeta(m)    — sets new meta and flushes
```

#### SnapshotWriter (`server/raft/snapshot_writer.h/.cc`)

Binary snapshot export format.

```
SnapshotRecord {
  key        : string_view
  value      : string_view
  expire_at  : uint64        — 0 = no expiry
}

Binary file format:
  [magic:uint32]       = 0x50414E53 ("PANS")
  [num_records:uint32]
  [SnapshotRecord]...
```

Atomic write: writes to `.tmp` → `fsync` → rename to `snapshot.bin`. Placeholder header written in `Open()`, num_records patched in `Finalize()`.

#### SnapshotLoader (`server/raft/snapshot_loader.h/.cc`)

Validates and loads a snapshot directory.

```
SnapshotLoader(path)
  ├── Load(&out) → SnapshotLoadStatus
  │   Validates:
  │   ├── snapshot.meta exists, index > 0, term > 0
  │   ├── snapshot.bin exists, size > 0, magic == 0x50414E53
  │   └── Returns OK / NoSnapshot / Corrupted
  └── out: LoadedSnapshot{ meta, bin_path }
```

#### SnapshotBarrier (`server/raft/snapshot_barrier.h`)

Readers-writer lock with fiber-friendly yield. Ensures a point-in-time view during snapshot export.

```
SnapshotBarrier
  ├── BeginRead() / EndRead()     — called by write ops before mutating DbSlice
  └── BeginWrite() / EndWrite()   — called by SaveSnapshot to freeze writes
```

Multiple readers proceed concurrently. Writer blocks until all readers drain, then blocks new readers.

#### SnapshotManager (`server/raft/snapshot_manager.h/.cc`)

Background fiber that auto-creates snapshots when the log gap exceeds threshold.

```
SnapshotManager(dir, state_machine, log_storage)
  ├── Start() / Stop()          — background fiber lifecycle
  ├── CreateSnapshot()          — immediate: barrier → SaveSnapshot → update meta
  ├── ScheduleCreateIfNeeded()  — check threshold and trigger if needed
  └── config: log_gap_          — default 100,000 entries
```

### State Machine

```
IStateMachine (interface)
  ├── Apply(CommandId*, args)
  ├── ApplyLogEntry(entry)        — parse + apply committed log entries
  ├── Set/Del/Expire/Get          — KV operations
  ├── SaveSnapshot(path)          — virtual, default no-op
  └── LoadSnapshot(path)          — virtual, default no-op

KvStateMachine : IStateMachine
  ├── SaveSnapshot(path)          — StateSerializer::Export → SnapshotWriter
  ├── LoadSnapshot(path)          — SnapshotLoader → parse → shard_set_->Await insert
  └── Barrier integration        — Set/Del/Expire wrapped in BeginRead/EndRead
```

### Recovery Flow

```
Node restart:
  1. Create SegmentLogStorage(path) → Open()
     └── Scans WAL segments, rebuilds entries_, index_, last_index_, last_term_

  2. RaftNode::SetStoragePath(path)
     └── Loads apply.meta → last_applied_
     └── Runs SnapshotLoader → if snapshot found:
           ├── last_applied_ = max(last_applied_, meta.index)
           ├── last_snapshot_index_ = meta.index
           ├── last_snapshot_term_ = meta.term
           └── state_machine_->LoadSnapshot(snapshot.bin)

  3. RaftNode::ReplayUnappliedLogs()
     └── commit_index_ = LastIndex()
     └── If (commit_index_ > last_applied_):
           └── GetRange(last_applied_ + 1, 128) in batches
           └── Apply each entry via state_machine_->ApplyLogEntry()
           └── Flush apply_progress_ after each batch
```

### RaftEngine Wiring (Planned)

```
RaftEngine
  ├── kv_              : KvStateMachine
  ├── group_           : RaftGroup (→ RaftNode)
  ├── log_             : SegmentLogStorage (persistent)
  ├── snapshot_mgr_    : SnapshotManager (background auto-snapshot)
  │
  │ Ownership:
  ├── RaftNode::SetLogStorage(&log_)       ✓
  ├── RaftNode::SetStateMachine(&kv_)      ✓
  └── RaftNode::SetStoragePath(...)        ✓
```

## File Layout

```
server/
├── raft/
│   ├── raft_engine.h/.cc              — Top-level orchestrator
│   ├── raft_group.h                   — RaftNode wrapper
│   ├── raft_node.h/.cc                — Core state machine (+ snapshot recovery)
│   ├── raft_types.h                   — Term, LogIndex, NodeId, Role, LogEntry
│   ├── transport.h                    — Transport interface
│   ├── local_transport.h/.cc          — In-process Transport
│   ├── peer_manager.h/.cc             — Peer ID management
│   ├── election_timer.h/.cc           — Election timer
│   ├── timer.h                        — ITimer interface
│   ├── vote_rpc.h                     — VoteRequest/VoteResponse
│   ├── heartbeat_rpc.h                — HeartbeatRequest/HeartbeatResponse
│   ├── append_entries_rpc.h           — AppendEntriesRequest/AppendEntriesResponse
│   ├── command_log.h/.cc              — In-memory log storage
│   ├── command_encoder.h/.cc          — Command serialization
│   ├── log_storage.h                  — ILogStorage interface
│   ├── raft_storage.h/.cc             — Persistent term/vote storage
│   ├── replicated_command.h           — Replicated command struct
│   │
│   ├── segment_log_storage.h/.cc      — Persistent log via WAL segments
│   ├── wal_writer.h/.cc               — WAL segment writer
│   ├── wal_index.h                    — LogIndex→offset map
│   ├── manifest.h                     — Segment manifest (ID list)
│   ├── file_log_storage.h             — File-based log storage (legacy)
│   ├── crc32.h                        — CRC32C checksum
│   ├── apply_progress.h/.cc           — last_applied_ persistence
│   │
│   ├── snapshot_meta.h/.cc            — Snapshot metadata + JSON I/O
│   ├── snapshot_writer.h/.cc          — Binary snapshot export
│   ├── snapshot_loader.h/.cc          — Snapshot validation + load
│   ├── snapshot_barrier.h             — Readers-writer lock for snapshots
│   ├── snapshot_manager.h/.cc         — Auto-snapshot manager (background fiber)
│   │
│   ├── raft_engine_test.cc            — Engine tests
│   ├── raft_integration_test.cc       — Full pipeline tests
│   ├── raft_node_test.cc              — 62 node tests
│   ├── raft_role_test.cc              — 12 role transition tests
│   ├── election_timer_test.cc         — 25 timer tests
│   ├── segment_log_storage_test.cc    — 25 persistent log tests
│   ├── raft_apply_recovery_test.cc    — 25 recovery tests (apply + snapshot + delta)
│   ├── snapshot_meta_test.cc          — 8 snapshot metadata tests
│   ├── snapshot_writer_test.cc        — 5 binary snapshot tests
│   ├── snapshot_loader_test.cc        — 9 validation tests
│   └── raft_snapshot_manager_test.cc  — 13 barrier + manager tests
├── state_machine/
│   ├── state_machine.h                — IStateMachine interface (+ Save/LoadSnapshot)
│   └── kv_state_machine.h/.cc         — KV state machine (+ Save/LoadSnapshot)
└── CMakeLists.txt
```
