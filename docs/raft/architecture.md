# Raft Architecture

## Layer Overview

```
┌──────────────────────────────────────────────────────┐
│  RaftEngine                     (orchestrator)       │
│  - holds RaftGroup + CommandLog + KvStateMachine     │
│  - SubmitCommand() entry point for Redis commands    │
├──────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌───────────┐  ┌──────────────────┐  │
│  │ RaftGroup│  │ CommandLog│  │ KvStateMachine   │  │
│  │ (wraps   │  │(ILogStor- │  │(IStateMachine)   │  │
│  │ RaftNode)│  │ age impl) │  │                  │  │
│  └────┬─────┘  └───────────┘  └──────────────────┘  │
│       │                                               │
│       ▼                                               │
│  ┌─────────────────────────────────────────────┐      │
│  │  RaftNode              (state machine)       │      │
│  │  - role: Follower/Candidate/Leader           │      │
│  │  - term + vote tracking                      │      │
│  │  - log replication + commit/apply            │      │
│  │  - ElectionTimer (random 150-300ms)          │      │
│  │  - HeartbeatLoop (50ms interval)             │      │
│  └──────┬──────────────────────┬──────────────┘      │
│         │                      │                       │
│         ▼                      ▼                       │
│  ┌────────────┐       ┌──────────────┐                │
│  │ PeerManager│       │  Transport*  │                │
│  │ (NodeId    │       │  (interface) │                │
│  │  set)      │       │              │                │
│  └────────────┘       └──────┬───────┘                │
│                              │                          │
│                              ▼                          │
│                    ┌──────────────────┐                │
│                    │ LocalTransport   │                │
│                    │ (in-process)     │                │
│                    │ NodeId→RaftNode* │                │
│                    └──────────────────┘                │
└──────────────────────────────────────────────────────┘
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
| `ApplyCommittedLogs()` | Commit advanced | Applies via IStateMachine |

### Transport (`server/raft/transport.h`)

Pure virtual interface for RPC communication. Decouples RaftNode from transport implementation.

```cpp
class Transport {
  virtual VoteResponse SendVoteRequest(NodeId, VoteRequest) = 0;
  virtual HeartbeatResponse SendHeartbeat(NodeId, HeartbeatRequest) = 0;
  virtual AppendEntriesResponse SendAppendEntries(NodeId, AppendEntriesRequest) = 0;
};
```

### LocalTransport (`server/raft/local_transport.h/.cc`)

In-process Transport implementation. Routes RPCs to registered RaftNode instances via a `NodeId → RaftNode*` map.

### PeerManager (`server/raft/peer_manager.h/.cc`)

Sorted NodeId collection (vector + binary search). Provides cluster size, peer enumeration, and membership queries. Static for this phase — no runtime membership changes.

### ElectionTimer (`server/raft/election_timer.h/.cc`)

Fiber-based asynchronous timer with random [150, 300]ms timeout per Raft spec.

- `Start(cb)` — creates fiber, begins countdown
- `Reset()` — restarts countdown with new random timeout
- `Stop()` — marks shutdown, joins fiber
- `Deactivate()` — marks inactive without stopping fiber (used by Candidate/Leader)

Epoch-based cancellation: `Reset()` bumps an atomic epoch counter. When the fiber wakes, it compares the saved epoch; a mismatch means the timeout was reset and the callback is skipped.

### Log Storage

```
ILogStorage (interface)
  ├── CommandLog   (in-memory, server/raft/command_log.h)
  └── RaftStorage  (persistent skeleton, server/raft/raft_storage.h)
```

### State Machine

```
IStateMachine (interface)
  ├── KvStateMachine   (production, via EngineShardSet)
  └── TestStateMachine (test helper)
```

## File Layout

```
server/
├── raft/
│   ├── raft_engine.h/.cc              — Top-level orchestrator
│   ├── raft_group.h                   — RaftNode wrapper
│   ├── raft_node.h/.cc                — Core state machine
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
│   ├── raft_storage.h/.cc             — Persistent storage (skeleton)
│   ├── replicated_command.h           — Replicated command struct
│   ├── raft_engine_test.cc            — Engine tests
│   ├── raft_integration_test.cc       — Full pipeline tests
│   ├── raft_node_test.cc              — 62 node tests
│   ├── raft_role_test.cc              — 12 role transition tests
│   └── election_timer_test.cc         — 25 timer tests
├── state_machine/
│   ├── state_machine.h                — IStateMachine interface
│   └── kv_state_machine.h/.cc         — KV state machine
└── CMakeLists.txt
```
