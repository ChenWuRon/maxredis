# midi-redis

**A distributed, strongly-consistent in-memory key-value store** with Raft consensus, supporting Redis RESP and Memcached ASCII protocols.

---

## Features

1. **High throughput** — millions of QPS on a single node via io_uring + fiber-based concurrency
2. **Strong consistency** — full Raft consensus implementation (Leader Election, Log Replication, Commit, Apply)
3. **Linearizable reads** — ReadIndex protocol with Leader Lease optimization
4. **Membership changes** — Joint Consensus (two-phase configuration changes)
5. **Graceful leader transfer** — TimeoutNow protocol
6. **Snapshot & log compaction** — automatic snapshot creation, InstallSnapshot RPC
7. **Persistent WAL** — file-backed and segment-based log storage with CRC32 verification
8. **AOF persistence** — append-only file for command logging
9. **Multi-protocol** — Redis RESP + Memcached ASCII
10. **TLS support** — via Helio framework
11. **Pipelining mode** — batch command processing

---

## Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                           Client                                     │
│              redis-cli / memcached / custom SDK                      │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ TCP (RESP / Memcached ASCII)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       DragonflyListener                              │
│                   (connection accept, TLS handshake)                  │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Protocol Parsers                                 │
│  ┌────────────────┐  ┌──────────────────┐  ┌────────────────────┐  │
│  │  RedisParser   │  │ MemcacheParser   │  │  DflyProtocol     │  │
│  │  (RESP)        │  │  (ASCII)         │  │  (custom binary)  │  │
│  └────────────────┘  └──────────────────┘  └────────────────────┘  │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ CmdArgList
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       MainService                                   │
│  CommandRegistry::Find() → Set/Get/Del/Expire/Ping handler          │
└───────────┬─────────────────────────────────────────────────────┬───┘
            │ SET/DEL/EXPIRE (write)                               │ GET (read)
            ▼                                                      ▼
┌──────────────────────────────┐              ┌──────────────────────────┐
│       RaftEngine             │              │ RaftEngine::Schedule()  │
│  SubmitCommand(cid, args)    │              │  → DbSlice::Find()      │
│                              │              │  (kLocal, fast path)    │
│  ┌────────────────────────┐  │              └──────────────────────────┘
│  │ CommandEncoder::Encode │  │
│  │  → RESP array (binary-safe log format) │  │
│  └────────────────────────┘  │
└───────────┬──────────────────┘
            │ LogEntry{term, index, command}
            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         RaftGroup                                    │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                     RaftNode                                 │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐  │   │
│  │  │Election  │  │Heartbeat │  │Transport │  │PeerManager │  │   │
│  │  │Timer     │  │Loop      │  │(RPC)     │  │            │  │   │
│  │  └──────────┘  └──────────┘  └──────────┘  └────────────┘  │   │
│  │                                                             │   │
│  │  Core methods:                                              │   │
│  │  • StartElection / OnRequestVote / TryBecomeLeader          │   │
│  │  • ReplicateLog / OnAppendEntries                           │   │
│  │  • AdvanceCommitIndex / AdvanceCommitIndexJoint             │   │
│  │  • ApplyCommittedLogs / ReplayUnappliedLogs                 │   │
│  │  • ReadIndex / WaitForApplied                               │   │
│  │  • OnInstallSnapshot / BeginConfigChange                    │   │
│  │  • StartTransfer / SendTimeoutNowToTarget                   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌──────────────────────┐  ┌────────────────────────────────────┐   │
│  │   ILogStorage        │  │   IStateMachine                    │   │
│  │   (Log Store)        │  │   (State Machine)                  │   │
│  │  ┌────────────────┐  │  │  ┌─────────────────────────────┐  │   │
│  │  │ CommandLog     │  │  │  │  KvStateMachine             │  │   │
│  │  │ (in-memory)    │  │  │  │  • ApplyLogEntry → SET/DEL  │  │   │
│  │  │ FileLogStorage │  │  │  │  • SaveSnapshot / Load      │  │   │
│  │  │ (file-backed)  │  │  │  │  • SnapshotBarrier (freeze) │  │   │
│  │  │ SegmentLog     │  │  │  └─────────────────────────────┘  │   │
│  │  │ (segmented)    │  │  └────────────────────────────────────┘   │
│  │  └────────────────┘  │                                           │
│  └──────────────────────┘                                           │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                  Snapshot Manager                            │   │
│  │  SnapshotSender / SnapshotReceiver / SnapshotLoader          │   │
│  │  SnapshotMetaStorage / SnapshotWriter                        │   │
│  └─────────────────────────────────────────────────────────────┘   │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ ApplyResult
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   Storage Layer (Sharded KV)                        │
│                                                                      │
│  EngineShardSet:                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐           ┌──────────┐  │
│  │Shard 0   │  │Shard 1   │  │Shard 2   │    ...    │Shard N   │  │
│  │DbSlice   │  │DbSlice   │  │DbSlice   │           │DbSlice   │  │
│  │PrimeTable│  │PrimeTable│  │PrimeTable│           │PrimeTable│  │
│  └──────────┘  └──────────┘  └──────────┘           └──────────┘  │
│                                                                      │
│  Shard routing: key → hash → Shard(key, shard_count)                │
└───────────────────────┬─────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Persistence Layer                               │
│                                                                      │
│  ┌──────────────────────┐  ┌───────────────────────────────────┐   │
│  │  PersistenceManager  │  │  SnapshotManager (Service level)  │   │
│  │  • RecordCommand()   │  │  • Save snapshot.bin              │   │
│  │  • Load() / Replay   │  │  • Load snapshot.bin              │   │
│  └──────────┬───────────┘  └───────────────────────────────────┘   │
│             │                                                        │
│             ▼                                                        │
│     appendonly.aof                                                   │
└─────────────────────────────────────────────────────────────────────┘
```

### Write Path (SET a 1)

```
Client          Service          RaftEngine       RaftNode(Leader)    RaftNode(Follower)    DbSlice
  │                │                │                   │                   │                 │
  │──SET a 1──────►│                │                   │                   │                 │
  │                │──SubmitCmd────►│                   │                   │                 │
  │                │                │─Encode(cid,args)  │                   │                 │
  │                │                │─role==Leader?     │                   │                 │
  │                │                │ True              │                   │                 │
  │                │                │─Append(entry)────►│                   │                 │
  │                │                │─ReplicateLog()───►│                   │                 │
  │                │                │                   │──SendAppendEntries────────────────►│
  │                │                │                   │◄──response───────│                 │
  │                │                │                   │──SendAppendEntries────────────────►│
  │                │                │                   │◄──response───────│                 │
  │                │                │                   │─AdvanceCommitIndex(majority=2)     │
  │                │                │                   │─ApplyCommittedLogs                 │
  │                │                │                   │──────────────────────►────Set──────►│
  │◄──+OK\r\n─────│◄──result──────│◄──result─────────│                   │                 │
```

### Read Path (GET a — kLocal mode)

```
Client ──GET──► Parser ──► Service::Get()
                              │
                      engine_.Schedule(key, cb)
                              │
                      Shard(key) → shardId
                              │
                      shard_set_->Add(shardId, cb)
                              │
                      EngineShard::db_slice.Find(key)
                              │
                      SendGetReply / SendGetNotFound
                              │
Client ◄── "$1\r\n1\r\n"
```

### Read Path (kLinearizable — via ReadIndex protocol)

```
Client ──GET──► RaftEngine::Get(key, kLinearizable)
                    │
              RaftNode::ReadIndex()
                    │
              ├─ Leader Lease valid? → commit_index_
              └─ Lease expired → ReadIndex RPC to peers
                    │              wait for quorum
                    │
              WaitForApplied(read_index)
                    │
              kv_.Get(db_ind, key) → return
```

---

## Raft Algorithm Reference

This project implements the **full Raft consensus algorithm** as described in the [original Raft paper](https://raft.github.io/raft.pdf) (In Search of an Understandable Consensus Algorithm, Diego Ongaro and John Ousterhout, 2014).

| Raft Paper Section | Implementation | Source File |
|-------------------|---------------|-------------|
| §5.1 Leader Election | `BecomeCandidate()` → `StartElection()` → `TryBecomeLeader()` | `raft/raft_node.cc:131-310` |
| §5.1 RequestVote | `OnRequestVote()` (term check, voted_for, log-up-to-date) | `raft/raft_node.cc:190-228` |
| §5.2 Heartbeat | `SendHeartbeatToPeers()` / `OnHeartbeat()` | `raft/raft_node.cc:312-409` |
| §5.3 Log Replication | `ReplicateLog()` → `OnAppendEntries()` | `raft/raft_node.cc:589-765` |
| §5.3 Log Matching | `prev_log_index` + `prev_log_term` consistency check | `raft/raft_node.cc:604-628` |
| §5.3 Commit | `AdvanceCommitIndex()` (majority sort) | `raft/raft_node.cc:768-834` |
| §5.3 Apply | `ApplyCommittedLogs()` (batch apply to state machine) | `raft/raft_node.cc:851-899` |
| §5.4.1 Election Restriction | Log-up-to-date check in `OnRequestVote()` | `raft/raft_node.cc:210-222` |
| §6 Joint Consensus | `BeginConfigChange()` two-phase, `AdvanceCommitIndexJoint()` | `raft/raft_node.cc:101-129, 801-833` |
| §6 Leader Transfer | `StartTransfer()` → `IsTransferReady()` → `SendTimeoutNowToTarget()` | `raft/raft_node.cc:490-587` |
| §6.4 ReadIndex | `ReadIndex()` with Leader Lease optimization | `raft/raft_node.cc:411-488` |
| §7 Snapshot | `RaftSnapshotManager` + `InstallSnapshot` RPC | `raft/snapshot_*.h/cc` |
| §7 Log Compaction | `CompactUpTo()` + `SetSnapshotAnchor()` | `raft/log_storage.h:67-83` |

### Core Raft Types (server/raft/raft_types.h)

| Type | Description |
|------|-------------|
| `RaftRole` | `Follower`, `Candidate`, `Leader` |
| `Term` | `uint64_t` — logical clock, monotonically increasing |
| `LogIndex` | `uint64_t` — 1-indexed log position |
| `NodeId` | `std::string` — unique node identifier |
| `LogEntry` | `{term, index, command}` — a single Raft log entry |
| `ClusterConfig` | `{version, voters, learners}` — cluster membership |
| `ConfigState` | `kStable` or `kJoint` — joint consensus phase |
| `ReadConsistency` | `kLocal` (fast) or `kLinearizable` (strong) |
| `SnapshotAnchor` | `{index, term}` — preserved after log compaction |

### RPC Messages (Transport layer)

| RPC | Request | Response | File |
|-----|---------|----------|------|
| RequestVote | `{term, candidate_id, last_log_index, last_log_term}` | `{term, vote_granted}` | `vote_rpc.h` |
| AppendEntries | `{term, leader_id, prev_log_index/term, entries[], leader_commit}` | `{term, success, last_log_index}` | `append_entries_rpc.h` |
| Heartbeat | `{term, leader_id}` | `{term, success}` | `heartbeat_rpc.h` |
| InstallSnapshot | `{term, leader_id, last_included_index/term, offset, data, done}` | `{term, success}` | `install_snapshot_rpc.h` |
| ReadIndex | `{term, leader_id, request_id}` | `{term, success, commit_index}` | `read_index_rpc.h` |
| TimeoutNow | `{term, leader_id}` | `{term, accepted}` | `timeout_now_rpc.h` |

---

## Directory Structure

```
server/
├── protocol/                    Protocol parsing layer
│   ├── conn_context.h/cc        Connection context (per-connection state)
│   ├── redis_parser.h/cc        Redis RESP protocol parser
│   ├── memcache_parser.h/cc     Memcached ASCII protocol parser
│   ├── resp_expr.h/cc           RESP expression AST
│   ├── reply_builder.h/cc       RESP reply builder
│   └── dfly_protocol.h          Custom binary protocol
│
├── service/                     Service layer
│   ├── main_service.h/cc        Command handlers (SET/GET/DEL/EXPIRE/PING)
│   ├── command_registry.h/cc    Command registration and dispatch
│   ├── command_serializer.h/cc  Command serialization for logging
│   ├── dragonfly_connection.h/cc Connection lifecycle management
│   ├── dragonfly_listener.h/cc  TCP listener
│   ├── dfly_main.cc             Entry point
│   ├── config_flags.h/cc        Configuration flags
│   ├── debugcmd.h/cc            DEBUG command handler
│   ├── snapshot_fiber.h/cc      Automatic periodic snapshotting
│   ├── state_serializer.h/cc    Snapshot data export/import
│   └── test_utils.h/cc          Test utilities
│
├── storage/                     Storage layer (sharded KV)
│   ├── db_slice.h/cc            Per-shard key-value store (PrimeTable)
│   ├── engine_shard_set.h/cc    Sharding engine (multi-thread dispatch)
│   ├── common_types.h/cc        Core types (PrimeValue, MainTable, CmdArgList)
│   └── op_status.h              Operation status codes
│
├── state_machine/               State machine abstraction
│   ├── state_machine.h          IStateMachine interface
│   ├── kv_state_machine.h/cc    KvStateMachine: ApplyLogEntry, Set/Del/Get
│   └── kv_state_machine_test.cc
│
├── raft/                        ★ Raft consensus layer (77 files) ★
│   │
│   ├── Core Types & State
│   │   ├── raft_types.h         RaftRole, Term, LogIndex, LogEntry, ClusterConfig
│   │   ├── raft_storage.h/cc    Persistent state: meta.json (term, voted_for)
│   │   ├── apply_progress.h/cc  Persistent last_applied: apply.meta
│   │   └── replicated_command.h Command serialization format
│   │
│   ├── Node & Group
│   │   ├── raft_node.h/cc       ★ Core Raft node (901 lines) — election, replication, commit, apply
│   │   ├── raft_group.h/cc      RaftGroup: RaftNode + LogStorage + StateMachine + SnapshotManager
│   │   ├── raft_group_manager.h/cc Multi-group management
│   │   └── raft_engine.h/cc     RaftEngine: Service-level entry point
│   │
│   ├── Election
│   │   └── election_timer.h/cc  Randomized election timer [150, 300]ms (fiber-based)
│   │
│   ├── RPC Messages
│   │   ├── proto/raft_rpc.proto  Protobuf wire schema for all 12 RPC messages
│   │   ├── vote_rpc.h            VoteRequest / VoteResponse (C++ structs)
│   │   ├── append_entries_rpc.h  AppendEntriesRequest / AppendEntriesResponse
│   │   ├── heartbeat_rpc.h       HeartbeatRequest / HeartbeatResponse
│   │   ├── install_snapshot_rpc.h InstallSnapshotRequest / InstallSnapshotResponse
│   │   ├── read_index_rpc.h      ReadIndexRequest / ReadIndexResponse
│   │   └── timeout_now_rpc.h     TimeoutNowRequest / TimeoutNowResponse
│   │
│   ├── Transport
│   │   ├── transport.h           Transport abstract interface (6 RPC methods)
│   │   ├── raft_codec.h/cc       Frame codec: Protobuf payload + magic/type/seq/len/CRC32C
│   │   ├── tcp_transport.h/cc    TCP transport (pooled per-proactor connections)
│   │   ├── local_transport.h/cc  In-process transport for testing
│   │   └── peer_manager.h/cc     Peer node management
│   │
│   ├── Log Storage
│   │   ├── log_storage.h        ILogStorage abstract interface
│   │   ├── command_log.h/cc     In-memory log storage (1-indexed vector)
│   │   ├── file_log_storage.h/cc File-backed log storage with WAL segments
│   │   ├── segment_log_storage.h/cc Segmented WAL log storage
│   │   ├── wal_writer.h/cc      WAL file writer (CRC32, fsync)
│   │   ├── wal_index.h          In-memory index: LogIndex → (segment, offset)
│   │   └── manifest.h/cc        Segment manifest (manifest.json)
│   │
│   ├── Snapshot
│   │   ├── snapshot_meta.h/cc   SnapshotMeta, SnapshotMetaStorage
│   │   ├── snapshot_manager.h/cc Automatic snapshot creation (log_gap trigger)
│   │   ├── snapshot_barrier.h   Readers-writer barrier for consistent snapshots
│   │   ├── snapshot_sender.h/cc Leader-side: send snapshot in 64KB chunks
│   │   ├── snapshot_receiver.h/cc Follower-side: receive and assemble snapshot
│   │   ├── snapshot_loader.h/cc  Load snapshot from disk on restart
│   │   ├── snapshot_writer.h/cc  Binary snapshot file writer
│   │   └── shard_router.h       Key-to-group hash routing
│   │
│   ├── Misc
│   │   ├── command_encoder.h/cc Encode CommandId+CmdArgList → ReplicatedCommand
│   │   ├── command_log.h/cc     Legacy name for CommandLog (backward compat)
│   │   ├── crc32.h              CRC32C checksum for WAL
│   │   └── timer.h              ITimer interface
│   │
│   └── Tests (15 test files)
│       ├── raft_node_test.cc
│       ├── raft_role_test.cc
│       ├── raft_storage_test.cc
│       ├── raft_types_test.cc
│       ├── raft_engine_test.cc
│       ├── raft_group_test.cc
│       ├── raft_integration_test.cc
│       ├── raft_multi_group_test.cc
│       ├── raft_apply_recovery_test.cc
│       ├── raft_snapshot_manager_test.cc
│       ├── vote_rpc_test.cc
│       ├── append_entries_rpc_test.cc
│       ├── heartbeat_rpc_test.cc
│       ├── install_snapshot_rpc_test.cc
│       └── election_timer_test.cc
│
├── persistence/                 Persistence layer
│   ├── aof_writer.h/cc          Append-Only File writer
│   ├── persistence_manager.h/cc Persistence manager (AOF lifecycle)
│   └── snapshot_manager.h/cc    Service-level snapshot save/load
│
└── test files (*_test.cc)
    ├── command_log_test.cc
    ├── command_serializer_test.cc
    ├── log_storage_test.cc
    ├── file_log_storage_test.cc
    ├── segment_log_storage_test.cc
    ├── snapshot_meta_test.cc
    ├── snapshot_sender_test.cc
    ├── snapshot_receiver_test.cc
    ├── snapshot_loader_test.cc
    ├── snapshot_writer_test.cc
    ├── wal_writer_test.cc
    ├── kv_state_machine_test.cc
    └── state_serializer_test.cc
```

---

## Quick Start (build → run, verified)

End-to-end sequence that was verified on this machine (Ubuntu 24.04, aarch64):

```bash
# 1. Clone
git clone --recursive https://github.com/romange/midi-redis
cd midi-redis

# 2. Configure (WITH_AWS=OFF avoids the zlib1g-dev dependency; not used by this project)
cmake -B build-opt -DCMAKE_BUILD_TYPE=Release -GNinja \
      -DFETCHCONTENT_FULLY_DISCONNECTED=ON -DWITH_AWS=OFF

# 3. Build (~5-10 min; needs several GB free disk; -j limits build parallelism)
ninja -C build-opt midi-redis            # full parallelism
ninja -j4 -C build-opt midi-redis        # limit to 4 jobs (low-memory machines)

# 4. Run (foreground)
build-opt/midi-redis --logtostderr

# 5. Verify — node self-bootstraps as Raft Leader on port 6380
redis-cli -p 6380 PING          # → PONG
redis-cli -p 6380 SET hello world
redis-cli -p 6380 GET hello     # → "world"
redis-cli -p 6380 INFO raft     # role:leader, term, commit/applied index

# 6. Stop
pkill -f midi-redis             # AOF + snapshot restore automatically on restart
```

Background variant:

```bash
nohup build-opt/midi-redis --logtostderr > /tmp/midi-redis.log 2>&1 &
tail -f /tmp/midi-redis.log
```

Durable Raft mode (WAL-backed, see "Durable Raft mode" below for the disk layout):

```bash
build-opt/midi-redis --raft_dir=./data --logtostderr
```

Limiting CPU cores:

```bash
# runtime: cap the io/fiber thread pool (default: 0 = one thread per core)
build-opt/midi-redis --proactor_threads=2 --logtostderr
# log shows: "Running 2 io threads"

# OS-level alternative (also restricts any helper threads)
taskset -c 0-3 build-opt/midi-redis --logtostderr
```

If any step fails, check [Troubleshooting](#troubleshooting) below.

---

## Building from source

Tested on Ubuntu 21.04+.

```
git clone --recursive https://github.com/romange/midi-redis
cd midi-redis && ./helio/blaze.sh -release
cd build-opt && ninja midi-redis
```

Or with ninja generator for faster rebuilds:

```
./helio/blaze.sh -release -ninja
ninja -C build-opt midi-redis
```

After modifying source files only (no dependency changes):

```
ninja -C build-opt midi-redis
```

If build files become stale after restructuring:

```
cmake -B build-opt -DCMAKE_BUILD_TYPE=Release -GNinja -DFETCHCONTENT_FULLY_DISCONNECTED=ON
ninja -C build-opt midi-redis
```

### Troubleshooting

- **`ZLIB::ZLIB target not found`** — the AWS/S3 support in Helio needs `zlib1g-dev`
  (`sudo apt-get install zlib1g-dev`). If you cannot install system packages, disable
  AWS support (midi-redis itself does not use it):

  ```
  cmake -B build-opt -DCMAKE_BUILD_TYPE=Release -GNinja -DFETCHCONTENT_FULLY_DISCONNECTED=ON -DWITH_AWS=OFF
  ninja -C build-opt midi-redis
  ```

- **`No space left on device` during linking** — a full Release build (third-party
  deps + binary) needs several GB of free disk space. Check with `df -h .`; large
  reclaimable sources are usually `~/.vscode-server/cli/servers/*` (stale remote
  server versions) and npm/pip caches.

### Running Tests

```
# Core Raft tests
./build-opt/server/raft_node_test
./build-opt/server/raft_integration_test
./build-opt/server/raft_engine_test
./build-opt/server/raft_role_test
./build-opt/server/election_timer_test

# RPC tests
./build-opt/server/vote_rpc_test
./build-opt/server/append_entries_rpc_test
./build-opt/server/heartbeat_rpc_test
./build-opt/server/install_snapshot_rpc_test

# Storage tests
./build-opt/server/command_log_test
./build-opt/server/log_storage_test
./build-opt/server/file_log_storage_test
./build-opt/server/segment_log_storage_test

# Snapshot tests
./build-opt/server/raft_snapshot_manager_test
./build-opt/server/snapshot_sender_test
./build-opt/server/snapshot_receiver_test
./build-opt/server/snapshot_loader_test
./build-opt/server/snapshot_writer_test

# Apply & recovery tests
./build-opt/server/raft_apply_recovery_test

# State machine tests
./build-opt/server/kv_state_machine_test

# Multi-group tests
./build-opt/server/raft_multi_group_test
```

---

## Running

```
build-opt/midi-redis --logtostderr
```

Default listeners (verified):

| Port | Protocol | Flag |
|------|----------|------|
| 6380 | Redis RESP | `--port` |
| 8080 | HTTP (metrics/status) | `--http_port` |
| disabled | Memcached ASCII (enable by setting > 0) | `--memcache_port` |

On startup the node self-bootstraps as Raft Leader of a single-node cluster
(`INFO` shows `role:leader`). Quick check:

```
redis-cli -p 6380 PING     # → PONG
```

For more options, run `build-opt/midi-redis --help`.

### Benchmarking vs Redis

Measured on Ubuntu 24.04, aarch64, 6 cores, both servers local:

```bash
# midi-redis (left column below)
build-opt/midi-redis --logtostderr --proactor_threads=2

# reference Redis 7.2.5, pinned to 2 cores, page-cache AOF (comparable flush policy)
redis-server --port 6381 --dir /tmp/redis-data --save '' \
             --appendonly yes --appendfsync no
taskset -c 0,1 redis-server ...   # as started above

redis-benchmark -p <port> -t set,get -n 200000 -c 50 -d 64 -q        # basic
redis-benchmark -p <port> -t set,get -n 200000 -c 50 -P 16 -d 64 -q  # pipelined
```

Results (`requests/sec`, n=200k, c=50, value=64B):

| Workload | midi-redis (2 io threads) | Redis 7.2.5 (2 cores) | midi-redis / Redis |
|----------|--------------------------:|----------------------:|-------------------:|
| SET          | 30,883 | 195,695 | 16% |
| GET          | 164,609 | 180,505 | **91%** |
| SET (P16)    | 53,248 | 900,901 | 6% |
| GET (P16)    | 1,398,601 | 1,869,159 | **75%** |

Interpretation:

- **Reads are competitive**: `kLocal` GETs reach ~75–91% of stock Redis.
- **Writes pay the consensus tax**: every SET traverses the Raft pipeline
  (RESP encode → WAL append + CRC32C → commit → state-machine apply → reply),
  so even a single node costs ~6x vs Redis's in-place dict mutation. This buys
  linearizable history, replication readiness, and crash-safe WAL recovery.
- **More threads ≠ faster here**: with the default 6 io threads throughput drops
  (SET ~21k, GET ~97k) because all writes serialize on the single RaftNode mutex
  while cross-shard dispatch adds hops. On small boxes cap
  `--proactor_threads=2`.
- Pipelining (-P 16) mainly helps reads; write batching happens at the
  connection layer but each entry still commits individually.

### Snapshot

```
redis-cli -p 6380 SAVE
# Generates snapshot.bin

# Automatic snapshots:
build-opt/midi-redis --snapshot_time_sec=60 --snapshot_cmd_count=1000
```

### Persistence

AOF is enabled by default. All SET/DEL commands are recorded to `appendonly.aof`.
On restart, data is restored from `snapshot.bin` (if exists) + `appendonly.aof`.

### Durable Raft mode (WAL-backed consensus)

When `--raft_dir` is set, Raft takes over durability and the AOF/server-snapshot
path is disabled (they carry no Raft index binding and would conflict with WAL
recovery):

```
build-opt/midi-redis --raft_dir=./data
```

The WAL fsync policy is configurable (analogue of Redis AOF `everysec`):

```
# fully durable: fsync every append (default, power-loss safe)
build-opt/midi-redis --raft_dir=./data

# batched: page-cache writes + background fsync every 1000ms.
# kill -9 safe (page cache survives process crash); a power failure may
# lose the last interval. apply.meta is flushed at the same cadence, always
# AFTER the WAL, so recovery can only replay idempotent entries — never skip
# committed ones.
build-opt/midi-redis --raft_dir=./data --raft_fsync_interval_ms=1000
```

This creates the following layout, with **every** Raft state change fsynced
before it is acknowledged:

```
data/raft/group_0/
├── meta.json              # hard state: (term, voted_for) written as ONE atomic
│                          #   tmp+fsync+rename record; also persists in-flight
│                          #   joint-consensus configs
├── apply.meta             # last_applied — flushed after each apply batch
├── wal/
│   ├── manifest.json      # current segment id
│   └── segment_*.log      # segmented WAL (64MB), CRC32C per record,
│                          #   fsync on every append (kill -9 safe)
└── snapshot/
    ├── snapshot.meta      # snapshot bound = last APPLIED index (never the
    │                      #   uncommitted log tail)
    └── snapshot.bin       # state machine snapshot
```

Recovery guarantees:

- `term/voted_for` are a single atomic record — no double-vote window.
- On restart the node **never self-commits**: `commit_index` is re-established
  by the leader via AppendEntries. A single-node cluster (self-bootstrap)
  replays its own WAL; a follower in a multi-node cluster waits for the leader.
- No snapshot → the whole WAL is re-applied (idempotent for the supported
  command set); with a snapshot → only entries after the snapshot index.
- A torn tail record (partial header / CRC mismatch) stops the WAL scan —
  the partial record is ignored, exactly the crash-recovery semantic Raft
  requires.

---

## Threading Model

- **Fiber-based concurrency**: uses `util::fb2::Fiber` (user-space cooperative fibers)
- **Single-writer consensus core**: every Raft state mutation (term, vote,
  commit index, log, apply, lease, joint config) is serialized through one
  fiber-friendly mutex (`util::fb2::Mutex`) on the RaftNode. The mutex parks
  the calling *fiber* (never the OS thread), so consensus work from any
  proactor thread is safely funneled into a single logical thread. Transport
  RPCs are always issued **outside** the lock to avoid AB-BA deadlocks
  between replicating peers.
- **Cross-shard dispatch**: `EngineShardSet::Await()` schedules work on the correct Proactor thread
- **Backpressure**: `FiberQueue` (128 slots per shard) blocks the producer
  fiber when full — overload propagates back to the client connection instead
  of dropping work or exhausting memory.
- **Key fibers**:
  - `heartbeat_fiber_` — sends heartbeats every 50ms (Leader only); renews the
    leader lease **only on a majority ACK** and steps down when quorum is lost
    for `check_quorum_ms` (CheckQuorum — a partitioned leader can never keep
    serving linearizable reads). All lease math uses `steady_clock`.
  - `election_timer_fiber_` — randomized election timeout [150, 300]ms (Follower only)
  - `snapshot_fiber_` — automatic snapshot creation in background, serialized
    with log appends on the consensus lock

---

## Multi-node deployment

The consensus core is fully covered by multi-node tests (3- and 5-node
elections, replication, commit, partition/lease behavior) via the in-process
`LocalTransport`. The transport layer is pluggable
(`ITransport` in `server/raft/transport.h`) — a production deployment wires a
TCP/gRPC transport implementing the 6 RPCs (RequestVote, AppendEntries,
Heartbeat, InstallSnapshot, ReadIndex, TimeoutNow), plus a config source
(initial peer list → `SetClusterConfig`) and `RaftGroupManager` for multi-group
routing:

```
# single-node (default): self-bootstraps to Leader
build-opt/midi-redis

# linearizable GET (ReadIndex; fails over cleanly if leadership is lost)
build-opt/midi-redis --linearizable_read=true

# durable Raft state (WAL + hard state + apply progress + snapshots)
build-opt/midi-redis --raft_dir=./data --linearizable_read=true
```

`INFO` exposes a `# Raft` section (term, voted_for, commit index, last
applied, log index, snapshot index) and a real `role` field for observability
and for configuring proxies to route writes to the leader.

---

## Fault Recovery

| Failure | Recovery | Guarantee |
|---------|----------|-----------|
| Leader crash | Election timeout → new Leader elected | Uncommitted logs may be lost |
| Follower crash | Restart → catch up via AppendEntries | Committed logs preserved |
| Network partition (majority) | New Leader elected in majority partition | Service continues |
| Network partition (minority) | Partition heals → old Leader steps down | Uncommitted writes discarded |
| Log conflict | Raft conflict detection → truncation | Committed logs never overwritten |
| Full cluster crash | Restart → snapshot + WAL recovery | Depends on fsync policy |

---

## References

- [Raft Consensus Algorithm](https://raft.github.io/) — In Search of an Understandable Consensus Algorithm (Ongaro & Ousterhout, 2014)
- [etcd raft](https://github.com/etcd-io/raft) / [TiKV raft-rs](https://github.com/tikv/raft-rs) — production implementations used as reference for lease renewal, CheckQuorum, joint-consensus auto-finalize, and single-record hard-state persistence
- [MIT 6.824 Distributed Systems](https://pdos.csail.mit.edu/6.824/) — Distributed Systems course
- [Helio Framework](https://github.com/romange/helio) — Fiber-based event loop library
- [Redis Protocol Specification](https://redis.io/topics/protocol) — RESP protocol
- [Memcached Protocol](https://github.com/memcached/memcached/blob/master/doc/protocol.txt) — ASCII protocol
