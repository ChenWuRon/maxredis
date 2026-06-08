# midi-redis

A toy memory store that supports basic commands like `SET` and `GET` for both memcached and redis protocols.
In addition, it supports redis `PING` command.

Demo features include:
1. High throughput reaching millions of QPS on a single node.
2. TLS support.
3. Pipelining mode.
4. AOF persistence and snapshot save/load.
5. Raft consensus integration (in progress).

## Architecture

```
Client
  ↓
Connection
  ↓
RESP Parser
  ↓
Service
  ↓
RaftEngine
  ↓
StateMachine
  ↓
DbSlice
  ↓
PersistenceManager
  ├── AOF
  └── Snapshot
```

## Directory Structure

```
server/
├── protocol/              RESP protocol parsing
│   ├── conn_context.h
│   ├── redis_parser.h
│   ├── memcache_parser.h
│   ├── resp_expr.h
│   ├── reply_builder.h
│   └── dfly_protocol.h
│
├── service/               Service layer
│   ├── main_service.h     Command handlers (SET, GET, DEL, ...)
│   ├── command_registry.h Command registration and dispatch
│   ├── command_serializer.h
│   ├── snapshot_fiber.h   Automatic snapshotting
│   ├── state_serializer.h Export/Import (SnapshotData)
│   └── dfly_main.cc       Entry point
│
├── storage/               Storage layer
│   ├── db_slice.h         Per-shard key-value store
│   ├── engine_shard_set.h Sharding engine
│   ├── common_types.h     PrimeValue, MainTable
│   └── op_status.h
│
├── state_machine/         State machine abstraction
│   ├── state_machine.h    IStateMachine interface
│   ├── kv_state_machine.h KvStateMachine (writes to DbSlice)
│   └── kv_state_machine.cc
│
├── raft/                  Raft consensus layer
│   ├── raft_types.h       RaftRole, Term, LogIndex, NodeId, LogEntry
│   ├── raft_storage.h     Persistent state (term, voted_for, log)
│   ├── raft_node.h        Role/term state transitions
│   ├── raft_group.h       Group wrapper
│   ├── raft_engine.h      RaftEngine (SubmitCommand entry)
│   └── command_log.h      Ordered log of commands
│
└── persistence/           Persistence layer
    ├── aof_writer.h       Append-Only File writer
    ├── persistence_manager.h
    ├── snapshot_manager.h Snapshot save/load (SnapshotEncoder/Decoder)
    └── snapshot_manager_test.cc
```

## Building from source

I've tested the build on Ubuntu 21.04+.

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

## Running

```
build-opt/midi-redis --logtostderr
```

For more options, run `build-opt/midi-redis --help`.

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
