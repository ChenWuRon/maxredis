## Goal
- Complete the Raft persistence and snapshot pipeline: WAL recovery → apply progress → snapshot metadata → snapshot export → automatic triggering with consistency barrier → snapshot loader + validation → restore state machine → bootstrap Raft state → delta WAL replay → end‑to‑end recovery verification.

## Constraints & Preferences
- `LastTerm()` must return the term of the last valid WAL record after recovery, not `entries_.back().term`.
- `commit_index_` is volatile per Raft paper; only `last_applied_` is persisted (via `apply.meta`).
- Snapshot format: binary with `magic:uint32 + num_records:uint32 + SnapshotRecord[]` where `SnapshotRecord` = `key_len:uint32 + value_len:uint32 + expire_at:uint64 + key + value`.
- Atomic writes everywhere: `write + fsync + rename .tmp → final`.
- Auto‑snapshot when `LastIndex - SnapshotIndex >= --snapshot_log_gap` (default 100 000).
- Snapshot barrier: readers‑writer lock → concurrent writes block during snapshot export for a point‑in‑time view.
- Snapshot validation rules: `meta.index > 0`, `meta.term > 0`, magic number `0x50414E53`, file size > 0.
- Recovery boot order: `SetLogStorage` → `SetStateMachine` → `SetStoragePath` (loads snapshot + apply_progress) → `ReplayUnappliedLogs` (batch‑replay delta via `GetRange`).
- `LoadedSnapshot::meta.index` overrides `apply.meta.last_applied` via `std::max()`.
- `SnapshotLoader` returns `NoSnapshot` when meta missing or index=0; `Corrupted` when term=0, bad magic, or empty/missing bin.

## Progress
### Done
- P0‑004C: Renamed `last_recovered_index_` → `last_index_`, added `last_term_` member; `LastTerm()` returns it; `RebuildIndex()`, `Append()`, `TruncateFrom()`, `Clear()` all maintain it. 5 tests (multi‑term, corrupted tail, empty dir, two integration variants). All 25 segment_log_storage tests pass.
- P0‑005: `ApplyProgress` persists `last_applied_` to `apply.meta`. `SetStoragePath` loads it. `ReplayUnappliedLogs()` sets `commit_index_ = LastIndex()` and calls `ApplyCommittedLogs()`. `ApplyCommittedLogs()` flushes `apply_progress_` after each batch. 15 tests (3 unit, 5 persistence, 7 integration covering all 5 acceptance criteria). All 107 existing + 15 new pass.
- P0‑006A: `SnapshotMeta` struct (`index`, `term`, `timestamp_ms`) + `SnapshotMetaStorage` with JSON persistence. 8 tests (3 unit, 5 persistence). Directory: `data/raft/snapshot/snapshot.meta`.
- P0‑006B: `SnapshotWriter` for binary snapshot format. `SnapshotRecord` = key_len/value_len/expire_at/key/value. `IStateMachine::SaveSnapshot(path)` default no‑op. `KvStateMachine::SaveSnapshot` uses `StateSerializer::Export` to collect entries from all shards in parallel, then writes via `SnapshotWriter` with atomic rename. 5 tests (empty, single, multiple, large, rename verify).
- P0‑006C/D: `SnapshotBarrier` (readers‑writer lock with fiber‑friendly yield). `SnapshotManager` with background fiber, threshold trigger (`log_gap_`), calls `CreateSnapshot()` which acquires barrier → `SaveSnapshot()` → updates `SnapshotMeta`. `KvStateMachine::Set/Del/Expire` wrapped with barrier reads. 13 tests (3 barrier, 10 manager). Target renamed to `raft_snapshot_manager_test` to avoid conflict with persistence target.
- P0‑007A: `SnapshotLoader` — validates snapshot.meta + snapshot.bin, checks magic, index>0, term>0, file size>0. Returns `NoSnapshot`/`Corrupted`/`OK`. 9 tests covering all validation rules.
- P0‑007B: `KvStateMachine::LoadSnapshot(path)` — parses snapshot.bin binary format, inserts key/value into shards via `shard_set_->Await`, skips expired entries (`expire_at < NowMs()`). `IStateMachine` gains virtual `LoadSnapshot(path)` (default false). 6 `SnapshotBinaryTest` tests (round‑trip, empty, bad magic, missing file, 10 K records, expiry preservation).
- P0‑007C: `RaftNode` gains `last_snapshot_index_`, `last_snapshot_term_` fields + getters. `SetStoragePath` wires `SnapshotLoader` → calls `state_machine_->LoadSnapshot(bin_path)` → sets `last_applied_ = max(last_applied_, loaded.meta.index)`. 5 new tests (basic bootstrap, index dominance over apply.meta, no‑state‑machine guard, snapshot+WAL coexistence, no‑snapshot graceful).
- P0‑007D: `ApplyCommittedLogs` rewritten to use `GetRange(start, limit)` batches (128 / batch) instead of per‑entry `Get(i)`. 4 new delta replay tests (snapshot+WAL, mixed apply_meta, fully covered WAL, WAL before snapshot).
- P0‑007E: End‑to‑end recovery integration test (`IntegrationRecoveryPipeline`): creates persistent WAL segments via `WalWriter`, snapshot files, and `apply.meta` on disk; destroys and recreates `SegmentLogStorage` + `RaftNode`; confirms snapshot data restored and delta WAL entries (index > snapshot index) correctly replayed. Records `snapshot_load_ms`, `wal_replay_ms`, `total_recovery_ms`.

### In Progress
- *(none)*

### Blocked
- Integration test for `KvStateMachine::LoadSnapshot` with real `EngineShardSet` — thread‑local `shard_` init conflict in test harness (separate fix needed in test infrastructure).

## Key Decisions
- `last_applied_` persisted; `commit_index_` derived as `LastIndex()` on restart (volatile). Avoids persisting a field the leader will overwrite anyway.
- Snapshot binary format chosen over protobuf for simplicity until the pipeline stabilises.
- fseek+overwrite for header num_records failed with buffered I/O; fixed by writing placeholder header in `Open()` and only patching the 4‑byte num_records field in `Finalize()`.
- Barrier uses `util::ThisFiber::Yield()` spin‑loop; acceptable because snapshot is infrequent and the critical section is short (per‑shard `Traverse`).
- Snapshot loading split into two layers: binary parser (`SnapshotBinaryTest` pure functions) and `KvStateMachine::LoadSnapshot` shard insertion. Avoids dependency on `EngineShardSet` in unit tests.
- `ApplyCommittedLogs` uses batched `GetRange` reads (128 entries) to reduce per‑iteration overhead during WAL replay, especially after snapshot recovery with large delta ranges.
- Integration recovery test uses `WalWriter` + `SegmentLogStorage` for persistent WAL segments to simulate real crash‑and‑recovery cycle without needing a full engine stack.

## Next Steps
- Wire `SnapshotManager` into `RaftEngine` (ownership, pass barrier down to `KvStateMachine`).
- Surface `--snapshot_log_gap` as a CLI flag.
- Add install/uninstall snapshot to `InstallSnapshot` RPC for leader‑follower snapshot transfer.
- Validate that `ApplyCommittedLogs` batch size is configurable (or make the constant tunable).

## Critical Context
- All P0‑005 through P0‑007E commits are on `master` (`225e978`). The full chain covers: WAL recovery → apply replay → snapshot metadata → snapshot writer → snapshot manager + barrier → snapshot loader → state machine restore → bootstrap Raft state → delta WAL replay → end‑to‑end integration.
- `IntegrationRecoveryPipeline` test records `snapshot_load_ms`, `wal_replay_ms`, `total_recovery_ms` via `std::chrono::steady_clock` timestamps.
- `SnapshotLoader::Load()` returns `NoSnapshot` when `snapshot.meta` is missing or has `index == 0`; treats empty meta file as `NoSnapshot`.
- `SnapshotLoader::Load()` returns `Corrupted` when `meta.term == 0`, bad magic, or `snapshot.bin` missing/empty.
- `KvStateMachine::LoadSnapshot` returns `true` only if all expected records are loaded (`loaded == num_records`).

## Relevant Files
- `server/raft/apply_progress.h/cc`: `ApplyProgress` — persists `last_applied_` to `apply.meta`.
- `server/raft/snapshot_barrier.h`: `SnapshotBarrier` — readers‑writer lock with fiber yield.
- `server/raft/snapshot_manager.h/cc`: `SnapshotManager` — background fiber, threshold trigger, calls `SaveSnapshot` under barrier.
- `server/raft/snapshot_meta.h/cc`: `SnapshotMeta` + `SnapshotMetaStorage` — JSON metadata.
- `server/raft/snapshot_writer.h/cc`: `SnapshotWriter` — binary format, placeholder header, atomic rename.
- `server/raft/snapshot_loader.h/cc`: `SnapshotLoader` — validation + load of snapshot metadata/bin. `SnapshotLoadStatus` enum (`OK`, `NoSnapshot`, `Corrupted`).
- `server/raft/raft_node.h/cc`: `last_snapshot_index_`, `last_snapshot_term_`, `SetStoragePath` with SnapshotLoader integration. `ApplyCommittedLogs` with `GetRange` batch reads.
- `server/state_machine/state_machine.h`: `IStateMachine` with `SaveSnapshot` + `LoadSnapshot`.
- `server/state_machine/kv_state_machine.h/cc`: production state machine with barrier integration. `LoadSnapshot(path)` parses binary and inserts into shards.
- `server/raft/segment_log_storage.h/cc`: LastIndex/LastTerm recovery (P0‑004C).
- `server/raft/snapshot_meta_test.cc`: 8 tests.
- `server/raft/snapshot_writer_test.cc`: 5 tests.
- `server/raft/snapshot_loader_test.cc`: 9 tests.
- `server/raft/raft_snapshot_manager_test.cc`: 13 tests (3 barrier, 10 manager).
- `server/raft/raft_apply_recovery_test.cc`: 25 tests (3 progress + 5 persistence + 17 integration/recovery).
- `server/state_machine/kv_state_machine_test.cc`: 6 `SnapshotBinaryTest` tests (binary format parsing).
- `server/raft/wal_writer.h/cc`: WAL segment writer for persistent log segments (used in recovery test).
