// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/snapshot_manager.h"

#include <sys/stat.h>

#include "base/logging.h"

namespace dfly {

RaftSnapshotManager::RaftSnapshotManager(std::string dir, IStateMachine* sm,
                                 ILogStorage* log)
    : dir_(std::move(dir)), state_machine_(sm), log_storage_(log) {
  // Ensure trailing slash.
  if (!dir_.empty() && dir_.back() != '/')
    dir_ += '/';

  meta_storage_ = SnapshotMetaStorage(dir_ + "snapshot.meta");
  meta_storage_.Load();
}

RaftSnapshotManager::~RaftSnapshotManager() {
  Stop();
}

void RaftSnapshotManager::Start() {
  if (snapshot_fiber_.IsJoinable())
    return;
  shutdown_.store(false, std::memory_order_release);
  snapshot_fiber_ = util::fb2::Fiber("snapshot_mgr", [this] { SnapshotLoop(); });
}

void RaftSnapshotManager::Stop() {
  shutdown_.store(true, std::memory_order_release);
  if (snapshot_fiber_.IsJoinable())
    snapshot_fiber_.Join();
}

bool RaftSnapshotManager::CreateSnapshot(LogIndex bound_index) {
  if (!state_machine_ || !log_storage_)
    return false;

  // The snapshot is only allowed to cover entries already applied to the
  // state machine (bound_index). Baking the raw log tail into the snapshot
  // would persist UNCOMMITTED entries: a follower/restart that loads the
  // snapshot would skip them and could diverge from the committed state.
  LogIndex log_last = log_storage_->LastIndex();
  LogIndex bound = (bound_index > 0) ? std::min(bound_index, log_last) : log_last;
  // Empty log → an empty snapshot (index 0) is still valid.
  Term bound_term = (bound == log_last) ? log_storage_->LastTerm()
                                        : log_storage_->GetTerm(bound);

  LogIndex snapshot_index = meta_storage_.meta().index;

  VLOG(1) << "CreateSnapshot: log_last=" << log_last << " bound=" << bound
          << " snapshot_index=" << snapshot_index;

  // Ensure snapshot directory exists.
  mkdir(dir_.c_str(), 0755);

  // Barrier: pause writes during export.
  barrier_.BeginWrite();

  bool ok = state_machine_->SaveSnapshot(dir_ + "snapshot.bin");

  // Update metadata after successful export.
  if (ok) {
    // The meta records the BOUND (applied) index — this is the barrier point
    // the snapshot actually reflects, not the possibly-uncommitted log tail.
    meta_storage_.SetMeta({bound, bound_term, NowMs()});
    VLOG(1) << "CreateSnapshot: OK index=" << bound << " term=" << bound_term;

    // Auto-compact the log now that a snapshot is safely persisted.
    log_storage_->CompactLogs(bound, bound_term);
  } else {
    LOG(WARNING) << "CreateSnapshot: SaveSnapshot failed";
  }

  barrier_.EndWrite();

  return ok;
}

bool RaftSnapshotManager::ScheduleCreateIfNeeded(LogIndex bound_index) {
  if (!log_storage_ || !state_machine_)
    return false;

  LogIndex log_last = log_storage_->LastIndex();
  LogIndex snapshot_index = meta_storage_.meta().index;

  if (log_last < snapshot_index + log_gap_)
    return false;

  VLOG(1) << "ScheduleCreateIfNeeded: gap=" << (log_last - snapshot_index)
          << " threshold=" << log_gap_;
  return CreateSnapshot(bound_index);
}

void RaftSnapshotManager::SnapshotLoop() {
  VLOG(1) << "SnapshotLoop started";
  while (!shutdown_.load(std::memory_order_acquire)) {
    ScheduleCreateIfNeeded();
    // Check every second to avoid busy-looping.
    for (int i = 0; i < 1000 && !shutdown_.load(std::memory_order_acquire); i++) {
      util::ThisFiber::SleepFor(std::chrono::milliseconds(1));
    }
  }
  VLOG(1) << "SnapshotLoop stopped";
}

}  // namespace dfly
