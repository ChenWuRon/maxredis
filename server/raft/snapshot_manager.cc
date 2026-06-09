// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/snapshot_manager.h"

#include <sys/stat.h>

#include "base/logging.h"

namespace dfly {

SnapshotManager::SnapshotManager(std::string dir, IStateMachine* sm,
                                 ILogStorage* log)
    : dir_(std::move(dir)), state_machine_(sm), log_storage_(log) {
  // Ensure trailing slash.
  if (!dir_.empty() && dir_.back() != '/')
    dir_ += '/';

  meta_storage_ = SnapshotMetaStorage(dir_ + "snapshot.meta");
  meta_storage_.Load();
}

SnapshotManager::~SnapshotManager() {
  Stop();
}

void SnapshotManager::Start() {
  if (snapshot_fiber_.IsJoinable())
    return;
  shutdown_.store(false, std::memory_order_release);
  snapshot_fiber_ = util::fb2::Fiber("snapshot_mgr", [this] { SnapshotLoop(); });
}

void SnapshotManager::Stop() {
  shutdown_.store(true, std::memory_order_release);
  if (snapshot_fiber_.IsJoinable())
    snapshot_fiber_.Join();
}

bool SnapshotManager::CreateSnapshot() {
  if (!state_machine_ || !log_storage_)
    return false;

  LogIndex last_index = log_storage_->LastIndex();
  LogIndex snapshot_index = meta_storage_.meta().index;

  VLOG(1) << "CreateSnapshot: last_index=" << last_index
          << " snapshot_index=" << snapshot_index;

  // Ensure snapshot directory exists.
  mkdir(dir_.c_str(), 0755);

  // Barrier: pause writes during export.
  barrier_.BeginWrite();

  bool ok = state_machine_->SaveSnapshot(dir_ + "snapshot.bin");

  // Update metadata after successful export.
  if (ok) {
    Term last_term = log_storage_->LastTerm();
    meta_storage_.SetMeta({last_index, last_term, NowMs()});
    VLOG(1) << "CreateSnapshot: OK index=" << last_index << " term=" << last_term;

    // Auto-compact the log now that a snapshot is safely persisted.
    log_storage_->CompactLogs(last_index, last_term);
  } else {
    LOG(WARNING) << "CreateSnapshot: SaveSnapshot failed";
  }

  barrier_.EndWrite();

  return ok;
}

bool SnapshotManager::ScheduleCreateIfNeeded() {
  if (!log_storage_ || !state_machine_)
    return false;

  LogIndex last_index = log_storage_->LastIndex();
  LogIndex snapshot_index = meta_storage_.meta().index;

  if (last_index < snapshot_index + log_gap_)
    return false;

  VLOG(1) << "ScheduleCreateIfNeeded: gap=" << (last_index - snapshot_index)
          << " threshold=" << log_gap_;
  return CreateSnapshot();
}

void SnapshotManager::SnapshotLoop() {
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
