// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include "server/raft/log_storage.h"
#include "server/raft/snapshot_barrier.h"
#include "server/raft/snapshot_meta.h"
#include "server/state_machine/state_machine.h"
#include "util/fibers/fibers.h"

namespace dfly {

// Manages automatic Raft snapshot creation.
//
// Trigger policy: when LastIndex - SnapshotIndex >= log_gap_, a snapshot
// is created on a background fiber. During snapshot creation, the
// SnapshotBarrier ensures a consistent point-in-time view by blocking
// concurrent writes.
class RaftSnapshotManager {
 public:
  // |dir| is the snapshot directory (e.g. "data/raft/snapshot/").
  // Creates the directory if it doesn't exist.
  RaftSnapshotManager(std::string dir, IStateMachine* sm, ILogStorage* log);

  ~RaftSnapshotManager();

  // Starts the background snapshot fiber. Fiber is joinable until Stop().
  void Start();

  // Stops the background fiber and waits for completion.
  void Stop();

  // -- Configuration --

  void set_log_gap(uint64_t gap) {
    log_gap_ = gap;
  }

  uint64_t log_gap() const {
    return log_gap_;
  }

  // -- Accessors --

  SnapshotBarrier& barrier() {
    return barrier_;
  }

  SnapshotMetaStorage& meta_storage() {
    return meta_storage_;
  }

  const SnapshotMeta& meta() const {
    return meta_storage_.meta();
  }

  // -- Snapshot creation --

  // Creates a snapshot immediately. Blocks until complete.
  // Uses the barrier to freeze writes during export.
  bool CreateSnapshot();

  // Checks the threshold and creates a snapshot if needed.
  bool ScheduleCreateIfNeeded();

 private:
  void SnapshotLoop();

  std::string dir_;
  IStateMachine* state_machine_;
  ILogStorage* log_storage_;

  SnapshotMetaStorage meta_storage_;
  SnapshotBarrier barrier_;

  uint64_t log_gap_ = 100000;

  util::fb2::Fiber snapshot_fiber_;
  std::atomic<bool> shutdown_{false};
};

}  // namespace dfly
