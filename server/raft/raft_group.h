// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "server/raft/command_log.h"
#include "server/raft/raft_node.h"
#include "server/raft/raft_types.h"
#include "server/state_machine/state_machine.h"
#include "util/fibers/fibers.h"

namespace dfly {

class RaftSnapshotManager;
class SegmentLogStorage;

// RaftGroup holds all components of a single Raft consensus group:
// the RaftNode, log storage, state machine, and snapshot manager.
class RaftGroup {
 public:
  explicit RaftGroup(GroupId group_id);

  ~RaftGroup();

  GroupId group_id() const {
    return group_id_;
  }

  RaftNode& node() {
    return node_;
  }

  const RaftNode& node() const {
    return node_;
  }

  ILogStorage* log_storage() {
    return log_storage_.get();
  }

  const ILogStorage* log_storage() const {
    return log_storage_.get();
  }

  RaftSnapshotManager* snapshot_manager() {
    return snapshot_manager_.get();
  }

  const RaftSnapshotManager* snapshot_manager() const {
    return snapshot_manager_.get();
  }

  IStateMachine* state_machine() {
    return state_machine_.get();
  }

  const IStateMachine* state_machine() const {
    return state_machine_.get();
  }

  // Sets the state machine. Must be called before InitStorage().
  void SetStateMachine(IStateMachine* sm) {
    state_machine_.reset(sm);
    node_.SetStateMachine(sm);
  }

  // Takes ownership of the state machine.
  void SetStateMachine(std::unique_ptr<IStateMachine> sm) {
    state_machine_ = std::move(sm);
    node_.SetStateMachine(state_machine_.get());
  }

  // Initializes storage directory for this group.
  // Creates directories: base_path/raft/group_N/wal/, base_path/raft/group_N/snapshot/
  // Replaces the in-memory CommandLog with a persistent SegmentLogStorage.
  // |fsync_interval_ms|: 0 = fsync every WAL append (durable, slower);
  // >0 = batch fsync every interval (page-cache writes + background fsync;
  // survives kill -9, may lose the last interval on power failure).
  // Returns false if initialization fails.
  bool InitStorage(const std::string& base_path, uint32_t fsync_interval_ms = 0);

  // Releases all resources. Stops snapshot manager, heartbeat, election timer.
  void Shutdown();

 private:
  GroupId group_id_;
  RaftNode node_;
  std::unique_ptr<ILogStorage> log_storage_;
  std::unique_ptr<IStateMachine> state_machine_;
  std::unique_ptr<RaftSnapshotManager> snapshot_manager_;
  std::string wal_dir_;
  std::string snapshot_dir_;
  util::fb2::Fiber snapshot_fiber_;
  std::atomic<bool> snapshot_shutdown_{false};
  util::fb2::Fiber wal_flush_fiber_;
  std::atomic<bool> wal_flush_shutdown_{false};
};

}  // namespace dfly
