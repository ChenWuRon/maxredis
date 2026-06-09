#pragma once

#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "server/raft/raft_group.h"
#include "server/raft/raft_types.h"
#include "server/raft/shard_router.h"

namespace dfly {

// Manages multiple Raft consensus groups.
// Each group has its own RaftNode, log storage, state machine, and snapshot manager.
// Groups are created, accessed, and removed by GroupId.
class RaftGroupManager {
 public:
  RaftGroupManager();

  ~RaftGroupManager();

  // Creates a new Raft group. Returns nullptr if group_id already exists.
  // The group must be initialized with InitStorage() before use.
  RaftGroup* CreateGroup(GroupId group_id);

  // Returns the group with the given id, or nullptr if not found.
  RaftGroup* GetGroup(GroupId group_id);

  const RaftGroup* GetGroup(GroupId group_id) const;

  // Removes and destroys a Raft group. Safe to call even if group doesn't exist.
  void RemoveGroup(GroupId group_id);

  // Returns the number of managed groups.
  size_t GroupCount() const {
    return groups_.size();
  }

  // Returns the number of groups that have a Leader.
  size_t LeaderCount() const;

  // Initializes storage for all groups under |base_path|.
  // Each group gets: base_path/raft/group_N/wal/, base_path/raft/group_N/snapshot/
  bool InitAllStorage(const std::string& base_path);

  // Recovers groups from disk by scanning |base_path|/raft/ for group_N/ directories.
  // For each found directory, creates a RaftGroup, initializes storage, and recovers state.
  // Returns the number of groups recovered.
  size_t RecoverFromDisk(const std::string& base_path);

  // Shuts down all groups.
  void ShutdownAll();

  // Returns list of all group IDs.
  std::vector<GroupId> GetAllGroupIds() const;

  // Access the shard router for key-to-group mapping.
  ShardRouter& shard_router() {
    return shard_router_;
  }

  const ShardRouter& shard_router() const {
    return shard_router_;
  }

  // -- Metrics -----------------------------------------------------------
  size_t total_applied_entries() const {
    return total_applied_entries_.load(std::memory_order_relaxed);
  }

  void RecordEntryApplied() {
    total_applied_entries_.fetch_add(1, std::memory_order_relaxed);
  }

  // Base path used for storage.
  const std::string& base_path() const {
    return base_path_;
  }

 private:
  absl::flat_hash_map<GroupId, std::unique_ptr<RaftGroup>> groups_;
  ShardRouter shard_router_;
  std::string base_path_;
  std::atomic<size_t> total_applied_entries_{0};
};

}  // namespace dfly
