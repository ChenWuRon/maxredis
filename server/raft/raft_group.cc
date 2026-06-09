#include "server/raft/raft_group.h"

#include <sys/stat.h>

#include "base/logging.h"
#include "server/raft/snapshot_manager.h"

namespace dfly {

RaftGroup::RaftGroup(GroupId group_id)
    : group_id_(group_id),
      log_storage_(std::make_unique<CommandLog>()),
      state_machine_(nullptr) {
  node_.SetLogStorage(log_storage_.get());
}

RaftGroup::~RaftGroup() {
  Shutdown();
}

bool RaftGroup::InitStorage(const std::string& base_path) {
  // Build paths: base_path/raft/group_N/wal/ and base_path/raft/group_N/snapshot/
  std::string group_dir = base_path;
  if (!group_dir.empty() && group_dir.back() != '/')
    group_dir += '/';
  group_dir += "raft/group_" + std::to_string(group_id_) + "/";

  wal_dir_ = group_dir + "wal/";
  snapshot_dir_ = group_dir + "snapshot/";

  mkdir(wal_dir_.c_str(), 0755);
  mkdir(snapshot_dir_.c_str(), 0755);

  // Set up the RaftNode storage path (meta.json, apply.meta).
  node_.SetStoragePath(group_dir);

  node_.SetSnapshotDir(snapshot_dir_);

  // Create snapshot manager that owns the snapshot cycle.
  snapshot_manager_ = std::make_unique<RaftSnapshotManager>(
      snapshot_dir_, state_machine_.get(), log_storage_.get());

  VLOG(1) << "RaftGroup " << group_id_ << " storage initialized: wal=" << wal_dir_
          << " snapshot=" << snapshot_dir_;
  return true;
}

void RaftGroup::Shutdown() {
  if (snapshot_manager_) {
    snapshot_manager_->Stop();
  }
  node_.StopHeartbeat();
}

}  // namespace dfly
