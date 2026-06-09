#include "server/raft/raft_group_manager.h"

#include <dirent.h>
#include <cstdlib>

#include <algorithm>

#include "base/logging.h"

namespace dfly {

RaftGroupManager::RaftGroupManager() {
}

RaftGroupManager::~RaftGroupManager() {
  ShutdownAll();
}

RaftGroup* RaftGroupManager::CreateGroup(GroupId group_id) {
  if (groups_.contains(group_id)) {
    LOG(WARNING) << "RaftGroupManager: group " << group_id << " already exists";
    return nullptr;
  }
  auto group = std::make_unique<RaftGroup>(group_id);
  RaftGroup* ptr = group.get();
  groups_[group_id] = std::move(group);
  VLOG(1) << "RaftGroupManager: created group " << group_id;
  return ptr;
}

RaftGroup* RaftGroupManager::GetGroup(GroupId group_id) {
  auto it = groups_.find(group_id);
  return it != groups_.end() ? it->second.get() : nullptr;
}

const RaftGroup* RaftGroupManager::GetGroup(GroupId group_id) const {
  auto it = groups_.find(group_id);
  return it != groups_.end() ? it->second.get() : nullptr;
}

size_t RaftGroupManager::LeaderCount() const {
  size_t count = 0;
  for (const auto& [gid, group] : groups_) {
    if (group->node().role() == RaftRole::Leader)
      count++;
  }
  return count;
}

void RaftGroupManager::RemoveGroup(GroupId group_id) {
  auto it = groups_.find(group_id);
  if (it != groups_.end()) {
    it->second->Shutdown();
    groups_.erase(it);
    VLOG(1) << "RaftGroupManager: removed group " << group_id;
  }
}

bool RaftGroupManager::InitAllStorage(const std::string& base_path) {
  base_path_ = base_path;
  for (auto& [gid, group] : groups_) {
    if (!group->InitStorage(base_path)) {
      LOG(WARNING) << "RaftGroupManager: failed to init storage for group " << gid;
      return false;
    }
  }
  return true;
}

size_t RaftGroupManager::RecoverFromDisk(const std::string& base_path) {
  base_path_ = base_path;

  std::string raft_dir = base_path;
  if (!raft_dir.empty() && raft_dir.back() != '/')
    raft_dir += '/';
  raft_dir += "raft/";

  DIR* dir = opendir(raft_dir.c_str());
  if (!dir) {
    VLOG(1) << "RaftGroupManager: no raft directory " << raft_dir
            << " — starting fresh";
    return 0;
  }

  size_t recovered = 0;
  struct dirent* entry;
  while ((entry = readdir(dir)) != nullptr) {
    std::string name(entry->d_name);

    // Look for directories matching "group_N"
    if (name.size() < 7 || name.compare(0, 6, "group_") != 0)
      continue;

    // Check if it's a directory
    if (entry->d_type != DT_DIR)
      continue;

    std::string suffix = name.substr(6);  // after "group_"
    if (suffix.empty())
      continue;

    char* end = nullptr;
    long gid = strtol(suffix.c_str(), &end, 10);
    if (end == suffix.c_str() || *end != '\0' || gid < 0)
      continue;

    GroupId group_id = static_cast<GroupId>(gid);

    auto group = std::make_unique<RaftGroup>(group_id);
    if (!group->InitStorage(base_path)) {
      LOG(WARNING) << "RaftGroupManager: failed to init storage for recovered group "
                    << group_id;
      continue;
    }

    // Recover unapplied logs.
    group->node().ReplayUnappliedLogs();

    groups_[group_id] = std::move(group);
    recovered++;
    LOG(INFO) << "RaftGroupManager: recovered group " << group_id;
  }

  closedir(dir);
  LOG(INFO) << "RaftGroupManager: recovered " << recovered << " groups from " << raft_dir;
  return recovered;
}

void RaftGroupManager::ShutdownAll() {
  for (auto& [gid, group] : groups_) {
    group->Shutdown();
  }
  groups_.clear();
}

std::vector<GroupId> RaftGroupManager::GetAllGroupIds() const {
  std::vector<GroupId> ids;
  ids.reserve(groups_.size());
  for (const auto& [gid, _] : groups_) {
    ids.push_back(gid);
  }
  return ids;
}

}  // namespace dfly
