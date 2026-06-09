#pragma once

#include <cstdint>
#include <string_view>

#include "server/raft/raft_types.h"

namespace dfly {

// ShardRouter maps keys to Raft groups using hash-based partitioning.
// This ensures all operations on the same key go to the same group,
// providing sequential consistency within a key.
class ShardRouter {
 public:
  explicit ShardRouter(GroupId num_groups = 1) : num_groups_(num_groups) {
  }

  // Maps |key| to a group using consistent hash-like partitioning.
  GroupId GetGroupForKey(std::string_view key) const {
    return HashSlot(key) % num_groups_;
  }

  // Simple hash function for key-to-group mapping.
  static uint32_t HashSlot(std::string_view key) {
    uint32_t hash = 5381;
    for (unsigned char c : key) {
      hash = ((hash << 5) + hash) + c;
    }
    return hash;
  }

  GroupId num_groups() const {
    return num_groups_;
  }

  void set_num_groups(GroupId n) {
    num_groups_ = n;
  }

 private:
  GroupId num_groups_ = 1;
};

}  // namespace dfly
