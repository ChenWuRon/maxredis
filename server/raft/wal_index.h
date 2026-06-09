// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <unordered_map>

#include "server/raft/raft_types.h"

namespace dfly {

// Location of an entry within a segment file on disk.
struct EntryLocation {
  uint32_t segment_id;  // segment file id (0-based)
  uint64_t offset;      // byte offset from start of file
};

// In-memory index mapping LogIndex → on-disk location.
// Enables O(1) random access via file seek + read.
class WalIndex {
 public:
  WalIndex() = default;

  void Add(LogIndex index, uint32_t segment_id, uint64_t offset) {
    map_[index] = EntryLocation{segment_id, offset};
  }

  const EntryLocation* Find(LogIndex index) const {
    auto it = map_.find(index);
    if (it == map_.end())
      return nullptr;
    return &it->second;
  }

  size_t Size() const {
    return map_.size();
  }

  // Removes all entries with index > new_last.
  // Used for Raft log conflict resolution (TruncateFrom).
  void Truncate(LogIndex new_last) {
    // Rebuild the map keeping only entries <= new_last.
    // This is O(N) but truncation is rare (only on AppendEntries conflicts).
    decltype(map_) keep;
    for (auto& [idx, loc] : map_) {
      if (idx <= new_last)
        keep.insert({idx, loc});
    }
    map_.swap(keep);
  }

  void Clear() {
    map_.clear();
  }

 private:
  std::unordered_map<LogIndex, EntryLocation> map_;
};

}  // namespace dfly
