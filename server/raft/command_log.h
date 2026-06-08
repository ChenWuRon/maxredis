// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <vector>

#include "server/raft/raft_types.h"

namespace dfly {

class CommandLog {
 public:
  CommandLog();

  // Returns the number of entries.
  size_t Size() const;

  // Returns the index of the last entry (0 if empty).
  LogIndex LastIndex() const;

  // Appends an entry. Automatically assigns its index.
  void Append(LogEntry entry);

  // Returns a reference to the entry at 'index'. 1-based.
  // Requires 1 <= index <= LastIndex().
  const LogEntry& Get(LogIndex index) const;

 private:
  // entries_[i] corresponds to log index i. entries_[0] is a sentinel.
  std::vector<LogEntry> entries_;
};

}  // namespace dfly
