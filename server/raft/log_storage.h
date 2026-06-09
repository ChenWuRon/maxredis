// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <vector>

#include "server/raft/raft_types.h"

namespace dfly {

// Abstract interface for Raft log storage.
// All methods are 1-indexed: entry at index 1 is the first entry.
class ILogStorage {
 public:
  virtual ~ILogStorage() = default;

  // Returns the number of entries in the log.
  virtual size_t LogSize() const = 0;

  // Returns the index of the last entry (0 if empty).
  virtual LogIndex LastIndex() const = 0;

  // Returns the term of the last entry (0 if empty).
  virtual Term LastTerm() const = 0;

  // Returns a reference to the entry at 'index'. 1-based.
  // Requires 1 <= index <= LastIndex().
  virtual const LogEntry& Get(LogIndex index) const = 0;

  // Appends a single entry. Automatically assigns the next index.
  virtual void Append(LogEntry entry) = 0;

  // Returns entries with index in [start, start + limit).
  // limit == 0 means all entries from start onward.
  virtual std::vector<LogEntry> GetRange(LogIndex start, size_t limit = 0) const = 0;

  // Removes all entries with index > new_last.
  // Used when a leader's log conflicts with our own.
  virtual void TruncateFrom(LogIndex new_last) = 0;

  // Removes all entries.
  virtual void Clear() = 0;
};

}  // namespace dfly
