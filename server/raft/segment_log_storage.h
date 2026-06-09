// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <vector>

#include "server/raft/log_storage.h"
#include "server/raft/raft_types.h"

namespace dfly {

// In-memory ILogStorage implementation backed by a single std::vector<LogEntry>.
// Entries are 1-indexed with a sentinel at index 0.
// Intended as the building block for future file-backed SegmentLogStorage.
class SegmentLogStorage : public ILogStorage {
 public:
  SegmentLogStorage();

  size_t LogSize() const final;
  LogIndex LastIndex() const final;
  Term LastTerm() const final;
  const LogEntry* Get(LogIndex index) const final;
  LogIndex Append(LogEntry entry) final;
  std::vector<LogEntry> GetRange(LogIndex start, size_t limit = 0) const final;
  void TruncateFrom(LogIndex new_last) final;
  void Clear() final;

 private:
  // entries_[i] corresponds to log index i. entries_[0] is a sentinel.
  std::vector<LogEntry> entries_;
};

}  // namespace dfly
