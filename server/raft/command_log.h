// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <vector>

#include "server/raft/log_storage.h"
#include "server/raft/raft_types.h"

namespace dfly {

// In-memory implementation of ILogStorage.
// Entries are 1-indexed: entries_[0] is a sentinel.
class CommandLog : public ILogStorage {
 public:
  CommandLog();

  // --- ILogStorage interface ---

  size_t LogSize() const final;
  LogIndex LastIndex() const final;
  Term LastTerm() const final;
  const LogEntry* Get(LogIndex index) const final;
  LogIndex Append(LogEntry entry) final;
  std::vector<LogEntry> GetRange(LogIndex start, size_t limit = 0) const final;
  void TruncateFrom(LogIndex new_last) final;
  void Clear() final;

  // --- Legacy aliases (for backward compatibility) ---

  size_t Size() const {
    return LogSize();
  }

  void AppendLog(LogEntry entry) {
    Append(std::move(entry));
  }

  void AppendLog(const std::vector<LogEntry>& entries);

  // Batch append (not part of ILogStorage, kept for compatibility).
  void AppendBatch(const std::vector<LogEntry>& entries);

 private:
  // entries_[i] corresponds to log index i. entries_[0] is a sentinel.
  std::vector<LogEntry> entries_;
};

}  // namespace dfly
