// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <vector>

#include "server/raft/raft_types.h"

namespace dfly {

class RaftStorage {
 public:
  RaftStorage();

  Term current_term() const {
    return current_term_;
  }

  void set_current_term(Term term);

  const NodeId& voted_for() const {
    return voted_for_;
  }

  void set_voted_for(NodeId node_id);

  // Log operations (1-indexed: entry at index 1 is the first entry).

  // Returns the number of entries in the log.
  size_t LogSize() const;

  // Returns the last log entry. Assumes log is non-empty.
  const LogEntry& LastLogEntry() const;

  // Returns the index of the last entry (0 if empty).
  LogIndex LastLogIndex() const;

  // Returns the term of the last entry (0 if empty).
  Term LastLogTerm() const;

  // Returns a reference to the entry at 'index'. 1-based.
  // Assumes 1 <= index <= LogSize().
  const LogEntry& EntryAt(LogIndex index) const;

  // Appends a single entry. Automatically assigns the next index.
  void AppendLog(LogEntry entry);

  // Appends multiple entries. Automatically assigns indices.
  void AppendLog(const std::vector<LogEntry>& entries);

  // Returns entries with index in [start, start + limit).
  // limit == 0 means all entries from start onward.
  std::vector<LogEntry> ReadLog(LogIndex start, size_t limit = 0) const;

  // Removes all entries with index > new_last.
  // Used when a leader's log conflicts with our own.
  void TruncateSuffix(LogIndex new_last);

  // Removes all entries.
  void Clear();

 private:
  Term current_term_ = 0;
  NodeId voted_for_;
  // entries_[i] corresponds to log index i. entries_[0] is a sentinel.
  std::vector<LogEntry> entries_;
};

}  // namespace dfly
