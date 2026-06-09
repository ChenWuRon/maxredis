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
// Entries are stored with sentinel at index 0.
class ILogStorage {
 public:
  virtual ~ILogStorage() = default;

  // Returns the number of entries in the log (excluding sentinel).
  virtual size_t LogSize() const = 0;

  // Returns the index of the first available entry (0 if empty).
  virtual LogIndex FirstIndex() const = 0;

  // Returns the index of the last entry (0 if empty).
  virtual LogIndex LastIndex() const = 0;

  // Returns the term of the last entry (0 if empty).
  virtual Term LastTerm() const = 0;

  // Returns a pointer to the entry at 'index', or nullptr if index is out of range.
  // 1-based. FirstIndex() <= index <= LastIndex() for valid entries.
  virtual const LogEntry* Get(LogIndex index) const = 0;

  // Returns the term at 'index'. Returns anchor term for compacted indices
  // that have been preserved via SetSnapshotAnchor.
  virtual Term GetTerm(LogIndex index) const {
    if (index == anchor_.index)
      return anchor_.term;
    const LogEntry* e = Get(index);
    return e ? e->term : 0;
  }

  // Sets the snapshot anchor, preserving lastIncludedIndex/term so that
  // AppendEntries consistency checks remain valid after compaction.
  virtual void SetSnapshotAnchor(LogIndex index, Term term) {
    anchor_.index = index;
    anchor_.term = term;
  }

  // Appends a single entry. Automatically assigns entry.index = LastIndex() + 1.
  // Returns the assigned index.
  virtual LogIndex Append(LogEntry entry) = 0;

  // Returns entries with index in [start, start + limit).
  // limit == 0 means all entries from start onward.
  virtual std::vector<LogEntry> GetRange(LogIndex start, size_t limit = 0) const = 0;

  // Removes all entries with index > new_last. new_last itself is preserved.
  // Used when a leader's log conflicts with our own.
  virtual void TruncateFrom(LogIndex new_last) = 0;

  // Removes all entries with index <= index. FirstIndex() becomes index + 1.
  // Used during log compaction to discard entries covered by a snapshot.
  virtual bool CompactUpTo(LogIndex index) = 0;

  // Removes all entries. Retains only the sentinel.
  virtual void Clear() = 0;

  // Returns the snapshot anchor preserved after compaction.
  const SnapshotAnchor& snapshot_anchor() const {
    return anchor_;
  }

  // Compact log entries up to snapshot_index.
  // Sets snapshot anchor and calls CompactUpTo.
  // Override in segment-based storages to also delete segment files.
  virtual void CompactLogs(LogIndex snapshot_index, Term snapshot_term) {
    SetSnapshotAnchor(snapshot_index, snapshot_term);
    CompactUpTo(snapshot_index);
  }

 protected:
  SnapshotAnchor anchor_;
};

}  // namespace dfly
