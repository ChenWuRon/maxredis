// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "server/raft/log_storage.h"
#include "server/raft/manifest.h"
#include "server/raft/raft_types.h"
#include "server/raft/wal_index.h"

namespace dfly {

// ILogStorage implementation backed by a single std::vector<LogEntry>.
// Entries are 1-indexed with a sentinel at index 0.
//
// When constructed with a directory and Open() is called, it discovers and
// scans existing WAL segment files on startup, loading their entries into
// the in-memory vector.
class SegmentLogStorage : public ILogStorage {
 public:
  SegmentLogStorage();
  explicit SegmentLogStorage(std::string dir);

  // Opens storage: reads manifest, discovers and scans WAL segments.
  // Populates the entry vector from on-disk segment files.
  // Returns true on success. On empty/missing directory, succeeds with no entries.
  bool Open();

  size_t LogSize() const final;
  LogIndex LastIndex() const final;
  Term LastTerm() const final;
  const LogEntry* Get(LogIndex index) const final;
  LogIndex Append(LogEntry entry) final;
  std::vector<LogEntry> GetRange(LogIndex start, size_t limit = 0) const final;
  void TruncateFrom(LogIndex new_last) final;
  void Clear() final;

 private:
  std::string SegmentPath(uint32_t segment_id) const;

  // Main load flow: discovers segments and scans each one.
  void LoadSegments();

  // Scans the WAL directory and returns a sorted list of segment IDs.
  std::vector<uint32_t> DiscoverSegments() const;

  // Scans a single segment file and appends valid entries to entries_.
  // Also records each entry's disk location in index_ for O(1) random access.
  void ScanSegment(uint32_t segment_id);

  // Called by ScanSegment for each validated record to build the index.
  void RebuildIndex(LogIndex index, uint32_t segment_id, uint64_t offset);

  std::string dir_;
  ManifestManager manifest_;
  // entries_[i] corresponds to log index i. entries_[0] is a sentinel.
  std::vector<LogEntry> entries_;
  // Index mapping LogIndex → on-disk location, rebuilt on startup.
  WalIndex index_;
  // Highest index seen during recovery scan. Used by LastIndex() and LogSize().
  // Updated during scan and on Append/TruncateFrom.
  LogIndex last_recovered_index_ = 0;
};

}  // namespace dfly
