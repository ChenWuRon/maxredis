// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "server/raft/log_storage.h"
#include "server/raft/manifest.h"
#include "server/raft/wal_index.h"
#include "server/raft/wal_writer.h"

namespace dfly {

// File-backed ILogStorage with automatic segment rotation.
// Maintains segments: {dir}/segment_%08lu.log
// Manifest:          {dir}/manifest.json
//
// Each entry is written to the current segment via WalWriter.
// An in-memory WalIndex maps LogIndex → (segment_id, offset) for O(1) Get().
// Segments roll automatically when the current file exceeds kMaxSegmentSize.
class FileLogStorage : public ILogStorage {
 public:
  static constexpr size_t kMaxSegmentSize = 64 * 1024 * 1024;  // 64 MB

  FileLogStorage();
  ~FileLogStorage();

  // Opens storage at 'dir'. Creates directory + initial segment + manifest.
  // If dir is empty, operates in-memory only (for testing).
  bool Open(const std::string& dir);

  // Flushes the current WAL segment to disk.
  bool Flush();

  // Rolls to a new segment. Flushes current segment, increments segment ID,
  // opens a new WAL file, and updates the manifest.
  void RollSegment();

  // --- ILogStorage ---

  size_t LogSize() const final;
  LogIndex FirstIndex() const final;
  LogIndex LastIndex() const final;
  Term LastTerm() const final;
  const LogEntry* Get(LogIndex index) const final;
  Term GetTerm(LogIndex index) const final;
  void SetSnapshotAnchor(LogIndex index, Term term) final;
  LogIndex Append(LogEntry entry) final;
  std::vector<LogEntry> GetRange(LogIndex start, size_t limit = 0) const final;
  void TruncateFrom(LogIndex new_last) final;
  bool CompactUpTo(LogIndex index) final;
  void Clear() final;

 private:
  // Returns the segment file path for a given segment ID.
  std::string SegmentPath(uint32_t segment_id) const;

  // Opens (or returns cached) FILE* for reading a segment.
  FILE* GetReadFile(uint32_t segment_id) const;

  // Reads one entry from a segment file at the given offset.
  LogEntry ReadEntryAt(uint32_t segment_id, uint64_t offset) const;

  std::string dir_;
  ManifestManager manifest_;
  uint32_t current_segment_ = 0;
  WalWriter writer_;
  WalIndex index_;

  // Cached read file handles per segment. Opened lazily.
  mutable std::vector<FILE*> read_files_;

  LogIndex base_index_ = 0;
  size_t log_size_ = 0;
  LogIndex last_index_ = 0;
  Term last_term_ = 0;

  mutable LogEntry cached_entry_;
};

}  // namespace dfly
