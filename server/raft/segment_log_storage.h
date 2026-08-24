// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "server/raft/log_storage.h"
#include "server/raft/manifest.h"
#include "server/raft/raft_types.h"
#include "server/raft/wal_index.h"

namespace dfly {

class WalWriter;

// ILogStorage implementation backed by segmented WAL files.
// Entries are 1-indexed with a sentinel at index 0.
//
// PERSISTENCE MODEL
// -----------------
// Append() writes the record through WalWriter and fsyncs it immediately,
// so once Append() returns, the entry survives a crash (kill -9 safe).
// Segments roll over at kMaxSegmentBytes; the current segment id is tracked
// in manifest.json.
//
// On startup, Open() discovers segment files, validates every record
// (24B header + CRC32C), and rebuilds the in-memory log. A torn write at the
// tail (partial header/payload or CRC mismatch) simply stops the scan — the
// partial record is ignored, which is exactly the crash-recovery semantic
// Raft requires.
//
// Compaction (CompactLogs) keeps the segment containing the snapshot index
// and deletes fully-covered older segments. Records covered by the snapshot
// but still present in the kept segment are ignored after recovery via the
// snapshot anchor (PruneCompacted).
class SegmentLogStorage : public ILogStorage {
 public:
  SegmentLogStorage();
  explicit SegmentLogStorage(std::string dir);
  ~SegmentLogStorage() override;

  // Opens storage: reads manifest, discovers and scans WAL segments.
  // Populates the entry vector from on-disk segment files.
  // Returns true on success. On empty/missing directory, succeeds with no entries.
  bool Open();

  size_t LogSize() const final;
  LogIndex FirstIndex() const final;
  LogIndex LastIndex() const final;
  Term LastTerm() const final;
  const LogEntry* Get(LogIndex index) const final;
  Term GetTerm(LogIndex index) const final;
  void SetSnapshotAnchor(LogIndex index, Term term) final;
  LogIndex Append(LogEntry entry, bool flush = true) final;

  // Flush (fsync) the WAL, respecting the durability policy. No-op in
  // everysec mode (a background fiber owns the periodic fsync) and in
  // in-memory mode. Callers use this to move the fsync OUT of the consensus
  // lock: append with flush=false, then Persist() after releasing mutex_.
  void Persist() override;
  std::vector<LogEntry> GetRange(LogIndex start, size_t limit = 0) const final;
  void TruncateFrom(LogIndex new_last) final;
  bool CompactUpTo(LogIndex index) final;
  void Clear() final;
  void PruneCompacted() final;

  void CompactLogs(LogIndex snapshot_index, Term snapshot_term) final;

  // fsyncs the current segment so all acknowledged records are durable.
  bool Flush() final;

  // Configures the durability policy for Append():
  //   true  (default): fsync every appended record before returning.
  //   false: only write() + fflush (page cache) before returning; a
  //          background fiber must call Flush() periodically ("everysec").
  //          Records survive kill -9 but may be lost on power failure.
  void SetFsyncPerAppend(bool value) {
    fsync_per_append_ = value;
  }

  bool fsync_per_append() const {
    return fsync_per_append_;
  }

  // Deletes WAL segment files that are entirely covered by the snapshot.
  // Keeps the segment containing snapshot_index (safety rule).
  void CompactSegments(LogIndex snapshot_index);

 private:
  std::string SegmentPath(uint32_t segment_id) const;

  // Main load flow: discovers segments and scans each one.
  void LoadSegments();

  // Scans the WAL directory and returns a sorted list of segment IDs.
  std::vector<uint32_t> DiscoverSegments() const;

  // Scans a single segment file and appends valid entries to entries_.
  // Also records each entry's disk location in index_ for O(1) random access.
  void ScanSegment(uint32_t segment_id);

  // Called by ScanSegment for each validated record to build the index
  // and track last_index_ / last_term_.
  void RebuildIndex(LogIndex index, Term term, uint32_t segment_id,
                    uint64_t offset);

  // Ensures a writer is open on the CURRENT segment, rolling over to a new
  // segment if the current one exceeds kMaxSegmentBytes.
  bool EnsureWriter();

  // Closes the writer and rolls over to segment_id+1 (updates manifest).
  bool RollSegment();

  // Deletes all segment files and resets the manifest.
  void DeleteAllSegments();

  std::string dir_;
  ManifestManager manifest_;
  std::unique_ptr<WalWriter> writer_;
  uint32_t current_segment_id_ = 0;
  // entries_[i] corresponds to log index base_index_ + i.
  // entries_[0] is a sentinel at logical index base_index_.
  // Initially base_index_ = 0 (sentinel at logical index 0).
  LogIndex base_index_ = 0;
  std::vector<LogEntry> entries_;
  // Index mapping LogIndex → on-disk location, rebuilt on startup.
  WalIndex index_;
  // Highest index and term seen during recovery / Append / TruncateFrom.
  LogIndex last_index_ = 0;
  Term last_term_ = 0;

  // Durability policy (see SetFsyncPerAppend).
  bool fsync_per_append_ = true;

  static constexpr uint64_t kMaxSegmentBytes = 64ULL * 1024 * 1024;
};

}  // namespace dfly