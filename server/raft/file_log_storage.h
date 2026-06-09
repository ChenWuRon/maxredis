// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <cstdio>
#include <string>

#include "server/raft/log_storage.h"
#include "server/raft/wal_index.h"
#include "server/raft/wal_writer.h"

namespace dfly {

// File-backed ILogStorage implementation.
// Writes entries to a WAL file via WalWriter and maintains an in-memory
// WalIndex for O(1) random access. Get/GetRange seek directly to the
// entry's file offset instead of scanning.
class FileLogStorage : public ILogStorage {
 public:
  static constexpr uint32_t kSegmentId = 0;

  FileLogStorage();
  ~FileLogStorage();

  // Opens the storage at 'path'. Creates/truncates the WAL file.
  // If 'path' is empty, operates in-memory only (for testing).
  bool Open(const std::string& path);

  // Flushes buffered data to disk.
  bool Flush();

  // --- ILogStorage ---

  size_t LogSize() const final;
  LogIndex LastIndex() const final;
  Term LastTerm() const final;
  const LogEntry* Get(LogIndex index) const final;
  LogIndex Append(LogEntry entry) final;
  std::vector<LogEntry> GetRange(LogIndex start, size_t limit = 0) const final;
  void TruncateFrom(LogIndex new_last) final;
  void Clear() final;

 private:
  // Reads one entry from the file at the given offset.
  LogEntry ReadEntryAt(uint64_t offset) const;

  WalWriter writer_;
  WalIndex index_;
  FILE* read_file_ = nullptr;
  std::string path_;

  // In-memory tracking (persisted state tracking).
  size_t log_size_ = 0;
  LogIndex last_index_ = 0;
  Term last_term_ = 0;

  // Cache for Get() return (ILogStorage returns const LogEntry*).
  mutable LogEntry cached_entry_;
};

}  // namespace dfly
