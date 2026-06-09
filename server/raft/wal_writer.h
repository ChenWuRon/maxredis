// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <cstdio>
#include <string>

#include "server/raft/raft_types.h"

namespace dfly {

// Binary WAL record header.
// Each record on disk: [header][command_data]
// Total overhead: 20 bytes per entry.
struct RecordHeader {
  uint64_t index;
  uint64_t term;
  uint32_t size;  // command string byte length
} __attribute__((packed));

static_assert(sizeof(RecordHeader) == 20,
              "RecordHeader must be 20 bytes (no padding)");

// Append-only WAL file writer for Raft log segments.
// Format: sequence of [RecordHeader][command_bytes] records.
// Crash-safe: Open creates/truncates; Flush does fwrite + fdatasync.
class WalWriter {
 public:
  WalWriter() = default;
  ~WalWriter();

  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;

  // Opens (or creates/truncates) a WAL file at path.
  // Returns true on success.
  bool Open(const std::string& path);

  // Appends one entry to the WAL. Writes to buffer (see Flush).
  void Append(const LogEntry& entry);

  // Flushes buffered data to disk with fwrite + fdatasync.
  bool Flush();

  // Flushes and closes the file.
  void Close();

  // Returns the current on-disk file size (0 if not open).
  size_t file_size() const {
    return file_size_;
  }

  bool IsOpen() const {
    return file_ != nullptr;
  }

 private:
  FILE* file_ = nullptr;
  size_t file_size_ = 0;
  std::string buf_;
};

}  // namespace dfly
