// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "server/raft/raft_types.h"

namespace dfly {

// A single key-value entry in the snapshot binary format.
struct SnapshotRecord {
  std::string key;
  std::string value;
  uint64_t expire_at = 0;
};

// Magic number for snapshot.bin header: "SNAP" as uint32_t.
inline constexpr uint32_t kSnapshotMagic = 0x50414E53;

// Binary snapshot writer. Produces:
//   [header]  magic:uint32 + num_records:uint32
//   [records] key_len:uint32 + value_len:uint32 + expire_at:uint64 + key + value
class SnapshotWriter {
 public:
  explicit SnapshotWriter(std::string path);

  ~SnapshotWriter();

  // Opens the output file (snapshot.tmp).
  bool Open();

  // Appends a single record to the output.
  bool Add(const SnapshotRecord& record);

  // Appends multiple records to the output.
  bool AddBatch(const SnapshotRecord* records, size_t count);

  // Flushes buffered data, fsyncs, and renames snapshot.tmp → path.
  // Must be called after all records are added.
  bool Finalize(uint32_t num_records);

  // Returns the path of the final snapshot file.
  const std::string& path() const { return path_; }

 private:

  std::string path_;
  std::string tmp_path_;
  FILE* fp_ = nullptr;
};

}  // namespace dfly
