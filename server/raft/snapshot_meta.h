// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>

#include "server/raft/raft_types.h"

namespace dfly {

// Metadata describing a Raft snapshot.
struct SnapshotMeta {
  LogIndex index = 0;
  Term term = 0;
  uint64_t timestamp_ms = 0;
};

// Persistent snapshot metadata storage.
// Serializes SnapshotMeta to a JSON file (snapshot.meta) with atomic writes.
class SnapshotMetaStorage {
 public:
  SnapshotMetaStorage() = default;

  // path: full path to snapshot.meta (e.g. "data/raft/snapshot/snapshot.meta").
  explicit SnapshotMetaStorage(std::string path);

  // Reads metadata from disk. Creates default meta if none exists.
  bool Load();

  // Writes current metadata to disk atomically (write + fsync + rename).
  bool Flush();

  const SnapshotMeta& meta() const {
    return meta_;
  }

  // Sets new metadata and flushes to disk.
  void SetMeta(SnapshotMeta m);

 private:
  std::string Serialize() const;
  bool Deserialize(const std::string& data);

  std::string path_;
  SnapshotMeta meta_;
};

}  // namespace dfly
