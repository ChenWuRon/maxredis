// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>

#include "server/raft/snapshot_meta.h"

namespace dfly {

enum class SnapshotLoadStatus {
  OK,
  NoSnapshot,
  Corrupted,
};

struct LoadedSnapshot {
  SnapshotMeta meta;
  std::string bin_path;
};

// Validates and loads a snapshot from a directory.
// The directory is expected to contain:
//   snapshot.meta  — JSON metadata
//   snapshot.bin   — binary snapshot data
class SnapshotLoader {
 public:
  explicit SnapshotLoader(std::string dir);

  // Validates all rules and loads metadata.
  // Returns OK and populates |out| on success.
  SnapshotLoadStatus Load(LoadedSnapshot* out);

 private:
  bool ValidateMeta();
  bool ValidateBin();

  std::string dir_;
  std::string meta_path_;
  std::string bin_path_;
  SnapshotMetaStorage meta_storage_;
};

}  // namespace dfly
