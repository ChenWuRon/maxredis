// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dfly {

class DbSlice;

struct SnapshotEntry {
  std::string key;
  std::string value;
  uint64_t expire_ms = 0;
};

struct SnapshotData {
  std::vector<SnapshotEntry> entries;
};

class StateSerializer {
 public:
  static SnapshotData Export(const DbSlice& slice);
  static void Import(DbSlice* slice, const SnapshotData& data);
};

}  // namespace dfly
