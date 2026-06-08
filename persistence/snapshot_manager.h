// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "server/state_serializer.h"

namespace dfly {

class SnapshotEncoder {
 public:
  std::string Encode(const SnapshotData& data);
};

class SnapshotDecoder {
 public:
  bool Decode(std::string_view bytes, SnapshotData* data);
};

class SnapshotManager {
 public:
  bool Save(std::string_view path, const SnapshotData& data);
  bool Load(std::string_view path, SnapshotData* data);

 private:
  SnapshotEncoder encoder_;
  SnapshotDecoder decoder_;
};

}  // namespace dfly
