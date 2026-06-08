// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/service/state_serializer.h"

#include "server/storage/db_slice.h"

namespace dfly {

SnapshotData StateSerializer::Export(const DbSlice& slice) {
  SnapshotData data;
  for (DbIndex i = 0; i < slice.db_array_size(); i++) {
    if (!slice.IsDbValid(i))
      continue;
    slice.Traverse(i, [&data](std::string_view key, const MainValue& mv) {
      data.entries.push_back({std::string(key), mv.value, mv.expire_ms});
    });
  }
  return data;
}

void StateSerializer::Import(DbSlice* slice, const SnapshotData& data) {
  for (const auto& entry : data.entries) {
    auto [it, _] = slice->AddOrFind(0, entry.key);
    it->second.value = entry.value;
    it->second.expire_ms = entry.expire_ms;
  }
}

}  // namespace dfly
