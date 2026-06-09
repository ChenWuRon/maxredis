// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>

#include "server/raft/raft_types.h"

namespace dfly {

struct InstallSnapshotRequest {
  Term term = 0;
  NodeId leader_id;
  LogIndex last_included_index = 0;
  Term last_included_term = 0;
  uint64_t offset = 0;
  bool done = false;
  std::string data;
};

struct InstallSnapshotResponse {
  Term term = 0;
  bool success = false;
};

inline bool operator==(const InstallSnapshotRequest& a, const InstallSnapshotRequest& b) {
  return a.term == b.term && a.leader_id == b.leader_id &&
         a.last_included_index == b.last_included_index &&
         a.last_included_term == b.last_included_term && a.offset == b.offset &&
         a.done == b.done && a.data == b.data;
}

inline bool operator!=(const InstallSnapshotRequest& a, const InstallSnapshotRequest& b) {
  return !(a == b);
}

inline bool operator==(const InstallSnapshotResponse& a, const InstallSnapshotResponse& b) {
  return a.term == b.term && a.success == b.success;
}

inline bool operator!=(const InstallSnapshotResponse& a, const InstallSnapshotResponse& b) {
  return !(a == b);
}

}  // namespace dfly
