// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/raft/raft_types.h"

namespace dfly {

struct HeartbeatRequest {
  GroupId group_id = 0;
  Term term = 0;
  NodeId leader_id;
};

struct HeartbeatResponse {
  GroupId group_id = 0;
  Term term = 0;
  bool success = false;
};

inline bool operator==(const HeartbeatRequest& a, const HeartbeatRequest& b) {
  return a.group_id == b.group_id && a.term == b.term &&
         a.leader_id == b.leader_id;
}

inline bool operator!=(const HeartbeatRequest& a, const HeartbeatRequest& b) {
  return !(a == b);
}

inline bool operator==(const HeartbeatResponse& a, const HeartbeatResponse& b) {
  return a.group_id == b.group_id && a.term == b.term &&
         a.success == b.success;
}

inline bool operator!=(const HeartbeatResponse& a, const HeartbeatResponse& b) {
  return !(a == b);
}

}  // namespace dfly
