// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <vector>

#include "server/raft/raft_types.h"

namespace dfly {

struct AppendEntriesRequest {
  Term term = 0;
  NodeId leader_id;
  LogIndex prev_log_index = 0;
  Term prev_log_term = 0;
  std::vector<LogEntry> entries;
  LogIndex leader_commit = 0;
};

struct AppendEntriesResponse {
  Term term = 0;
  bool success = false;
  LogIndex last_log_index = 0;
};

inline bool operator==(const AppendEntriesRequest& a, const AppendEntriesRequest& b) {
  return a.term == b.term && a.leader_id == b.leader_id &&
         a.prev_log_index == b.prev_log_index && a.prev_log_term == b.prev_log_term &&
         a.entries == b.entries && a.leader_commit == b.leader_commit;
}

inline bool operator!=(const AppendEntriesRequest& a, const AppendEntriesRequest& b) {
  return !(a == b);
}

inline bool operator==(const AppendEntriesResponse& a, const AppendEntriesResponse& b) {
  return a.term == b.term && a.success == b.success && a.last_log_index == b.last_log_index;
}

inline bool operator!=(const AppendEntriesResponse& a, const AppendEntriesResponse& b) {
  return !(a == b);
}

}  // namespace dfly
