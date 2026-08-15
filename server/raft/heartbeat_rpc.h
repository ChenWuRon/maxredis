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
  // The leader's commit index. Raft heartbeats are empty AppendEntries and
  // MUST carry the commit index, otherwise followers that have already
  // replicated an entry never learn it is committed (their commit_index
  // stalls until the next AppendEntries). Mirror of etcd MsgHeartbeat.
  Term leader_commit = 0;
};

struct HeartbeatResponse {
  GroupId group_id = 0;
  Term term = 0;
  bool success = false;
  // The follower's last log index. Lets the leader (a) detect a follower
  // that missed entries while partitioned/unavailable and push them via
  // AppendEntries, and (b) update match progress so commit can advance
  // between writes (etcd MsgHeartbeatResp semantic).
  LogIndex last_log_index = 0;
};

inline bool operator==(const HeartbeatRequest& a, const HeartbeatRequest& b) {
  return a.group_id == b.group_id && a.term == b.term &&
         a.leader_id == b.leader_id && a.leader_commit == b.leader_commit;
}

inline bool operator!=(const HeartbeatRequest& a, const HeartbeatRequest& b) {
  return !(a == b);
}

inline bool operator==(const HeartbeatResponse& a, const HeartbeatResponse& b) {
  return a.group_id == b.group_id && a.term == b.term &&
         a.success == b.success && a.last_log_index == b.last_log_index;
}

inline bool operator!=(const HeartbeatResponse& a, const HeartbeatResponse& b) {
  return !(a == b);
}

}  // namespace dfly
