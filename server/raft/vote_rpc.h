// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/raft/raft_types.h"

namespace dfly {

struct VoteRequest {
  GroupId group_id = 0;
  Term term = 0;
  NodeId candidate_id;
  LogIndex last_log_index = 0;
  Term last_log_term = 0;
};

struct VoteResponse {
  GroupId group_id = 0;
  Term term = 0;
  bool vote_granted = false;
};

inline bool operator==(const VoteRequest& a, const VoteRequest& b) {
  return a.group_id == b.group_id && a.term == b.term &&
         a.candidate_id == b.candidate_id &&
         a.last_log_index == b.last_log_index && a.last_log_term == b.last_log_term;
}

inline bool operator!=(const VoteRequest& a, const VoteRequest& b) {
  return !(a == b);
}

inline bool operator==(const VoteResponse& a, const VoteResponse& b) {
  return a.group_id == b.group_id && a.term == b.term &&
         a.vote_granted == b.vote_granted;
}

inline bool operator!=(const VoteResponse& a, const VoteResponse& b) {
  return !(a == b);
}



}  // namespace dfly
