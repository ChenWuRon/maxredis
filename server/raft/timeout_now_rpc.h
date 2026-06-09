#pragma once

#include "server/raft/raft_types.h"

namespace dfly {

struct TimeoutNowRequest {
  Term term = 0;
  NodeId leader_id;

  bool operator==(const TimeoutNowRequest& o) const {
    return term == o.term && leader_id == o.leader_id;
  }

  bool operator!=(const TimeoutNowRequest& o) const {
    return !(*this == o);
  }
};

struct TimeoutNowResponse {
  Term term = 0;
  bool accepted = false;

  bool operator==(const TimeoutNowResponse& o) const {
    return term == o.term && accepted == o.accepted;
  }

  bool operator!=(const TimeoutNowResponse& o) const {
    return !(*this == o);
  }
};

}  // namespace dfly
