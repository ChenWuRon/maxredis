#pragma once

#include "server/raft/raft_types.h"

namespace dfly {

struct ReadIndexRequest {
  GroupId group_id = 0;
  Term term = 0;
  NodeId leader_id;
  uint64_t request_id = 0;

  bool operator==(const ReadIndexRequest& o) const {
    return group_id == o.group_id && term == o.term &&
           leader_id == o.leader_id && request_id == o.request_id;
  }

  bool operator!=(const ReadIndexRequest& o) const {
    return !(*this == o);
  }
};

struct ReadIndexResponse {
  GroupId group_id = 0;
  Term term = 0;
  bool success = false;
  LogIndex commit_index = 0;

  bool operator==(const ReadIndexResponse& o) const {
    return group_id == o.group_id && term == o.term &&
           success == o.success && commit_index == o.commit_index;
  }

  bool operator!=(const ReadIndexResponse& o) const {
    return !(*this == o);
  }
};

}  // namespace dfly
