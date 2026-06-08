// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

#include "server/raft_node.h"
#include "server/raft_types.h"

namespace dfly {

using GroupId = uint32_t;

class RaftGroup {
 public:
  explicit RaftGroup(GroupId group_id) : group_id_(group_id) {
  }

  GroupId group_id() const {
    return group_id_;
  }

  RaftNode& node() {
    return node_;
  }

  const RaftNode& node() const {
    return node_;
  }

 private:
  GroupId group_id_;
  RaftNode node_;
};

}  // namespace dfly
