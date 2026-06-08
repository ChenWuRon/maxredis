// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/raft/raft_types.h"

namespace dfly {

class RaftNode {
 public:
  RaftRole role() const {
    return role_;
  }

  Term term() const {
    return term_;
  }

  void BecomeFollower(Term term);
  void BecomeCandidate();
  void BecomeLeader();

 private:
  RaftRole role_ = RaftRole::Follower;
  Term term_ = 0;
};

}  // namespace dfly
