// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_node.h"

#include "base/logging.h"

namespace dfly {

void RaftNode::BecomeFollower(Term term) {
  DCHECK_GE(term, term_);
  term_ = term;
  role_ = RaftRole::Follower;
}

void RaftNode::BecomeCandidate() {
  term_++;
  role_ = RaftRole::Candidate;
}

void RaftNode::BecomeLeader() {
  DCHECK_EQ(role_, RaftRole::Candidate);
  role_ = RaftRole::Leader;
}

}  // namespace dfly
