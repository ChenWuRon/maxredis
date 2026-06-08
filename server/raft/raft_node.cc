// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_node.h"

#include "base/logging.h"

namespace dfly {

RaftNode::RaftNode(NodeId node_id) : node_id_(std::move(node_id)) {
}

void RaftNode::BecomeFollower(Term term) {
  DCHECK_GE(term, term_);
  term_ = term;
  role_ = RaftRole::Follower;
  voted_for_.clear();
  vote_count_ = 0;
}

void RaftNode::OnElectionTimeout() {
  if (role_ != RaftRole::Follower)
    return;
  BecomeCandidate();
}

void RaftNode::BecomeCandidate() {
  term_++;
  role_ = RaftRole::Candidate;
  voted_for_ = node_id_;
  vote_count_ = 1;
}

void RaftNode::BecomeLeader() {
  DCHECK_EQ(role_, RaftRole::Candidate);
  role_ = RaftRole::Leader;
}

VoteResponse RaftNode::OnRequestVote(const VoteRequest& request) {
  // Rule 1: Stale term — reject.
  if (request.term < term_) {
    return {term_, false};
  }

  // Rule 2: Higher term — update local state and become follower.
  if (request.term > term_) {
    BecomeFollower(request.term);
  }

  // After rule 2, term_ == request.term.

  // Rule 3: Already voted for another candidate in this term.
  if (!voted_for_.empty() && voted_for_ != request.candidate_id) {
    return {term_, false};
  }

  // Rule 4: Grant vote.
  voted_for_ = request.candidate_id;
  return {term_, true};
}

}  // namespace dfly
