// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <vector>

#include "server/raft/raft_types.h"
#include "server/raft/vote_rpc.h"

namespace dfly {

class RaftNode {
 public:
  explicit RaftNode(NodeId node_id = "");

  const NodeId& node_id() const {
    return node_id_;
  }

  RaftRole role() const {
    return role_;
  }

  Term term() const {
    return term_;
  }

  const NodeId& voted_for() const {
    return voted_for_;
  }

  uint32_t vote_count() const {
    return vote_count_;
  }

  void AddPeer(RaftNode* peer);

  void BecomeFollower(Term term);
  void BecomeCandidate();
  void BecomeLeader();

  // Called when the election timer fires.
  // Transitions Follower → Candidate if still in Follower state.
  void OnElectionTimeout();

  // Processes an incoming VoteRequest according to Raft rules.
  VoteResponse OnRequestVote(const VoteRequest& request);

  // Transitions to Candidate, sends VoteRequest to all peers,
  // collects responses, and returns the tally.
  ElectionResult StartElection();

  // Checks if votes_received >= majority (N/2+1).
  // If so, calls BecomeLeader() and returns true.
  bool TryBecomeLeader(const ElectionResult& result);

  Term leader_term() const {
    return leader_term_;
  }

 private:
  NodeId node_id_;
  RaftRole role_ = RaftRole::Follower;
  Term term_ = 0;
  Term leader_term_ = 0;
  NodeId voted_for_;
  uint32_t vote_count_ = 0;
  std::vector<RaftNode*> peers_;
};

}  // namespace dfly
