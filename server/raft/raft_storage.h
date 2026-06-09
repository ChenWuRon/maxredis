// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>

#include "server/raft/raft_types.h"

namespace dfly {

// Persistent Raft metadata storage (term, voted_for, snapshot metadata).
// Log entries are managed separately via ILogStorage.
class RaftStorage {
 public:
  RaftStorage() = default;

  Term current_term() const {
    return current_term_;
  }

  void set_current_term(Term term);

  const NodeId& voted_for() const {
    return voted_for_;
  }

  void set_voted_for(NodeId node_id);

  void Clear();

 private:
  Term current_term_ = 0;
  NodeId voted_for_;
};

}  // namespace dfly
