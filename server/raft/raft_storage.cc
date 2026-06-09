// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_storage.h"

#include <absl/strings/str_cat.h>

#include <stdexcept>

#include "base/logging.h"

namespace dfly {

void RaftStorage::set_current_term(Term term) {
  DCHECK_GE(term, current_term_);
  current_term_ = term;
}

void RaftStorage::set_voted_for(NodeId node_id) {
  voted_for_ = std::move(node_id);
}

void RaftStorage::Clear() {
  current_term_ = 0;
  voted_for_.clear();
}

}  // namespace dfly
