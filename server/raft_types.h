// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dfly {

enum class RaftRole : uint8_t {
  Follower = 0,
  Candidate = 1,
  Leader = 2,
};

using Term = uint64_t;
using LogIndex = uint64_t;
using NodeId = std::string;

struct LogEntry {
  Term term = 0;
  LogIndex index = 0;
  std::string command;

  LogEntry() = default;
  LogEntry(Term t, LogIndex i, std::string cmd)
      : term(t), index(i), command(std::move(cmd)) {
  }
};

}  // namespace dfly
