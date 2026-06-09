// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <ostream>
#include <string>
#include <unordered_set>
#include <vector>

namespace dfly {

enum class RaftRole : uint8_t {
  Follower = 0,
  Candidate = 1,
  Leader = 2,
};

inline std::ostream& operator<<(std::ostream& os, RaftRole role) {
  switch (role) {
    case RaftRole::Follower: return os << "Follower";
    case RaftRole::Candidate: return os << "Candidate";
    case RaftRole::Leader: return os << "Leader";
  }
  return os << "Unknown(" << static_cast<int>(role) << ")";
}

using Term = uint64_t;
using LogIndex = uint64_t;
using NodeId = std::string;

enum class LogEntryType : uint8_t {
  kCommand = 0,
  kConfig = 1,
};

struct LogEntry {
  Term term = 0;
  LogIndex index = 0;
  std::string command;

  LogEntry() = default;
  LogEntry(Term t, LogIndex i, std::string cmd)
      : term(t), index(i), command(std::move(cmd)) {
  }

  bool operator==(const LogEntry& o) const {
    return term == o.term && index == o.index && command == o.command;
  }
};

struct ClusterConfig {
  uint64_t version = 0;
  std::unordered_set<NodeId> voters;
  std::unordered_set<NodeId> learners;
};

enum class ConfigState : uint8_t {
  kStable = 0,
  kJoint = 1,
};

struct JointConfig {
  ClusterConfig old_config;
  ClusterConfig new_config;
};

struct ElectionResult {
  uint32_t votes_received = 0;
  uint32_t votes_rejected = 0;
};

// Snapshot anchor entry preserved after log compaction.
// Allows AppendEntries consistency checks to reference
// the last index/term covered by a snapshot.
struct SnapshotAnchor {
  LogIndex index = 0;
  Term term = 0;
};

}  // namespace dfly
