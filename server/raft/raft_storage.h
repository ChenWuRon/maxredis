// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>

#include "server/raft/raft_types.h"

namespace dfly {

// Persistent Raft metadata storage for current_term and voted_for.
// Serializes to a JSON file at the configured path with atomic writes
// (write + fsync + rename) for crash safety.
//
// CRASH-SAFETY (Election Safety):
// term and voted_for are written as a SINGLE atomic record via SetState().
// The legacy two-write sequence (term fsync, then voted_for fsync) had a
// crash window in which a new term was durable but the vote was not, which
// could lead to two votes being granted for the same term after recovery.
//
// The joint-consensus config state is persisted alongside (less frequent,
// only on config change STEP transitions) so a node restarted mid-change
// resumes the joint configuration instead of silently reverting to stable.
class RaftStorage {
 public:
  RaftStorage() = default;

  // path: full path to the meta.json file (e.g. "data/raft/meta.json").
  // An empty path disables persistence (in-memory only).
  explicit RaftStorage(std::string path);

  // Reads state from disk. Creates an empty file if none exists.
  // Returns true on success, false on I/O error.
  bool Load();

  // Writes current state to disk atomically.
  // No-op if path is empty (in-memory only mode).
  bool Flush();

  Term current_term() const {
    return current_term_;
  }

  // Sets current_term and flushes to disk.
  void set_current_term(Term term);

  const NodeId& voted_for() const {
    return voted_for_;
  }

  // Sets voted_for and flushes to disk.
  void set_voted_for(NodeId node_id);

  // Atomically persists (current_term, voted_for) in a SINGLE fsync.
  // This is the only correct way to persist a new term together with a vote
  // decision (election, becoming candidate, stepping down on a new term).
  void SetState(Term term, const NodeId& voted_for);

  // Persists the consensus config state (stable/joint) and the joint config.
  // Called on config-change STEP transitions; allows resuming a joint
  // membership change across restarts.
  void SetJointConfigState(ConfigState state, const JointConfig& joint);

  ConfigState config_state() const {
    return config_state_;
  }

  const JointConfig& joint_config() const {
    return joint_config_;
  }

  // Resets both fields to zero/empty.
  void Clear();

 private:
  std::string Serialize() const;
  bool Deserialize(const std::string& data);

  static std::string EscapeJson(const std::string& raw);
  static std::string UnescapeJson(const std::string& escaped);
  static std::string JoinToken(const std::unordered_set<NodeId>& set);

  std::string path_;
  Term current_term_ = 0;
  NodeId voted_for_;
  ConfigState config_state_ = ConfigState::kStable;
  JointConfig joint_config_;
};

}  // namespace dfly