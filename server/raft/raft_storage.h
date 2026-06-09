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

  // Resets both fields to zero/empty.
  void Clear();

 private:
  std::string Serialize() const;
  bool Deserialize(const std::string& data);

  static std::string EscapeJson(const std::string& raw);
  static std::string UnescapeJson(const std::string& escaped);

  std::string path_;
  Term current_term_ = 0;
  NodeId voted_for_;
};

}  // namespace dfly
