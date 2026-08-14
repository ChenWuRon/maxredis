// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>

#include "base/logging.h"
#include "server/raft/raft_types.h"

namespace dfly {

// Persistent apply progress tracker.
// Serializes last_applied to a JSON file (apply.meta) with atomic writes
// for crash safety. Used by RaftNode to track which log entries have been
// applied to the state machine.
class ApplyProgress {
 public:
  ApplyProgress() = default;

  // path: full path to the apply.meta file (e.g. "data/raft/apply.meta").
  explicit ApplyProgress(std::string path);

  // Reads last_applied from disk. Creates an empty file if none exists.
  // Returns true on success, false on I/O error.
  bool Load();

  // Writes current state to disk atomically (write + fsync + rename).
  // No-op if path is empty (in-memory only mode).
  bool Flush();

  LogIndex LastApplied() const {
    return last_applied_;
  }

  // Sets last_applied and flushes to disk.
  void Update(LogIndex index);

  // Sets last_applied_ WITHOUT touching disk. The caller defers the flush
  // (batched apply.meta policy) but the in-memory value stays current so
  // recovery logic always sees the true applied index within this process.
  void UpdateMemoryOnly(LogIndex index) {
    DCHECK_GE(index, last_applied_);
    last_applied_ = index;
  }

  // Sets last_applied to a LOWER value and flushes (allowed to move
  // backwards). Used at startup when no state-machine snapshot was restored:
  // the in-memory state is empty, so replay must start from the beginning
  // of the WAL instead of trusting a stale apply.meta.
  void Reset(LogIndex index);

 private:
  std::string Serialize() const;
  bool Deserialize(const std::string& data);

  std::string path_;
  LogIndex last_applied_ = 0;
};

}  // namespace dfly
