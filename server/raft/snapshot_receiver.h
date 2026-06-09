// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>

#include "server/raft/raft_types.h"

namespace dfly {

struct InstallSnapshotRequest;
struct InstallSnapshotResponse;

// Follower-side snapshot receiver. Receives InstallSnapshot RPC chunks,
// writes them to snapshot.recv.tmp, and renames to snapshot.bin on completion.
// Crash-safe: stale snapshot.recv.tmp is removed on Init().
class SnapshotReceiver {
 public:
  explicit SnapshotReceiver(std::string dir);

  // Cleans up any stale snapshot.recv.tmp from a previous crash.
  void Init();

  const std::string& bin_path() const {
    return bin_path_;
  }

  // Processes a single InstallSnapshot chunk.
  // Writes data to snapshot.recv.tmp at the given offset.
  // When request.done is true, fsyncs and renames to snapshot.bin.
  InstallSnapshotResponse HandleChunk(const InstallSnapshotRequest& request);

 private:
  std::string dir_;
  std::string tmp_path_;
  std::string bin_path_;
};

}  // namespace dfly
