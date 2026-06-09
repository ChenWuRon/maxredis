// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <string>

#include "server/raft/raft_types.h"

namespace dfly {

class Transport;

// Leader-side snapshot sender. Reads a snapshot.bin file and sends it to a
// follower in chunks via InstallSnapshot RPC.
class SnapshotSender {
 public:
  static constexpr size_t kChunkSize = 65536;  // 64KB

  SnapshotSender(std::string snapshot_path, Transport* transport);

  // Sends the entire snapshot to |follower| in kChunkSize chunks.
  // Each chunk carries the given term/leader_id/last_included_index/last_included_term.
  // Returns true if all chunks were sent successfully.
  bool SendSnapshot(const NodeId& follower, Term term, const NodeId& leader_id,
                    LogIndex last_included_index, Term last_included_term);

 private:
  std::string snapshot_path_;
  Transport* transport_;
};

}  // namespace dfly
