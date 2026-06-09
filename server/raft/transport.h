#pragma once

#include "server/raft/append_entries_rpc.h"
#include "server/raft/heartbeat_rpc.h"
#include "server/raft/install_snapshot_rpc.h"
#include "server/raft/raft_types.h"
#include "server/raft/read_index_rpc.h"
#include "server/raft/timeout_now_rpc.h"
#include "server/raft/vote_rpc.h"

namespace dfly {

class Transport {
 public:
  virtual ~Transport() = default;

  virtual VoteResponse SendVoteRequest(const NodeId& peer_id,
                                       const VoteRequest& request) = 0;

  virtual HeartbeatResponse SendHeartbeat(const NodeId& peer_id,
                                          const HeartbeatRequest& request) = 0;

  virtual AppendEntriesResponse SendAppendEntries(const NodeId& peer_id,
                                                   const AppendEntriesRequest& request) = 0;

  virtual InstallSnapshotResponse SendInstallSnapshot(const NodeId& peer_id,
                                                       const InstallSnapshotRequest& request) = 0;

  virtual ReadIndexResponse SendReadIndex(const NodeId& peer_id,
                                           const ReadIndexRequest& request) = 0;

  virtual TimeoutNowResponse SendTimeoutNow(const NodeId& peer_id,
                                             const TimeoutNowRequest& request) = 0;
};

}  // namespace dfly
