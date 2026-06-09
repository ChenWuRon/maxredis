#pragma once

#include <absl/container/flat_hash_map.h>

#include "server/raft/transport.h"

namespace dfly {

class RaftNode;

class LocalTransport : public Transport {
 public:
  void RegisterNode(const NodeId& id, RaftNode* node);

  VoteResponse SendVoteRequest(const NodeId& peer_id,
                               const VoteRequest& request) override;

  HeartbeatResponse SendHeartbeat(const NodeId& peer_id,
                                  const HeartbeatRequest& request) override;

  AppendEntriesResponse SendAppendEntries(const NodeId& peer_id,
                                           const AppendEntriesRequest& request) override;

  InstallSnapshotResponse SendInstallSnapshot(const NodeId& peer_id,
                                               const InstallSnapshotRequest& request) override;

  ReadIndexResponse SendReadIndex(const NodeId& peer_id,
                                   const ReadIndexRequest& request) override;

  TimeoutNowResponse SendTimeoutNow(const NodeId& peer_id,
                                     const TimeoutNowRequest& request) override;

  bool HasNode(const NodeId& id) const;

 private:
  absl::flat_hash_map<NodeId, RaftNode*> nodes_;
};

}  // namespace dfly
