#pragma once

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <string>
#include <utility>

#include "server/raft/transport.h"

namespace dfly {

class RaftNode;

// Composite key for group-aware node registration.
struct GroupNodeKey {
  GroupId group_id;
  NodeId node_id;

  bool operator==(const GroupNodeKey& o) const {
    return group_id == o.group_id && node_id == o.node_id;
  }
};

struct GroupNodeKeyHash {
  size_t operator()(const GroupNodeKey& k) const {
    return std::hash<GroupId>()(k.group_id) ^ (std::hash<NodeId>()(k.node_id) << 1);
  }
};

class LocalTransport : public Transport {
 public:
  // Registers a RaftNode for the given group and node id.
  // Messages for (group_id, node_id) pair will be routed to |node|.
  void RegisterNode(GroupId group_id, const NodeId& id, RaftNode* node);

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

  bool HasNode(GroupId group_id, const NodeId& id) const;

 private:
  RaftNode* Lookup(GroupId group_id, const NodeId& node_id) const;

  absl::flat_hash_map<GroupNodeKey, RaftNode*, GroupNodeKeyHash> nodes_;
};

}  // namespace dfly
