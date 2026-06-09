#include "server/raft/local_transport.h"

#include <absl/container/flat_hash_map.h>

#include "base/logging.h"
#include "server/raft/raft_node.h"

namespace dfly {

void LocalTransport::RegisterNode(GroupId group_id, const NodeId& id, RaftNode* node) {
  nodes_[{group_id, id}] = node;
}

bool LocalTransport::HasNode(GroupId group_id, const NodeId& id) const {
  return nodes_.contains({group_id, id});
}

RaftNode* LocalTransport::Lookup(GroupId group_id, const NodeId& node_id) const {
  auto it = nodes_.find({group_id, node_id});
  DCHECK(it != nodes_.end()) << "Unknown peer: group=" << group_id << " node=" << node_id;
  return it != nodes_.end() ? it->second : nullptr;
}

VoteResponse LocalTransport::SendVoteRequest(const NodeId& peer_id,
                                              const VoteRequest& request) {
  RaftNode* node = Lookup(request.group_id, peer_id);
  if (!node)
    return {request.group_id, 0, false};
  return node->OnRequestVote(request);
}

HeartbeatResponse LocalTransport::SendHeartbeat(const NodeId& peer_id,
                                                 const HeartbeatRequest& request) {
  RaftNode* node = Lookup(request.group_id, peer_id);
  if (!node)
    return {request.group_id, 0, false};
  return node->OnHeartbeat(request);
}

AppendEntriesResponse LocalTransport::SendAppendEntries(const NodeId& peer_id,
                                                         const AppendEntriesRequest& request) {
  RaftNode* node = Lookup(request.group_id, peer_id);
  if (!node)
    return {request.group_id, 0, false, 0};
  return node->OnAppendEntries(request);
}

InstallSnapshotResponse LocalTransport::SendInstallSnapshot(const NodeId& peer_id,
                                                             const InstallSnapshotRequest& request) {
  RaftNode* node = Lookup(request.group_id, peer_id);
  if (!node)
    return {request.group_id, 0, false};
  return node->OnInstallSnapshot(request);
}

ReadIndexResponse LocalTransport::SendReadIndex(const NodeId& peer_id,
                                                  const ReadIndexRequest& request) {
  RaftNode* node = Lookup(request.group_id, peer_id);
  if (!node)
    return {request.group_id, 0, false, 0};
  return node->OnReadIndex(request);
}

TimeoutNowResponse LocalTransport::SendTimeoutNow(const NodeId& peer_id,
                                                   const TimeoutNowRequest& request) {
  RaftNode* node = Lookup(request.group_id, peer_id);
  if (!node)
    return {request.group_id, 0, false};
  return node->OnTimeoutNow(request);
}

}  // namespace dfly
