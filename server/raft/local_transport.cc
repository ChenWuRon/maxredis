#include "server/raft/local_transport.h"

#include <absl/container/flat_hash_map.h>

#include "base/logging.h"
#include "server/raft/raft_node.h"

namespace dfly {

void LocalTransport::RegisterNode(GroupId group_id, const NodeId& id, RaftNode* node) {
  std::lock_guard<util::fb2::Mutex> guard(mutex_);
  nodes_[{group_id, id}] = node;
}

void LocalTransport::UnregisterNode(GroupId group_id, const NodeId& node_id) {
  std::lock_guard<util::fb2::Mutex> guard(mutex_);
  nodes_.erase({group_id, node_id});
}

bool LocalTransport::HasNode(GroupId group_id, const NodeId& id) const {
  std::lock_guard<util::fb2::Mutex> guard(mutex_);
  return nodes_.contains({group_id, id});
}

RaftNode* LocalTransport::LookupAcquire(GroupId group_id, const NodeId& node_id) {
  std::lock_guard<util::fb2::Mutex> guard(mutex_);
  auto it = nodes_.find({group_id, node_id});
  if (it == nodes_.end())
    return nullptr;
  // Acquiring the RPC ref under the registry lock pairs with the
  // rpc_alive_/UnregisterNode ordering in RaftNode::Shutdown(): either the
  // ref is acquired before the node flips to shutting-down (the handler is
  // then safe to run — it observes shutdown_ and returns promptly), or the
  // acquire fails and the RPC is rejected.
  if (!it->second->TryAcquireRpcRef())
    return nullptr;
  return it->second;
}

VoteResponse LocalTransport::SendVoteRequest(const NodeId& peer_id,
                                              const VoteRequest& request) {
  RaftNode* node = LookupAcquire(request.group_id, peer_id);
  if (!node)
    return {request.group_id, 0, false};
  VoteResponse rsp = node->OnRequestVote(request);
  node->ReleaseRpcRef();
  return rsp;
}

HeartbeatResponse LocalTransport::SendHeartbeat(const NodeId& peer_id,
                                                 const HeartbeatRequest& request) {
  RaftNode* node = LookupAcquire(request.group_id, peer_id);
  if (!node)
    return {request.group_id, 0, false};
  HeartbeatResponse rsp = node->OnHeartbeat(request);
  node->ReleaseRpcRef();
  return rsp;
}

AppendEntriesResponse LocalTransport::SendAppendEntries(const NodeId& peer_id,
                                                         const AppendEntriesRequest& request) {
  RaftNode* node = LookupAcquire(request.group_id, peer_id);
  if (!node)
    return {request.group_id, 0, false, 0};
  AppendEntriesResponse rsp = node->OnAppendEntries(request);
  node->ReleaseRpcRef();
  return rsp;
}

InstallSnapshotResponse LocalTransport::SendInstallSnapshot(const NodeId& peer_id,
                                                             const InstallSnapshotRequest& request) {
  RaftNode* node = LookupAcquire(request.group_id, peer_id);
  if (!node)
    return {request.group_id, 0, false};
  InstallSnapshotResponse rsp = node->OnInstallSnapshot(request);
  node->ReleaseRpcRef();
  return rsp;
}

ReadIndexResponse LocalTransport::SendReadIndex(const NodeId& peer_id,
                                                  const ReadIndexRequest& request) {
  RaftNode* node = LookupAcquire(request.group_id, peer_id);
  if (!node)
    return {request.group_id, 0, false, 0};
  ReadIndexResponse rsp = node->OnReadIndex(request);
  node->ReleaseRpcRef();
  return rsp;
}

TimeoutNowResponse LocalTransport::SendTimeoutNow(const NodeId& peer_id,
                                                   const TimeoutNowRequest& request) {
  RaftNode* node = LookupAcquire(request.group_id, peer_id);
  if (!node)
    return {request.group_id, 0, false};
  TimeoutNowResponse rsp = node->OnTimeoutNow(request);
  node->ReleaseRpcRef();
  return rsp;
}

}  // namespace dfly
