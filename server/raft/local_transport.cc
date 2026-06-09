#include "server/raft/local_transport.h"

#include <absl/container/flat_hash_map.h>

#include "base/logging.h"
#include "server/raft/raft_node.h"

namespace dfly {

void LocalTransport::RegisterNode(const NodeId& id, RaftNode* node) {
  nodes_[id] = node;
}

bool LocalTransport::HasNode(const NodeId& id) const {
  return nodes_.contains(id);
}

VoteResponse LocalTransport::SendVoteRequest(const NodeId& peer_id,
                                              const VoteRequest& request) {
  auto it = nodes_.find(peer_id);
  DCHECK(it != nodes_.end()) << "Unknown peer: " << peer_id;
  return it->second->OnRequestVote(request);
}

HeartbeatResponse LocalTransport::SendHeartbeat(const NodeId& peer_id,
                                                 const HeartbeatRequest& request) {
  auto it = nodes_.find(peer_id);
  DCHECK(it != nodes_.end()) << "Unknown peer: " << peer_id;
  return it->second->OnHeartbeat(request);
}

AppendEntriesResponse LocalTransport::SendAppendEntries(const NodeId& peer_id,
                                                         const AppendEntriesRequest& request) {
  auto it = nodes_.find(peer_id);
  DCHECK(it != nodes_.end()) << "Unknown peer: " << peer_id;
  return it->second->OnAppendEntries(request);
}

InstallSnapshotResponse LocalTransport::SendInstallSnapshot(const NodeId& peer_id,
                                                             const InstallSnapshotRequest& request) {
  auto it = nodes_.find(peer_id);
  DCHECK(it != nodes_.end()) << "Unknown peer: " << peer_id;
  return it->second->OnInstallSnapshot(request);
}

ReadIndexResponse LocalTransport::SendReadIndex(const NodeId& peer_id,
                                                  const ReadIndexRequest& request) {
  auto it = nodes_.find(peer_id);
  DCHECK(it != nodes_.end()) << "Unknown peer: " << peer_id;
  return it->second->OnReadIndex(request);
}

TimeoutNowResponse LocalTransport::SendTimeoutNow(const NodeId& peer_id,
                                                   const TimeoutNowRequest& request) {
  auto it = nodes_.find(peer_id);
  DCHECK(it != nodes_.end()) << "Unknown peer: " << peer_id;
  return it->second->OnTimeoutNow(request);
}

}  // namespace dfly
