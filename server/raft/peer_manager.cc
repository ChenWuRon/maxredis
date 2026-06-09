#include "server/raft/peer_manager.h"

#include <algorithm>

namespace dfly {

void PeerManager::AddPeer(const NodeId& id) {
  if (std::find(peer_ids_.begin(), peer_ids_.end(), id) == peer_ids_.end()) {
    peer_ids_.push_back(id);
  }
}

bool PeerManager::RemovePeer(const NodeId& id) {
  auto it = std::find(peer_ids_.begin(), peer_ids_.end(), id);
  if (it == peer_ids_.end())
    return false;
  peer_ids_.erase(it);
  return true;
}

bool PeerManager::HasPeer(const NodeId& id) const {
  return std::find(peer_ids_.begin(), peer_ids_.end(), id) != peer_ids_.end();
}

size_t PeerManager::PeerCount() const {
  return peer_ids_.size();
}

size_t PeerManager::ClusterSize() const {
  return peer_ids_.size() + 1;
}

const std::vector<NodeId>& PeerManager::GetPeerIds() const {
  return peer_ids_;
}

}  // namespace dfly
