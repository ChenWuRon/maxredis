#include "server/raft/peer_manager.h"

namespace dfly {

PeerManager::PeerManager(ClusterConfig* config) : config_(config) {
}

void PeerManager::SetConfig(ClusterConfig* config) {
  config_ = config;
}

void PeerManager::AddPeer(const NodeId& id) {
  if (config_)
    config_->voters.insert(id);
}

bool PeerManager::RemovePeer(const NodeId& id) {
  if (!config_)
    return false;
  return config_->voters.erase(id) > 0;
}

bool PeerManager::HasPeer(const NodeId& id) const {
  return config_ && config_->voters.count(id) > 0;
}

size_t PeerManager::PeerCount() const {
  return config_ ? config_->voters.size() : 0;
}

size_t PeerManager::ClusterSize() const {
  return PeerCount() + 1;
}

std::vector<NodeId> PeerManager::GetPeerIds() const {
  if (!config_)
    return {};
  return {config_->voters.begin(), config_->voters.end()};
}

}  // namespace dfly