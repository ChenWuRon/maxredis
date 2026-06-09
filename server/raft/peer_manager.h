#pragma once

#include <string>
#include <vector>

#include "server/raft/raft_types.h"

namespace dfly {

class PeerManager {
 public:
  void AddPeer(const NodeId& id);
  bool RemovePeer(const NodeId& id);
  bool HasPeer(const NodeId& id) const;
  size_t PeerCount() const;
  size_t ClusterSize() const;

  const std::vector<NodeId>& GetPeerIds() const;

 private:
  std::vector<NodeId> peer_ids_;
};

}  // namespace dfly
