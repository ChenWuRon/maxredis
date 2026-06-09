#pragma once

#include <string>
#include <vector>

#include "server/raft/raft_types.h"

namespace dfly {

class PeerManager {
 public:
  explicit PeerManager(ClusterConfig* config = nullptr);

  void SetConfig(ClusterConfig* config);

  void AddPeer(const NodeId& id);
  bool RemovePeer(const NodeId& id);
  bool HasPeer(const NodeId& id) const;
  size_t PeerCount() const;
  size_t ClusterSize() const;

  std::vector<NodeId> GetPeerIds() const;

 private:
  ClusterConfig* config_ = nullptr;
};

}  // namespace dfly
