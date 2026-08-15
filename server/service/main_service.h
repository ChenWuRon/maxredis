// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cstdint>

#include "base/varz_value.h"
#include "server/service/command_registry.h"
#include "server/storage/engine_shard_set.h"
#include "server/raft/raft_engine.h"
#include "server/service/snapshot_fiber.h"
#include "util/http/http_handler.h"
#include "server/protocol/memcache_parser.h"

namespace util {
class AcceptServer;
}  // namespace util

namespace dfly {

class PersistenceManager;
class TcpTransport;

class Service {
  friend class SnapshotFiber;

 public:
  using error_code = std::error_code;

  explicit Service(util::ProactorPool* pp);
  ~Service();

  void RegisterHttp(util::HttpListenerBase* listener);

  void Init(util::AcceptServer* acceptor);

  void Shutdown();

  void DispatchCommand(CmdArgList args, ConnectionContext* cntx);
  void DispatchMC(const MemcacheParser::Command& cmd, std::string_view value,
                  ConnectionContext* cntx);

  uint32_t shard_count() const {
    return shard_set_.size();
  }

  EngineShardSet& shard_set() {
    return shard_set_;
  }

  util::ProactorPool& proactor_pool() {
    return pp_;
  }

 private:
  void Ping(CmdArgList args, ConnectionContext* cntx);
  void Set(CmdArgList args, ConnectionContext* cntx);
  void Get(CmdArgList args, ConnectionContext* cntx);
  void Del(CmdArgList args, ConnectionContext* cntx);
  void Expire(CmdArgList args, ConnectionContext* cntx);
  void Debug(CmdArgList args, ConnectionContext* cntx);
  void Info(CmdArgList args, ConnectionContext* cntx);
  void Save(CmdArgList args, ConnectionContext* cntx);

  bool CreateSnapshot();
  void LoadSnapshot();

  void RegisterCommands();

  // Multi-node mode: parses --raft_peers, wires the TCP transport into the
  // Raft node, adds peers to the cluster config, and registers the Raft RPC
  // listener with the accept server. No-op in single-node mode.
  bool InitRaftCluster(util::AcceptServer* acceptor);

  base::VarzValue::Map GetVarzStats();

  void ReplayAof();

  CommandRegistry registry_;
  EngineShardSet shard_set_;
  util::ProactorPool& pp_;
  PersistenceManager* persistence_manager_ = nullptr;
  bool replay_mode_ = false;
  SnapshotFiber snapshot_fiber_{this};

  // Multi-node mode: TCP transport (the RPC listener is owned by the
  // AcceptServer). Declared BEFORE engine_ so it is destroyed AFTER the
  // Raft node (reverse declaration order) — the node's fibers reference the
  // transport until Shutdown joins them.
  std::unique_ptr<TcpTransport> raft_transport_;

  RaftEngine engine_;

  // Real INFO statistics (atomic: written from connection fibers on any
  // proactor thread, read by the INFO handler).
  std::atomic<uint64_t> commands_processed_{0};
  std::atomic<uint64_t> keyspace_hits_{0};
  std::atomic<uint64_t> keyspace_misses_{0};
};

}  // namespace dfly
