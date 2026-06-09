// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/raft/snapshot_barrier.h"
#include "server/state_machine/state_machine.h"
#include "server/storage/engine_shard_set.h"
#include "server/storage/op_status.h"

namespace util {
class ProactorPool;
}  // namespace util

namespace dfly {

class KvStateMachine : public IStateMachine {
 public:
  KvStateMachine(EngineShardSet* shard_set, util::ProactorPool* pp);

  ApplyResult Apply(const CommandId* cid, CmdArgList args) override;
  ApplyResult ApplyLogEntry(const LogEntry& entry) override;

  void Set(DbIndex db_ind, std::string_view key, std::string_view val) override;
  bool Del(DbIndex db_ind, std::string_view key) override;
  bool Expire(DbIndex db_ind, std::string_view key, uint64_t expire_at_ms) override;
  OpResult<std::string> Get(DbIndex db_ind, std::string_view key,
                             ReadConsistency consistency = ReadConsistency::kLocal) override;
  size_t DbSize(DbIndex db_ind) const override;
  void Schedule(DbIndex db_ind, std::string_view key,
                std::function<void(EngineShard*)> cb) override;

  bool SaveSnapshot(const std::string& path) override;
  bool LoadSnapshot(const std::string& path) override;

  // Sets the snapshot barrier for write-freeze during snapshot.
  // May be nullptr (no barrier).
  void SetSnapshotBarrier(SnapshotBarrier* barrier) {
    barrier_ = barrier;
  }

 private:
  EngineShardSet* shard_set_;
  util::ProactorPool* pp_;
  SnapshotBarrier* barrier_ = nullptr;

  ShardId Shard(std::string_view key) const;
};

}  // namespace dfly
