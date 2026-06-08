// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

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

  void Set(DbIndex db_ind, std::string_view key, std::string_view val) override;
  bool Del(DbIndex db_ind, std::string_view key) override;
  bool Expire(DbIndex db_ind, std::string_view key, uint64_t expire_at_ms) override;
  OpResult<std::string> Get(DbIndex db_ind, std::string_view key) override;
  size_t DbSize(DbIndex db_ind) const override;
  void Schedule(DbIndex db_ind, std::string_view key,
                std::function<void(EngineShard*)> cb) override;

 private:
  EngineShardSet* shard_set_;
  util::ProactorPool* pp_;

  ShardId Shard(std::string_view key) const;
};

}  // namespace dfly
