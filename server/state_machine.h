// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <memory>
#include <string_view>

#include "server/common_types.h"
#include "server/engine_shard_set.h"
#include "server/op_status.h"

namespace util {
class ProactorPool;
}  // namespace util

namespace dfly {

class CommandId;

enum class ApplyOp : uint8_t {
  OK = 0,
  NOT_FOUND = 1,
  ERROR = 2,
};

struct ApplyResult {
  ApplyOp op = ApplyOp::OK;
  uint64_t affected_rows = 0;
};

class IStateMachine {
 public:
  virtual ~IStateMachine() = default;

  virtual ApplyResult Apply(const CommandId* cid, CmdArgList args) = 0;

  virtual void Set(DbIndex db_ind, std::string_view key, std::string_view val) = 0;
  virtual bool Del(DbIndex db_ind, std::string_view key) = 0;
  virtual bool Expire(DbIndex db_ind, std::string_view key, uint64_t expire_at_ms) = 0;
  virtual OpResult<std::string> Get(DbIndex db_ind, std::string_view key) = 0;
  virtual size_t DbSize(DbIndex db_ind) const = 0;
  virtual void Schedule(DbIndex db_ind, std::string_view key,
                         std::function<void(EngineShard*)> cb) = 0;
};

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
