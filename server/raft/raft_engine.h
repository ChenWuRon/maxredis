// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <string_view>

#include "server/raft/command_log.h"
#include "server/raft/raft_group.h"
#include "server/raft/replicated_command.h"
#include "server/state_machine/kv_state_machine.h"

namespace dfly {

class CommandId;

class RaftEngine {
 public:
  RaftEngine(EngineShardSet* shard_set, util::ProactorPool* pp);

  ApplyResult SubmitCommand(const CommandId* cid, CmdArgList args);

  bool Expire(DbIndex db_ind, std::string_view key, uint64_t expire_at_ms);
  OpResult<std::string> Get(DbIndex db_ind, std::string_view key);
  size_t DbSize(DbIndex db_ind) const;
  void Schedule(DbIndex db_ind, std::string_view key,
                std::function<void(EngineShard*)> cb);

  RaftGroup& group() {
    return group_;
  }

  const RaftGroup& group() const {
    return group_;
  }

  CommandLog& log() {
    return log_;
  }

  const CommandLog& log() const {
    return log_;
  }

 private:
  // Fast path for single-node: append log, advance commit index, apply.
  ApplyResult FastCommitPath(const ReplicatedCommand& cmd);

  KvStateMachine kv_;
  RaftGroup group_;
  CommandLog log_;
};

}  // namespace dfly
