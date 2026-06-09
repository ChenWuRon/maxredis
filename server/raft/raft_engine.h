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
  OpResult<std::string> Get(DbIndex db_ind, std::string_view key,
                             ReadConsistency consistency = ReadConsistency::kLocal);
  size_t DbSize(DbIndex db_ind) const;
  LogIndex ReadIndex();
  void Schedule(DbIndex db_ind, std::string_view key,
                std::function<void(EngineShard*)> cb);

  RaftGroup& group() {
    return group_;
  }

  const RaftGroup& group() const {
    return group_;
  }

  // Delegates to RaftGroup's owned log storage.
  ILogStorage* log_storage() {
    return group_.log_storage();
  }

  const ILogStorage* log_storage() const {
    return group_.log_storage();
  }

  // For backward compatibility with tests that use engine.log().
  // Returns the underlying CommandLog if available, or null.
  CommandLog* log() {
    return static_cast<CommandLog*>(group_.log_storage());
  }

  const CommandLog* log() const {
    return static_cast<const CommandLog*>(group_.log_storage());
  }

  KvStateMachine& kv() {
    return kv_;
  }

  const KvStateMachine& kv() const {
    return kv_;
  }

 private:
  // Fast path for single-node: append log, advance commit index, apply.
  ApplyResult FastCommitPath(const ReplicatedCommand& cmd);

  KvStateMachine kv_;
  RaftGroup group_;
};

}  // namespace dfly
