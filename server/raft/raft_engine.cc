// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_engine.h"

#include "server/service/command_registry.h"

namespace dfly {

RaftEngine::RaftEngine(EngineShardSet* shard_set, util::ProactorPool* pp)
    : kv_(shard_set, pp), group_(0) {
  group_.node().SetLogStorage(&log_);
  group_.node().SetStateMachine(&kv_);
}

ApplyResult RaftEngine::SubmitCommand(const CommandId* cid, CmdArgList args) {
  return kv_.Apply(cid, args);
}

bool RaftEngine::Expire(DbIndex db_ind, std::string_view key, uint64_t expire_at_ms) {
  return kv_.Expire(db_ind, key, expire_at_ms);
}

OpResult<std::string> RaftEngine::Get(DbIndex db_ind, std::string_view key) {
  return kv_.Get(db_ind, key);
}

size_t RaftEngine::DbSize(DbIndex db_ind) const {
  return kv_.DbSize(db_ind);
}

void RaftEngine::Schedule(DbIndex db_ind, std::string_view key,
                            std::function<void(EngineShard*)> cb) {
  kv_.Schedule(db_ind, key, std::move(cb));
}

}  // namespace dfly
