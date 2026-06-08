// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft_engine.h"

namespace dfly {

RaftEngine::RaftEngine(EngineShardSet* shard_set, util::ProactorPool* pp)
    : kv_(shard_set, pp), group_(0) {
}

void RaftEngine::Set(DbIndex db_ind, std::string_view key, std::string_view val) {
  kv_.Set(db_ind, key, val);
}

bool RaftEngine::Del(DbIndex db_ind, std::string_view key) {
  return kv_.Del(db_ind, key);
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
