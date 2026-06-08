// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/state_machine.h"

#include <absl/time/clock.h>

#include "util/proactor_pool.h"

namespace dfly {

using namespace std;
using namespace util;

KvStateMachine::KvStateMachine(EngineShardSet* shard_set, ProactorPool* pp)
    : shard_set_(shard_set), pp_(pp) {
}

ShardId KvStateMachine::Shard(string_view key) const {
  return dfly::Shard(key, shard_set_->size());
}

void KvStateMachine::Set(DbIndex db_ind, std::string_view key, std::string_view val) {
  ShardId sid = Shard(key);
  shard_set_->Await(sid, [db_ind, key, val] {
    EngineShard* es = EngineShard::tlocal();
    auto [it, res] = es->db_slice.AddOrFind(db_ind, key);
    it->second.value = val;
  });
}

bool KvStateMachine::Del(DbIndex db_ind, std::string_view key) {
  ShardId sid = Shard(key);
  return shard_set_->Await(sid, [db_ind, key] {
    EngineShard* es = EngineShard::tlocal();
    return es->db_slice.Del(db_ind, key);
  });
}

bool KvStateMachine::Expire(DbIndex db_ind, std::string_view key, uint64_t expire_at_ms) {
  ShardId sid = Shard(key);
  return shard_set_->Await(sid, [db_ind, key, expire_at_ms] {
    EngineShard* es = EngineShard::tlocal();
    return es->db_slice.SetExpire(db_ind, key, expire_at_ms) == OpStatus::OK;
  });
}

OpResult<string> KvStateMachine::Get(DbIndex db_ind, std::string_view key) {
  ShardId sid = Shard(key);
  return shard_set_->Await(sid, [db_ind, key]() -> OpResult<string> {
    EngineShard* es = EngineShard::tlocal();
    OpResult<MainIterator> res = es->db_slice.Find(db_ind, key);
    if (res) {
      return res.value()->second.value;
    }
    return res.status();
  });
}

size_t KvStateMachine::DbSize(DbIndex db_ind) const {
  atomic_ulong total{0};
  shard_set_->RunBriefInParallel([db_ind, &total](EngineShard* es) {
    total += es->db_slice.DbSize(db_ind);
  });
  return total.load();
}

void KvStateMachine::Schedule(DbIndex db_ind, std::string_view key,
                               std::function<void(EngineShard*)> cb) {
  ShardId sid = Shard(key);
  shard_set_->Add(sid, [cb = std::move(cb)] {
    EngineShard* es = EngineShard::tlocal();
    cb(es);
  });
}

}  // namespace dfly
