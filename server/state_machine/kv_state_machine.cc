// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/state_machine/kv_state_machine.h"

#include <absl/strings/numbers.h>
#include <absl/time/clock.h>

#include "server/raft/raft_types.h"
#include "server/service/command_registry.h"
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

ApplyResult KvStateMachine::Apply(const CommandId* cid, CmdArgList args) {
  string_view name = cid->name();
  if (name == "SET") {
    Set(0, ArgS(args, 1), ArgS(args, 2));
    return {ApplyOp::OK, 1};
  }
  if (name == "DEL") {
    bool deleted = Del(0, ArgS(args, 1));
    return {ApplyOp::OK, deleted ? 1u : 0u};
  }
  return {ApplyOp::ERROR, 0};
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

ApplyResult KvStateMachine::ApplyLogEntry(const LogEntry& entry) {
  string_view cmd = entry.command;
  auto space1 = cmd.find(' ');
  if (space1 == string_view::npos)
    return {ApplyOp::ERROR, 0};

  string_view name = cmd.substr(0, space1);
  if (name == "SET") {
    auto space2 = cmd.find(' ', space1 + 1);
    if (space2 == string_view::npos)
      return {ApplyOp::ERROR, 0};
    string_view key = cmd.substr(space1 + 1, space2 - space1 - 1);
    string_view val = cmd.substr(space2 + 1);
    Set(0, key, val);
    return {ApplyOp::OK, 1};
  }
  if (name == "DEL") {
    bool deleted = Del(0, cmd.substr(space1 + 1));
    return {ApplyOp::OK, deleted ? 1u : 0u};
  }
  if (name == "EXPIRE") {
    auto space2 = cmd.find(' ', space1 + 1);
    if (space2 == string_view::npos)
      return {ApplyOp::ERROR, 0};
    string_view key = cmd.substr(space1 + 1, space2 - space1 - 1);
    string_view val = cmd.substr(space2 + 1);
    int64_t seconds;
    if (!absl::SimpleAtoi(val, &seconds) || seconds < 0)
      return {ApplyOp::ERROR, 0};
    uint64_t expire_at_ms = NowMs() + seconds * 1000;
    bool found = Expire(0, key, expire_at_ms);
    return {ApplyOp::OK, found ? 1u : 0u};
  }
  return {ApplyOp::ERROR, 0};
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
