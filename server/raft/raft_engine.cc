// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_engine.h"

#include "base/logging.h"
#include "server/raft/command_encoder.h"
#include "server/raft/raft_types.h"
#include "server/raft/raft_node.h"
#include "server/service/command_registry.h"

namespace dfly {

RaftEngine::RaftEngine(EngineShardSet* shard_set, util::ProactorPool* pp)
    : kv_(shard_set, pp), group_(0) {
  group_.node().set_group_id(0);
  group_.node().SetStateMachine(&kv_);
}

bool RaftEngine::InitRaftStorage(const std::string& base_path, uint32_t fsync_interval_ms) {
  if (!group_.InitStorage(base_path, fsync_interval_ms))
    return false;
  // Wire the snapshot barrier into the KV state machine so that Raft
  // snapshot export freezes concurrent writes across ALL shard threads.
  kv_.SetSnapshotBarrier(&group_.snapshot_manager()->barrier());
  return true;
}

ApplyResult RaftEngine::SubmitCommand(const CommandId* cid, CmdArgList args) {
  auto cmd = CommandEncoder::Encode(cid, args);

  if (!cmd) {
    return kv_.Apply(cid, args);
  }

  VLOG(1) << "SubmitCommand: " << cmd->Serialize();

  // All appends + replication go through the consensus-locked node path.
  LogEntry entry(0, 0, cmd->Serialize());
  return group_.node().SubmitEntry(std::move(entry));
}

bool RaftEngine::Expire(DbIndex db_ind, std::string_view key, uint64_t expire_at_ms) {
  return kv_.Expire(db_ind, key, expire_at_ms);
}

OpResult<std::string> RaftEngine::Get(DbIndex db_ind, std::string_view key,
                                      ReadConsistency consistency) {
  if (consistency == ReadConsistency::kLinearizable) {
    LogIndex ri = ReadIndex();
    if (ri == 0) {
      return OpStatus::KEY_NOTFOUND;
    }
  }
  return kv_.Get(db_ind, key);
}

LogIndex RaftEngine::ReadIndex() {
  return group_.node().ReadIndex();
}

size_t RaftEngine::DbSize(DbIndex db_ind) const {
  return kv_.DbSize(db_ind);
}

void RaftEngine::Schedule(DbIndex db_ind, std::string_view key,
                            std::function<void(EngineShard*)> cb) {
  kv_.Schedule(db_ind, key, std::move(cb));
}

}  // namespace dfly
