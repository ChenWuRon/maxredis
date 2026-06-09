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
  group_.node().SetLogStorage(&log_);
  group_.node().SetStateMachine(&kv_);
}

ApplyResult RaftEngine::SubmitCommand(const CommandId* cid, CmdArgList args) {
  auto cmd = CommandEncoder::Encode(cid, args);

  if (!cmd) {
    return kv_.Apply(cid, args);
  }

  if (group_.node().role() != RaftRole::Leader) {
    VLOG(1) << "SubmitCommand rejected: not leader (role=" << group_.node().role() << ")";
    return {ApplyOp::ERROR, 0};
  }

  VLOG(1) << "SubmitCommand: " << cmd->Serialize();

  if (group_.node().peer_manager().PeerCount() == 0) {
    return FastCommitPath(*cmd);
  }

  LogEntry entry(group_.node().term(), 0, cmd->Serialize());
  log_.Append(entry);

  return group_.node().ReplicateLog();
}

ApplyResult RaftEngine::FastCommitPath(const ReplicatedCommand& cmd) {
  LogEntry entry(group_.node().term(), 0, cmd.Serialize());
  log_.Append(entry);
  VLOG(1) << "FastCommitPath: appended " << cmd.Serialize()
          << " log_size=" << log_.LogSize();

  group_.node().AdvanceCommitIndex();
  return group_.node().ApplyCommittedLogs();
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
