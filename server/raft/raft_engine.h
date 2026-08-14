// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <string_view>

#include "server/raft/command_log.h"
#include "server/raft/raft_group.h"
#include "server/raft/replicated_command.h"
#include "server/raft/snapshot_manager.h"
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

  // Bootstraps a single-node cluster into Leader state so that writes and
  // linearizable reads become functional. No-op if already Leader or if peers
  // exist. Returns true if the node is Leader afterwards.
  bool BootstrapSingleNode() {
    return group_.node().BootstrapSingleNode();
  }

  // Initializes persistent storage for the Raft group (segmented WAL,
  // meta.json, apply.meta, snapshot manager + barrier wiring).
  // |base_path| is the directory under which raft/group_N/ lives.
  // |fsync_interval_ms|: 0 = fsync per append; >0 = batch fsync interval.
  // Must be called once, before any consensus traffic.
  bool InitRaftStorage(const std::string& base_path, uint32_t fsync_interval_ms = 0);

  // Stops all Raft background activity (snapshot driver, heartbeat, timers).
  void Shutdown() {
    group_.Shutdown();
  }

  bool IsLeader() const {
    return group_.node().role() == RaftRole::Leader;
  }

  // Consensus state accessors for INFO/observability. All lock internally.
  Term CurrentTerm() const {
    return group_.node().term();
  }

  std::string VotedFor() const {
    return group_.node().voted_for();
  }

  LogIndex CommitIndex() const {
    return group_.node().commit_index();
  }

  LogIndex LastApplied() const {
    return group_.node().last_applied();
  }

  LogIndex LastLogIndex() const {
    const ILogStorage* ls = group_.log_storage();
    return ls ? ls->LastIndex() : 0;
  }

  LogIndex LastSnapshotIndex() const {
    return group_.node().last_snapshot_index();
  }

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
  KvStateMachine kv_;
  RaftGroup group_;
};

}  // namespace dfly
