// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/service/main_service.h"

#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <xxhash.h>

#include "base/flags.h"
#include "base/logging.h"

extern "C" {
#include "examples/redis_dict/sds.h"
}
#include "server/protocol/conn_context.h"
#include "server/service/debugcmd.h"
#include "server/persistence/persistence_manager.h"
#include "util/metrics/metrics.h"
#include "util/varz.h"

#include "io/io.h"
#include "server/persistence/snapshot_manager.h"
#include "server/service/dragonfly_connection.h"
#include "server/service/state_serializer.h"

ABSL_FLAG(uint32_t, port, 6380, "Redis port");
ABSL_FLAG(uint32_t, memcache_port, 0, "Memcached port");
ABSL_FLAG(uint32_t, snapshot_time_sec, 0, "Snapshot interval in seconds (0 = disabled)");
ABSL_FLAG(uint32_t, snapshot_cmd_count, 0, "Snapshot after N write commands (0 = disabled)");
ABSL_FLAG(bool, linearizable_read, false,
          "If true, GET performs a linearizable read via the Raft ReadIndex "
          "protocol (confirms leadership before reading). If false, GET reads "
          "the local state directly (may return stale data).");
ABSL_FLAG(std::string, raft_dir, "",
          "Directory for durable Raft state (meta.json, segmented WAL, "
          "apply.meta, snapshots). Empty disables Raft persistence "
          "(in-memory log, AOF remains the durability mechanism).");
ABSL_FLAG(uint32_t, raft_fsync_interval_ms, 0,
          "Raft WAL fsync interval in milliseconds. 0 = fsync every append "
          "(fully durable). >0 = batch fsync every interval: appends write to "
          "the page cache (kill -9 safe) and a background fiber fsyncs; a "
          "power failure may lose the last interval (AOF 'everysec' analogue).");

namespace dfly {

using namespace std;
using namespace util;
using base::VarzValue;

namespace {

optional<VarzFunction> engine_varz;

}  // namespace

Service::Service(ProactorPool* pp)
    : shard_set_(pp), pp_(*pp), engine_(&shard_set_, &pp_) {
  CHECK(pp);
  RegisterCommands();
  engine_varz.emplace("engine", [this] { return GetVarzStats(); });

  persistence_manager_ = new PersistenceManager;
}

Service::~Service() {
  if (persistence_manager_) {
    persistence_manager_->Flush();
    delete persistence_manager_;
  }
}

void Service::Init(util::AcceptServer* acceptor) {
  uint32_t shard_num = pp_.size() > 1 ? pp_.size() - 1 : pp_.size();
  shard_set_.Init(shard_num);

  pp_.AwaitBrief([&](uint32_t index, ProactorBase* pb) {
    if (index < shard_count()) {
      shard_set_.InitThreadLocal(index);
    }
  });

  std::string raft_dir = absl::GetFlag(FLAGS_raft_dir);
  bool raft_persistence = !raft_dir.empty();

  // When the Raft WAL is the durability mechanism, the Raft snapshot dir is
  // the ONLY authoritative state source: the server-level snapshot.bin / AOF
  // carry no Raft index binding, so replaying them on top of WAL recovery
  // would double-apply or skip writes. They are only used in classic
  // single-node mode (raft_dir empty).
  if (!raft_persistence) {
    LoadSnapshot();
  }

  {
    using absl::GetFlag;
    uint32_t time_sec = GetFlag(FLAGS_snapshot_time_sec);
    uint32_t cmd_cnt = GetFlag(FLAGS_snapshot_cmd_count);
    if (time_sec > 0 || cmd_cnt > 0) {
      pp_.AwaitBrief([&](uint32_t index, ProactorBase*) {
        if (index == 0) {
          snapshot_fiber_.Start(time_sec, cmd_cnt);
        }
      });
    }
  }

  persistence_manager_->Open("appendonly.aof");

  if (raft_persistence) {
    // Durable Raft state: open the segmented WAL + meta, restore in-flight
    // joint config. (Pure file IO — safe to run on the calling thread.)
    bool ok = engine_.InitRaftStorage(
        raft_dir, absl::GetFlag(FLAGS_raft_fsync_interval_ms));
    if (!ok) {
      LOG(WARNING) << "Raft: could not initialize durable storage in '"
                   << raft_dir << "'";
    } else {
      if (engine_.LastSnapshotIndex() == 0) {
        // No Raft snapshot → the KV state machine is EMPTY after this
        // restart (server snapshot/AOF are not replayed in Raft mode).
        // Drop the stale apply.meta so the WAL is re-applied from the
        // beginning (single-node leader) or the leader re-syncs us
        // (follower — ReplayUnappliedLogs never self-commits).
        engine_.group().node().ResetApplyProgress(0);
        LOG(INFO) << "Raft: no snapshot found, replaying the full WAL";
      }
      // Batch apply.meta fsyncs together with the WAL policy: with
      // raft_fsync_interval_ms > 0, apply.meta is flushed at the same cadence
      // (after the WAL), removing the per-write fsync from the hot path.
      engine_.group().node().SetApplyMetaFlushInterval(
          absl::GetFlag(FLAGS_raft_fsync_interval_ms));
      // Replay entries that were applied to the log but not yet to the state
      // machine (crash-recovery point). Runs inside a wrapped fiber (NOT the
      // dispatcher, unlike AwaitBrief): applying entries dispatches work to
      // shards via shard_set_->Await, which fiber-blocks. Preempting the
      // dispatcher would trip fb2's "should not preempt dispatcher" check.
      pp_.AwaitFiberOnAll([&](uint32_t index, ProactorBase*) {
        if (index == 0) {
          engine_.group().node().ReplayUnappliedLogs();
          LOG(INFO) << "Raft: durable storage initialized in '" << raft_dir << "'";
        }
      });
    }
  }

  // Bootstrap a single-node Raft cluster into Leader state BEFORE replaying the
  // AOF: replayed writes go through SubmitCommand, which requires Leader. This
  // is a no-op if peers are configured. It makes writes (SET/DEL/EXPIRE) and
  // linearizable reads (ReadIndex requires Leader) functional in the default
  // single-node setup.
  pp_.AwaitBrief([&](uint32_t index, ProactorBase*) {
    if (index == 0) {
      if (engine_.BootstrapSingleNode()) {
        LOG(INFO) << "Raft: node is Leader (single-node bootstrap)"
                  << (absl::GetFlag(FLAGS_linearizable_read)
                          ? "; linearizable reads enabled"
                          : "");
      } else {
        LOG(WARNING) << "Raft: could not bootstrap Leader; writes and "
                        "linearizable reads will fail until a Leader is elected";
      }
    }
  });

  // When the Raft WAL is the durable write path, AOF replay would re-apply
  // already-durable writes; the WAL replay above is authoritative instead.
  if (!raft_persistence)
    ReplayAof();
}

void Service::Shutdown() {
  VLOG(1) << "Service::Shutdown";

  // No new connections are accepted at this point (the accept loop was
  // already broken by the signal handler); drain what is in flight:
  //   1. stop the snapshot fiber (no more snapshot cycles),
  //   2. stop Raft background activity and fsync the WAL,
  //   3. fsync the AOF.
  snapshot_fiber_.Stop();

  engine_.Shutdown();

  if (persistence_manager_) {
    persistence_manager_->Flush();
  }

  engine_varz.reset();
  for (unsigned i = 0; i < shard_set_.size(); ++i) {
    shard_set_.pool()->at(i)->Await([&] { EngineShard::DestroyThreadLocal(); });
  }
}

namespace {

struct AofParseResult {
  vector<vector<string>> commands;
  // true when the whole file was consumed cleanly.
  bool ok = true;
  size_t error_pos = 0;
  string error_msg;
};

// Parses RESP arrays of bulk strings (the AOF format produced by
// CommandSerializer). Unlike the previous implementation this never throws
// and reports the exact offset + reason of any corruption instead of
// silently dropping the rest of the file.
AofParseResult ParseRespCommands(string_view content) {
  AofParseResult result;
  size_t pos = 0;

  auto fail = [&](size_t at, std::string_view msg) {
    result.ok = false;
    result.error_pos = at;
    result.error_msg = std::string(msg);
  };

  while (pos < content.size()) {
    if (content[pos] == '\r' || content[pos] == '\n') {
      pos += (content[pos] == '\r' && pos + 1 < content.size() && content[pos + 1] == '\n') ? 2 : 1;
      continue;
    }
    if (content[pos] != '*') {
      fail(pos, "expected '*' (RESP array) at offset " + std::to_string(pos));
      break;
    }
    pos++;

    size_t end = content.find("\r\n", pos);
    if (end == string_view::npos) {
      fail(pos, "unterminated array length");
      break;
    }
    int argc = 0;
    if (!absl::SimpleAtoi(content.substr(pos, end - pos), &argc) || argc < 0) {
      fail(pos, "invalid array length at offset " + std::to_string(pos));
      break;
    }
    pos = end + 2;

    vector<string> args;
    args.reserve(argc);
    bool malformed = false;
    for (int i = 0; i < argc; i++) {
      if (pos >= content.size() || content[pos] != '$') {
        fail(pos, "expected '$' (bulk string) at offset " + std::to_string(pos));
        malformed = true;
        break;
      }
      pos++;
      end = content.find("\r\n", pos);
      if (end == string_view::npos) {
        fail(pos, "unterminated bulk length");
        malformed = true;
        break;
      }
      int len = 0;
      if (!absl::SimpleAtoi(content.substr(pos, end - pos), &len) || len < 0) {
        fail(pos, "invalid bulk length at offset " + std::to_string(pos));
        malformed = true;
        break;
      }
      pos = end + 2;
      if (pos + len > content.size()) {
        fail(pos, "truncated bulk payload at offset " + std::to_string(pos) +
                       " (need " + std::to_string(len) + " bytes, have " +
                       std::to_string(content.size() - pos) + ")");
        malformed = true;
        break;
      }
      args.emplace_back(content.substr(pos, len));
      pos += len;
      if (pos + 2 > content.size() || content[pos] != '\r' || content[pos + 1] != '\n') {
        fail(pos, "missing CRLF after bulk payload at offset " + std::to_string(pos));
        malformed = true;
        break;
      }
      pos += 2;
    }
    if (malformed)
      break;
    if (!args.empty()) {
      result.commands.push_back(std::move(args));
    }
  }
  return result;
}

}  // namespace

void Service::ReplayAof() {
  string content;
  if (!persistence_manager_->Load(&content)) {
    return;
  }

  auto parse_result = ParseRespCommands(content);
  if (!parse_result.ok) {
    // Corruption must never be swallowed: refuse to silently replay a
    // truncated AOF and tell the operator exactly where it broke.
    LOG(ERROR) << "AOF parse error at offset " << parse_result.error_pos << ": "
               << parse_result.error_msg
               << ". " << parse_result.commands.size()
               << " commands replayed before the corruption.";
    if (parse_result.commands.empty())
      return;
  }
  auto& commands = parse_result.commands;

  LOG(INFO) << "Replaying " << commands.size() << " commands from AOF";

  replay_mode_ = true;

  io::NullSink null_sink;
  Connection conn(Protocol::REDIS, this, nullptr);
  ConnectionContext cntx(&null_sink, &conn);
  cntx.shard_set = &shard_set_;

  for (const auto& cmd_args : commands) {
    unsigned argc = cmd_args.size();
    sds* tokens = (sds*)malloc(argc * sizeof(sds));
    for (unsigned i = 0; i < argc; i++) {
      tokens[i] = sdsnewlen(cmd_args[i].data(), cmd_args[i].size());
    }

    ParsedCommand parsed_cmd;
    parsed_cmd.tokens = tokens;
    parsed_cmd.argc = argc;
    parsed_cmd.parse_complete = 1;

    cntx.to_execute = &parsed_cmd;

    CmdArgList arg_list{reinterpret_cast<MutableStrSpan*>(tokens), argc};
    DispatchCommand(arg_list, &cntx);

    for (unsigned i = 0; i < argc; i++) {
      sdsfree(tokens[i]);
    }
    free(tokens);
  }

  replay_mode_ = false;
}

void Service::DispatchCommand(CmdArgList deprecated, ConnectionContext* cntx) {
  CHECK(cntx->to_execute);
  DCHECK_NE(0u, shard_set_.size()) << "Init was not called";

  auto& parsed_cmd = *cntx->to_execute;
  CHECK_GT(parsed_cmd.argc, 0u);

  //ToUpper(&args[0]);
  sdstoupper(parsed_cmd.tokens[0]);
  // VLOG(2) << "Got: " << args;

  string_view cmd_str = string_view(parsed_cmd.tokens[0], sdslen(parsed_cmd.tokens[0]));
  const CommandId* cid = registry_.Find(cmd_str);

  if (cid == nullptr) {
    return cntx->SendError(absl::StrCat("unknown command `", cmd_str, "`"));
  }
  unsigned argc = parsed_cmd.argc;
  if ((cid->arity() > 0 && argc != size_t(cid->arity())) ||
      (cid->arity() < 0 && argc < size_t(-cid->arity()))) {
    return cntx->SendError(WrongNumArgsError(cmd_str));
  }
  cntx->cid = cid;
  parsed_cmd.dispatched = 1;
  commands_processed_.fetch_add(1, std::memory_order_relaxed);
  cid->Invoke(deprecated, cntx);
}

void Service::DispatchMC(const MemcacheParser::Command& cmd, std::string_view value,
                         ConnectionContext* cntx) {
  absl::InlinedVector<MutableStrSpan, 8> args;
  char cmd_name[16];
  char set_opt[4] = {0};

  switch (cmd.type) {
    case MemcacheParser::REPLACE:
      strcpy(cmd_name, "SET");
      strcpy(set_opt, "XX");
      break;
    case MemcacheParser::SET:
      strcpy(cmd_name, "SET");
      break;
    case MemcacheParser::ADD:
      strcpy(cmd_name, "SET");
      strcpy(set_opt, "NX");
      break;
    case MemcacheParser::GET:
      strcpy(cmd_name, "GET");
      break;
    default:
      cntx->SendMCClientError("bad command line format");
      return;
  }

  args.emplace_back(cmd_name, strlen(cmd_name));
  char* key = const_cast<char*>(cmd.key.data());
  args.emplace_back(key, cmd.key.size());

  if (MemcacheParser::IsStoreCmd(cmd.type)) {
    char* v = const_cast<char*>(value.data());
    args.emplace_back(v, value.size());

    if (set_opt[0]) {
      args.emplace_back(set_opt, strlen(set_opt));
    }
  }

  CmdArgList arg_list{args.data(), args.size()};
  DispatchCommand(arg_list, cntx);
}

void Service::RegisterHttp(HttpListenerBase* listener) {
  CHECK_NOTNULL(listener);
}

void Service::Ping(CmdArgList args, ConnectionContext* cntx) {
  const ParsedCommand& pcmd = *cntx->to_execute;

  if (pcmd.argc > 2) {
    return cntx->SendError("wrong number of arguments for 'ping' command");
  }

  if (pcmd.argc == 1) {
    return cntx->SendSimpleRespString("PONG");
  }
  std::string_view arg = string_view(pcmd.tokens[1], sdslen(pcmd.tokens[1]));
  DVLOG(2) << "Ping " << arg;

  return cntx->SendSimpleRespString(arg);
}

void Service::Set(CmdArgList args, ConnectionContext* cntx) {
  const ParsedCommand& pcmd = *cntx->to_execute;
  VLOG(2) << "Set " << pcmd.tokens[1] << " " << pcmd.tokens[2];

  CmdArgVec cmd_vec;
  cmd_vec.reserve(pcmd.argc);
  for (unsigned i = 0; i < pcmd.argc; ++i) {
    cmd_vec.emplace_back(pcmd.tokens[i], sdslen(pcmd.tokens[i]));
  }
  ApplyResult result = engine_.SubmitCommand(cntx->cid, CmdArgList{cmd_vec.data(), cmd_vec.size()});

  if (result.op == ApplyOp::ERROR) {
    return cntx->SendError("READONLY You can't write against a non-leader");
  }

  cntx->SendStored();

  if (!replay_mode_) {
    vector<string> cmd_args;
    cmd_args.reserve(pcmd.argc);
    for (unsigned i = 0; i < pcmd.argc; ++i) {
      cmd_args.emplace_back(pcmd.tokens[i], sdslen(pcmd.tokens[i]));
    }
    persistence_manager_->RecordCommand(cmd_args);
    snapshot_fiber_.NotifyWrite();
  }
}

void Service::Get(CmdArgList args, ConnectionContext* cntx) {
  const ParsedCommand& pcmd = *cntx->to_execute;
  string_view key = string_view(pcmd.tokens[1], sdslen(pcmd.tokens[1]));

  // Linearizable read path: confirm leadership via the Raft ReadIndex protocol
  // before serving the value. ReadIndex() blocks (on the current fiber) until
  // last_applied has caught up to the confirmed read index, guaranteeing we
  // observe every write that completed before this GET was issued.
  if (absl::GetFlag(FLAGS_linearizable_read)) {
    LogIndex ri = engine_.ReadIndex();
    if (ri == 0) {
      return cntx->SendError("READONLY Cannot serve linearizable read: not leader");
    }
  }

  CHECK(cntx->to_execute);
  cntx->to_execute->execute_async = 1;
  auto cb = [cntx, cmd = cntx->to_execute, this](EngineShard* es) {
    string_view key = string_view(cmd->tokens[1], sdslen(cmd->tokens[1]));
    OpResult<MainIterator> res = es->db_slice.Find(0, key);
    if (res) {
      keyspace_hits_.fetch_add(1, std::memory_order_relaxed);
      cntx->SendGetReply(key, 0, res.value()->second.value, cmd);
    } else if (res.status() == OpStatus::KEY_NOTFOUND) {
      keyspace_misses_.fetch_add(1, std::memory_order_relaxed);
      cntx->SendGetNotFound(cmd);
    }
  };
  engine_.Schedule(0, key, std::move(cb));
}

void Service::Del(CmdArgList args, ConnectionContext* cntx) {
  const ParsedCommand& pcmd = *cntx->to_execute;
  VLOG(2) << "Del " << pcmd.tokens[1];

  CmdArgVec cmd_vec;
  cmd_vec.reserve(pcmd.argc);
  for (unsigned i = 0; i < pcmd.argc; ++i) {
    cmd_vec.emplace_back(pcmd.tokens[i], sdslen(pcmd.tokens[i]));
  }
  ApplyResult result = engine_.SubmitCommand(cntx->cid, CmdArgList{cmd_vec.data(), cmd_vec.size()});

  if (result.op == ApplyOp::ERROR) {
    return cntx->SendError("READONLY You can't write against a non-leader");
  }

  cntx->SendLong(result.affected_rows);

  if (!replay_mode_) {
    vector<string> cmd_args;
    cmd_args.reserve(pcmd.argc);
    for (unsigned i = 0; i < pcmd.argc; ++i) {
      cmd_args.emplace_back(pcmd.tokens[i], sdslen(pcmd.tokens[i]));
    }
    persistence_manager_->RecordCommand(cmd_args);
    snapshot_fiber_.NotifyWrite();
  }
}

void Service::Expire(CmdArgList args, ConnectionContext* cntx) {
  const ParsedCommand& pcmd = *cntx->to_execute;
  string_view key = string_view(pcmd.tokens[1], sdslen(pcmd.tokens[1]));
  string_view val = string_view(pcmd.tokens[2], sdslen(pcmd.tokens[2]));
  VLOG(2) << "Expire " << key << " " << val;

  int64_t seconds;
  if (!absl::SimpleAtoi(val, &seconds) || seconds < 0) {
    return cntx->SendError("value is not an integer or out of range");
  }

  CmdArgVec cmd_vec;
  cmd_vec.reserve(pcmd.argc);
  for (unsigned i = 0; i < pcmd.argc; ++i) {
    cmd_vec.emplace_back(pcmd.tokens[i], sdslen(pcmd.tokens[i]));
  }
  ApplyResult result = engine_.SubmitCommand(cntx->cid, CmdArgList{cmd_vec.data(), cmd_vec.size()});

  if (result.op == ApplyOp::ERROR) {
    return cntx->SendError("READONLY You can't write against a non-leader");
  }

  cntx->SendLong(result.affected_rows);
}

bool Service::CreateSnapshot() {
  vector<SnapshotData> shard_snapshots(shard_count());

  shard_set_.RunBriefInParallel([&](EngineShard* es) {
    ShardId sid = es->shard_id();
    shard_snapshots[sid] = StateSerializer::Export(es->db_slice);
  });

  size_t total = 0;
  for (const auto& ss : shard_snapshots)
    total += ss.entries.size();

  SnapshotData merged;
  merged.entries.reserve(total);
  for (auto& ss : shard_snapshots) {
    for (auto& e : ss.entries) {
      merged.entries.push_back(std::move(e));
    }
  }

  SnapshotManager mgr;
  return mgr.Save("snapshot.bin", merged);
}

void Service::LoadSnapshot() {
  SnapshotManager mgr;
  SnapshotData data;
  if (!mgr.Load("snapshot.bin", &data)) {
    return;
  }

  LOG(INFO) << "Loading snapshot with " << data.entries.size() << " keys";

  vector<vector<SnapshotEntry>> shard_entries(shard_count());
  for (auto& entry : data.entries) {
    ShardId sid = Shard(entry.key, shard_count());
    shard_entries[sid].push_back(std::move(entry));
  }

  for (ShardId sid = 0; sid < shard_count(); sid++) {
    if (shard_entries[sid].empty())
      continue;
    shard_set_.Await(sid, [&] {
      EngineShard* es = EngineShard::tlocal();
      SnapshotData shard_data;
      shard_data.entries = std::move(shard_entries[sid]);
      StateSerializer::Import(&es->db_slice, shard_data);
    });
  }
}

void Service::Save(CmdArgList args, ConnectionContext* cntx) {
  if (CreateSnapshot()) {
    cntx->SendOk();
  } else {
    cntx->SendError("ERR Failed to create snapshot");
  }
}

void Service::Debug(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[1]);

  DebugCmd dbg_cmd{&shard_set_, cntx};

  return dbg_cmd.Run(args);
}

void Service::Info(CmdArgList args, ConnectionContext* cntx) {
  string info;

  absl::StrAppend(&info, "# Server\r\n");
  absl::StrAppend(&info, "redis_version:7.2.0\r\n");
  absl::StrAppend(&info, "os:Linux\r\n");
  absl::StrAppend(&info, "tcp_port:", absl::GetFlag(FLAGS_port), "\r\n");
  absl::StrAppend(&info, "arch_bits:64\r\n");
  absl::StrAppend(&info, "multiplexing_api:iouring\r\n");
  absl::StrAppend(&info, "process_id:", getpid(), "\r\n");
  absl::StrAppend(&info, "\r\n");

  absl::StrAppend(&info, "# Clients\r\n");
  absl::StrAppend(&info, "connected_clients:", Connection::connection_count(), "\r\n");
  absl::StrAppend(&info, "blocked_clients:0\r\n");
  absl::StrAppend(&info, "\r\n");

  absl::StrAppend(&info, "# Memory\r\n");
  absl::StrAppend(&info, "used_memory:0\r\n");
  absl::StrAppend(&info, "used_memory_human:0B\r\n");
  absl::StrAppend(&info, "maxmemory:0\r\n");
  absl::StrAppend(&info, "maxmemory_policy:noeviction\r\n");
  absl::StrAppend(&info, "\r\n");

  absl::StrAppend(&info, "# Persistence\r\n");
  absl::StrAppend(&info, "loading:0\r\n");
  absl::StrAppend(&info, "rdb_enabled:0\r\n");
  absl::StrAppend(&info, "aof_enabled:1\r\n");
  absl::StrAppend(&info, "\r\n");

  absl::StrAppend(&info, "# Stats\r\n");
  absl::StrAppend(&info, "total_connections_received:",
                 Connection::connection_count(), "\r\n");
  absl::StrAppend(&info, "total_commands_processed:",
                 commands_processed_.load(std::memory_order_relaxed), "\r\n");
  absl::StrAppend(&info, "instantaneous_ops_per_sec:0\r\n");
  absl::StrAppend(&info, "keyspace_hits:", keyspace_hits_.load(std::memory_order_relaxed), "\r\n");
  absl::StrAppend(&info, "keyspace_misses:", keyspace_misses_.load(std::memory_order_relaxed),
                 "\r\n");
  absl::StrAppend(&info, "\r\n");

  absl::StrAppend(&info, "# Replication\r\n");
  absl::StrAppend(&info, "role:", engine_.IsLeader() ? "master" : "follower", "\r\n");
  absl::StrAppend(&info, "connected_slaves:0\r\n");
  absl::StrAppend(&info, "\r\n");

  absl::StrAppend(&info, "# Raft\r\n");
  absl::StrAppend(&info, "raft_term:", engine_.CurrentTerm(), "\r\n");
  absl::StrAppend(&info, "raft_voted_for:", engine_.VotedFor(), "\r\n");
  absl::StrAppend(&info, "raft_commit_index:", engine_.CommitIndex(), "\r\n");
  absl::StrAppend(&info, "raft_last_applied:", engine_.LastApplied(), "\r\n");
  absl::StrAppend(&info, "raft_last_log_index:", engine_.LastLogIndex(), "\r\n");
  absl::StrAppend(&info, "raft_last_snapshot_index:", engine_.LastSnapshotIndex(), "\r\n");
  absl::StrAppend(&info, "\r\n");

  absl::StrAppend(&info, "# CPU\r\n");
  absl::StrAppend(&info, "used_cpu_sys:0.000000\r\n");
  absl::StrAppend(&info, "used_cpu_user:0.000000\r\n");
  absl::StrAppend(&info, "\r\n");

  absl::StrAppend(&info, "# Keyspace\r\n");
  absl::StrAppend(&info, "db0:keys=", engine_.DbSize(0), ",expires=0,avg_ttl=0\r\n");

  cntx->SendRespBlob(absl::StrCat("$", info.size(), "\r\n", info, "\r\n"));
}

VarzValue::Map Service::GetVarzStats() {
  VarzValue::Map res;

  res.emplace_back("keys", VarzValue::FromInt(engine_.DbSize(0)));

  return res;
}

using ServiceFunc = void (Service::*)(CmdArgList args, ConnectionContext* cntx);
inline CommandId::Handler HandlerFunc(Service* se, ServiceFunc f) {
  return [=](CmdArgList args, ConnectionContext* cntx) { return (se->*f)(args, cntx); };
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &Service::x))

void Service::RegisterCommands() {
  using CI = CommandId;

  registry_ << CI{"PING", CO::STALE | CO::FAST, -1, 0, 0, 0}.HFUNC(Ping)
            << CI{"SET", CO::WRITE | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(Set)
            << CI{"GET", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(Get)
            << CI{"DEL", CO::WRITE, -2, 1, 1, 1}.HFUNC(Del)
            << CI{"EXPIRE", CO::WRITE, 3, 1, 1, 1}.HFUNC(Expire)
            << CI{"SAVE", CO::WRITE | CO::STALE, 1, 0, 0, 0}.HFUNC(Save)
            << CI{"DEBUG", CO::RANDOM | CO::READONLY, -2, 0, 0, 0}.HFUNC(Debug)
            << CI{"INFO", CO::READONLY | CO::LOADING | CO::STALE, -1, 0, 0, 0}.HFUNC(Info);
}

}  // namespace dfly
