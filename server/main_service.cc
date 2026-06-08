// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/main_service.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <xxhash.h>

#include "base/flags.h"
#include "base/logging.h"

extern "C" {
#include "examples/redis_dict/sds.h"
}
#include "server/conn_context.h"
#include "server/debugcmd.h"
#include "util/metrics/metrics.h"
#include "util/varz.h"

ABSL_FLAG(uint32_t, port, 6380, "Redis port");
ABSL_FLAG(uint32_t, memcache_port, 0, "Memcached port");

namespace dfly {

using namespace std;
using namespace util;
using base::VarzValue;

namespace {

optional<VarzFunction> engine_varz;

}  // namespace

Service::Service(ProactorPool* pp) : shard_set_(pp), pp_(*pp) {
  CHECK(pp);
  RegisterCommands();
  engine_varz.emplace("engine", [this] { return GetVarzStats(); });
}

Service::~Service() {
}

void Service::Init(util::AcceptServer* acceptor) {
  uint32_t shard_num = pp_.size() > 1 ? pp_.size() - 1 : pp_.size();
  shard_set_.Init(shard_num);

  pp_.AwaitBrief([&](uint32_t index, ProactorBase* pb) {
    if (index < shard_count()) {
      shard_set_.InitThreadLocal(index);
    }
  });
}

void Service::Shutdown() {
  VLOG(1) << "Service::Shutdown";

  engine_varz.reset();
  for (unsigned i = 0; i < shard_set_.size(); ++i) {
    shard_set_.pool()->at(i)->Await([&] { EngineShard::DestroyThreadLocal(); });
  }
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
  string_view key = string_view(pcmd.tokens[1], sdslen(pcmd.tokens[1]));
  string_view val = string_view(pcmd.tokens[2], sdslen(pcmd.tokens[2]));
  VLOG(2) << "Set " << key << " " << val;

  ShardId sid = Shard(key, shard_count());
  shard_set_.Await(sid, [&] {
    EngineShard* es = EngineShard::tlocal();
    auto [it, res] = es->db_slice.AddOrFind(0, key);
    it->second.value = val;
  });

  cntx->SendStored();
}

void Service::Get(CmdArgList args, ConnectionContext* cntx) {
  const ParsedCommand& pcmd = *cntx->to_execute;
  string_view key = string_view(pcmd.tokens[1], sdslen(pcmd.tokens[1]));
  ShardId sid = Shard(key, shard_count());

  #if 0
  OpResult<string> opres = shard_set_.Await(sid, [&]() -> OpResult<string> {
    EngineShard* es = EngineShard::tlocal();
    OpResult<MainIterator> res = es->db_slice.Find(0, key);
    if (res) {
      return res.value()->second;
    }
    return res.status();
  });

  if (opres) {
    // cntx->SendGetReply(key, 0, opres.value());
  } else if (opres.status() == OpStatus::KEY_NOTFOUND) {
    cntx->SendGetNotFound();
  }
  cntx->EndMultilineReply();
#else
  CHECK(cntx->to_execute);
  cntx->to_execute->execute_async = 1;
  auto cb = [cntx, cmd = cntx->to_execute] {
    EngineShard* es = EngineShard::tlocal();
    string_view key = string_view(cmd->tokens[1], sdslen(cmd->tokens[1]));
    OpResult<MainIterator> res = es->db_slice.Find(0, key);
    if (res) {
      cntx->SendGetReply(key, 0, res.value()->second.value, cmd);
    } else if (res.status() == OpStatus::KEY_NOTFOUND) {
      cntx->SendGetNotFound(cmd);
    }
  };
  shard_set_.Add(sid, cb);
#endif
}

void Service::Del(CmdArgList args, ConnectionContext* cntx) {
  const ParsedCommand& pcmd = *cntx->to_execute;
  string_view key = string_view(pcmd.tokens[1], sdslen(pcmd.tokens[1]));
  VLOG(2) << "Del " << key;

  ShardId sid = Shard(key, shard_count());
  bool deleted = shard_set_.Await(sid, [&] {
    EngineShard* es = EngineShard::tlocal();
    return es->db_slice.Del(0, key);
  });

  cntx->SendLong(deleted ? 1 : 0);
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
  absl::StrAppend(&info, "tcp_port:6380\r\n");
  absl::StrAppend(&info, "arch_bits:64\r\n");
  absl::StrAppend(&info, "multiplexing_api:iouring\r\n");
  absl::StrAppend(&info, "process_id:", getpid(), "\r\n");
  absl::StrAppend(&info, "\r\n");

  absl::StrAppend(&info, "# Clients\r\n");
  absl::StrAppend(&info, "connected_clients:1\r\n");
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
  absl::StrAppend(&info, "aof_enabled:0\r\n");
  absl::StrAppend(&info, "\r\n");

  absl::StrAppend(&info, "# Stats\r\n");
  absl::StrAppend(&info, "total_connections_received:1\r\n");
  absl::StrAppend(&info, "total_commands_processed:0\r\n");
  absl::StrAppend(&info, "instantaneous_ops_per_sec:0\r\n");
  absl::StrAppend(&info, "keyspace_hits:0\r\n");
  absl::StrAppend(&info, "keyspace_misses:0\r\n");
  absl::StrAppend(&info, "\r\n");

  absl::StrAppend(&info, "# Replication\r\n");
  absl::StrAppend(&info, "role:master\r\n");
  absl::StrAppend(&info, "connected_slaves:0\r\n");
  absl::StrAppend(&info, "\r\n");

  absl::StrAppend(&info, "# CPU\r\n");
  absl::StrAppend(&info, "used_cpu_sys:0.000000\r\n");
  absl::StrAppend(&info, "used_cpu_user:0.000000\r\n");
  absl::StrAppend(&info, "\r\n");

  absl::StrAppend(&info, "# Keyspace\r\n");
  atomic_ulong num_keys{0};
  shard_set_.RunBriefInParallel([&](EngineShard* es) { num_keys += es->db_slice.DbSize(0); });
  absl::StrAppend(&info, "db0:keys=", num_keys.load(), ",expires=0,avg_ttl=0\r\n");

  cntx->SendRespBlob(absl::StrCat("$", info.size(), "\r\n", info, "\r\n"));
}

VarzValue::Map Service::GetVarzStats() {
  VarzValue::Map res;

  atomic_ulong num_keys{0};
  shard_set_.RunBriefInParallel([&](EngineShard* es) { num_keys += es->db_slice.DbSize(0); });
  res.emplace_back("keys", VarzValue::FromInt(num_keys.load()));

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
            << CI{"DEBUG", CO::RANDOM | CO::READONLY, -2, 0, 0, 0}.HFUNC(Debug)
            << CI{"INFO", CO::READONLY | CO::LOADING | CO::STALE, -1, 0, 0, 0}.HFUNC(Info);
}

}  // namespace dfly
