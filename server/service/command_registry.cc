// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/service/command_registry.h"

#include <vector>

extern "C" {
#include "examples/redis_dict/sds.h"
}

#include "absl/strings/str_cat.h"
#include "base/bits.h"
#include "base/logging.h"
#include "server/protocol/conn_context.h"

using namespace std;

namespace dfly {

using absl::StrAppend;
using absl::StrCat;

CommandId::CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key,
                     int8_t last_key, int8_t step)
    : name_(name), opt_mask_(mask), arity_(arity), first_key_(first_key), last_key_(last_key),
      step_key_(step) {
}

uint32_t CommandId::OptCount(uint32_t mask) {
  return absl::popcount(mask);
}

CommandRegistry::CommandRegistry() {
  CommandId cd("COMMAND", CO::RANDOM | CO::LOADING | CO::STALE, 0, 0, 0, 0);

  cd.SetHandler([this](const auto& args, auto* cntx) { return Command(args, cntx); });
  const char* nm = cd.name();
  cmd_map_.emplace(nm, std::move(cd));
}

void CommandRegistry::Command(CmdArgList args, ConnectionContext* cntx) {
  CHECK(cntx->to_execute);
  const ParsedCommand& pcmd = *cntx->to_execute;

  string_view subcmd;
  if (pcmd.argc >= 2) {
    sdstoupper(pcmd.tokens[1]);
    subcmd = string_view(pcmd.tokens[1], sdslen(pcmd.tokens[1]));
  }

  if (subcmd == "DOCS") {
    return CommandDoc(cntx);
  } else if (subcmd == "INFO") {
    return CommandInfo(pcmd, cntx);
  } else if (subcmd == "COUNT") {
    return CommandCount(cntx);
  } else if (subcmd == "GETKEYS") {
    return cntx->SendError("ERR unknown subcommand 'COMMAND GETKEYS'");
  } else if (!subcmd.empty()) {
    return cntx->SendError(absl::StrCat("ERR unknown subcommand 'COMMAND ", subcmd, "'"));
  }

  size_t sz = cmd_map_.size();
  string resp = StrCat("*", sz, "\r\n");

  for (const auto& val : cmd_map_) {
    const CommandId& cd = val.second;
    StrAppend(&resp, "*6\r\n$", strlen(cd.name()), "\r\n", cd.name(), "\r\n");
    StrAppend(&resp, ":", int(cd.arity()), "\r\n");
    StrAppend(&resp, "*", CommandId::OptCount(cd.opt_mask()), "\r\n");

    for (uint32_t i = 0; i < 32; ++i) {
      unsigned obit = (1u << i);
      if (cd.opt_mask() & obit) {
        const char* name = CO::OptName(CO::CommandOpt{obit});
        StrAppend(&resp, "+", name, "\r\n");
      }
    }

    StrAppend(&resp, ":", cd.first_key_pos(), "\r\n");
    StrAppend(&resp, ":", cd.last_key_pos(), "\r\n");
    StrAppend(&resp, ":", cd.key_arg_step(), "\r\n");
  }

  cntx->SendRespBlob(resp);
}

void CommandRegistry::CommandDoc(ConnectionContext* cntx) {
  // RESP2 flat alternating array: [name1, specs1, name2, specs2, ...]
  string resp = StrCat("*", cmd_map_.size() * 2, "\r\n");

  for (const auto& val : cmd_map_) {
    const CommandId& cd = val.second;
    // Command name
    StrAppend(&resp, "$", strlen(cd.name()), "\r\n", cd.name(), "\r\n");
    // Specs as flat alternating array (key-value pairs)
    StrAppend(&resp, "*10\r\n");
    StrAppend(&resp, "$7\r\nsummary\r\n");
    StrAppend(&resp, "$0\r\n\r\n");
    StrAppend(&resp, "$5\r\nsince\r\n");
    StrAppend(&resp, "$1\r\n1\r\n");
    StrAppend(&resp, "$5\r\ngroup\r\n");
    StrAppend(&resp, "$7\r\ngeneric\r\n");
    StrAppend(&resp, "$9\r\narguments\r\n");
    StrAppend(&resp, "*0\r\n");
    StrAppend(&resp, "$11\r\nsubcommands\r\n");
    StrAppend(&resp, "*0\r\n");
  }

  cntx->SendRespBlob(resp);
}

void CommandRegistry::CommandInfo(const ParsedCommand& pcmd, ConnectionContext* cntx) {
  if (pcmd.argc == 2) {
    // COMMAND INFO with no specific commands -> same as COMMAND
    size_t sz = cmd_map_.size();
    string resp = StrCat("*", sz, "\r\n");
    for (const auto& val : cmd_map_) {
      const CommandId& cd = val.second;
      StrAppend(&resp, "*6\r\n$", strlen(cd.name()), "\r\n", cd.name(), "\r\n");
      StrAppend(&resp, ":", int(cd.arity()), "\r\n");
      StrAppend(&resp, "*", CommandId::OptCount(cd.opt_mask()), "\r\n");
      for (uint32_t i = 0; i < 32; ++i) {
        unsigned obit = (1u << i);
        if (cd.opt_mask() & obit) {
          const char* name = CO::OptName(CO::CommandOpt{obit});
          StrAppend(&resp, "+", name, "\r\n");
        }
      }
      StrAppend(&resp, ":", cd.first_key_pos(), "\r\n");
      StrAppend(&resp, ":", cd.last_key_pos(), "\r\n");
      StrAppend(&resp, ":", cd.key_arg_step(), "\r\n");
    }
    cntx->SendRespBlob(resp);
  } else {
    // COMMAND INFO <cmd> [<cmd> ...]
    vector<string_view> names;
    for (unsigned i = 2; i < pcmd.argc; ++i) {
      names.emplace_back(pcmd.tokens[i], sdslen(pcmd.tokens[i]));
      sdstoupper(pcmd.tokens[i]);
    }
    string resp = StrCat("*", names.size(), "\r\n");
    for (const auto& name : names) {
      const CommandId* cid = Find(name.data());
      if (cid) {
        StrAppend(&resp, "*6\r\n$", strlen(cid->name()), "\r\n", cid->name(), "\r\n");
        StrAppend(&resp, ":", int(cid->arity()), "\r\n");
        StrAppend(&resp, "*", CommandId::OptCount(cid->opt_mask()), "\r\n");
        for (uint32_t i = 0; i < 32; ++i) {
          unsigned obit = (1u << i);
          if (cid->opt_mask() & obit) {
            const char* name_str = CO::OptName(CO::CommandOpt{obit});
            StrAppend(&resp, "+", name_str, "\r\n");
          }
        }
        StrAppend(&resp, ":", cid->first_key_pos(), "\r\n");
        StrAppend(&resp, ":", cid->last_key_pos(), "\r\n");
        StrAppend(&resp, ":", cid->key_arg_step(), "\r\n");
      } else {
        StrAppend(&resp, "$-1\r\n");
      }
    }
    cntx->SendRespBlob(resp);
  }
}

void CommandRegistry::CommandCount(ConnectionContext* cntx) {
  string resp = StrCat(":", cmd_map_.size(), "\r\n");
  cntx->SendRespBlob(resp);
}

namespace CO {

const char* OptName(CO::CommandOpt fl) {
  using namespace CO;

  switch (fl) {
    case WRITE:
      return "write";
    case READONLY:
      return "readonly";
    case DENYOOM:
      return "denyoom";
    case FAST:
      return "fast";
    case STALE:
      return "stale";
    case LOADING:
      return "loading";
    case RANDOM:
      return "random";
  }
  return "";
}

}  // namespace CO

}  // namespace dfly
