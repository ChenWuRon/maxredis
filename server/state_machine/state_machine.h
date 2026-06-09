// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <cstdint>
#include <string_view>

#include "server/raft/raft_types.h"
#include "server/storage/common_types.h"
#include "server/storage/op_status.h"

namespace dfly {

class CommandId;
class EngineShard;
struct LogEntry;

enum class ApplyOp : uint8_t {
  OK = 0,
  NOT_FOUND = 1,
  ERROR = 2,
};

struct ApplyResult {
  ApplyOp op = ApplyOp::OK;
  uint64_t affected_rows = 0;
};

class IStateMachine {
 public:
  virtual ~IStateMachine() = default;

  virtual ApplyResult Apply(const CommandId* cid, CmdArgList args) = 0;

  // Applies a committed Raft log entry. The command string is
  // space-delimited: "SET key value" or "DEL key".
  virtual ApplyResult ApplyLogEntry(const LogEntry& entry) = 0;

  virtual void Set(DbIndex db_ind, std::string_view key, std::string_view val) = 0;
  virtual bool Del(DbIndex db_ind, std::string_view key) = 0;
  virtual bool Expire(DbIndex db_ind, std::string_view key, uint64_t expire_at_ms) = 0;
  virtual OpResult<std::string> Get(DbIndex db_ind, std::string_view key,
                                     ReadConsistency consistency = ReadConsistency::kLocal) = 0;
  virtual size_t DbSize(DbIndex db_ind) const = 0;
  virtual void Schedule(DbIndex db_ind, std::string_view key,
                         std::function<void(EngineShard*)> cb) = 0;

  // Exports the entire state machine state to a binary snapshot file at |path|.
  // Returns true on success.
  virtual bool SaveSnapshot(const std::string& path) {
    return false;
  }

  // Loads state from a binary snapshot file at |path|.
  // Replaces all existing state. Returns true on success.
  virtual bool LoadSnapshot(const std::string& path) {
    return false;
  }
};

}  // namespace dfly
