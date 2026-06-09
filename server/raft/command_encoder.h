#pragma once

#include <optional>

#include "server/raft/replicated_command.h"
#include "server/storage/common_types.h"

namespace dfly {

class CommandId;

class CommandEncoder {
 public:
  static std::optional<ReplicatedCommand> Encode(const CommandId* cid, CmdArgList args);
  static bool IsWriteCommand(const CommandId* cid);
};

}  // namespace dfly
