#pragma once

#include <optional>
#include <string>
#include <vector>

#include "server/raft/replicated_command.h"
#include "server/storage/common_types.h"

namespace dfly {

class CommandId;

// Parses a RESP array of bulk strings. Returns false on malformed input.
// Shared by ReplicatedCommand::Deserialize and the state machine apply path.
bool ParseRespArray(std::string_view data, std::vector<std::string>* out);

class CommandEncoder {
 public:
  static std::optional<ReplicatedCommand> Encode(const CommandId* cid, CmdArgList args);
  static bool IsWriteCommand(const CommandId* cid);
};

}  // namespace dfly
