#include "server/raft/command_encoder.h"

#include "server/service/command_registry.h"

namespace dfly {

std::string ReplicatedCommand::Serialize() const {
  std::string result;
  for (size_t i = 0; i < args.size(); i++) {
    if (i > 0)
      result += ' ';
    result += args[i];
  }
  return result;
}

ReplicatedCommand ReplicatedCommand::Deserialize(std::string_view data) {
  ReplicatedCommand cmd;
  cmd.type = CommandType::SET;

  size_t pos = 0;
  while (pos < data.size()) {
    size_t end = data.find(' ', pos);
    if (end == std::string_view::npos) {
      cmd.args.emplace_back(data.substr(pos));
      break;
    }
    cmd.args.emplace_back(data.substr(pos, end - pos));
    pos = end + 1;
  }

  if (!cmd.args.empty()) {
    std::string_view name = cmd.args[0];
    if (name == "SET")
      cmd.type = CommandType::SET;
    else if (name == "DEL")
      cmd.type = CommandType::DEL;
    else if (name == "EXPIRE")
      cmd.type = CommandType::EXPIRE;
  }

  return cmd;
}

std::optional<ReplicatedCommand> CommandEncoder::Encode(const CommandId* cid, CmdArgList args) {
  std::string_view name = cid->name();
  uint32_t mask = cid->opt_mask();

  if (!(mask & CO::WRITE))
    return std::nullopt;

  CommandType type;
  if (name == "SET") {
    type = CommandType::SET;
  } else if (name == "DEL") {
    type = CommandType::DEL;
  } else if (name == "EXPIRE") {
    type = CommandType::EXPIRE;
  } else {
    return std::nullopt;
  }

  std::vector<std::string> cmd_args;
  cmd_args.reserve(args.size());
  for (size_t i = 0; i < args.size(); i++) {
    cmd_args.push_back(std::string(ArgS(args, i)));
  }

  return ReplicatedCommand{type, std::move(cmd_args)};
}

bool CommandEncoder::IsWriteCommand(const CommandId* cid) {
  return cid->opt_mask() & CO::WRITE;
}

}  // namespace dfly
