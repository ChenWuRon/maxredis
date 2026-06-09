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
    else if (name == "CONFIG_CHANGE")
      cmd.type = CommandType::CONFIG_CHANGE;
  }

  return cmd;
}

std::string ConfigChangeCommand::Serialize() const {
  std::string result = "CONFIG_CHANGE ";
  result += std::to_string(target.version);
  result += ' ';
  result += std::to_string(target.voters.size());
  for (const auto& v : target.voters) {
    result += ' ';
    result += v;
  }
  result += ' ';
  result += std::to_string(target.learners.size());
  for (const auto& l : target.learners) {
    result += ' ';
    result += l;
  }
  return result;
}

ConfigChangeCommand ConfigChangeCommand::Deserialize(std::string_view data) {
  ConfigChangeCommand cmd;
  size_t pos = 0;

  auto next_token = [&]() -> std::string_view {
    if (pos >= data.size())
      return {};
    size_t end = data.find(' ', pos);
    if (end == std::string_view::npos) {
      std::string_view tok = data.substr(pos);
      pos = data.size();
      return tok;
    }
    std::string_view tok = data.substr(pos, end - pos);
    pos = end + 1;
    return tok;
  };

  // Skip "CONFIG_CHANGE"
  next_token();

  // Parse version
  auto ver_token = next_token();
  if (!ver_token.empty())
    cmd.target.version = std::stoull(std::string(ver_token));

  // Parse voters
  auto num_voters_token = next_token();
  if (!num_voters_token.empty()) {
    size_t num_voters = std::stoul(std::string(num_voters_token));
    for (size_t i = 0; i < num_voters; i++) {
      auto v = next_token();
      if (!v.empty())
        cmd.target.voters.insert(std::string(v));
    }
  }

  // Parse learners
  auto num_learners_token = next_token();
  if (!num_learners_token.empty()) {
    size_t num_learners = std::stoul(std::string(num_learners_token));
    for (size_t i = 0; i < num_learners; i++) {
      auto l = next_token();
      if (!l.empty())
        cmd.target.learners.insert(std::string(l));
    }
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
