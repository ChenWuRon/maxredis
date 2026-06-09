#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "server/raft/raft_types.h"

namespace dfly {

enum class CommandType : uint8_t {
  SET = 0,
  DEL = 1,
  EXPIRE = 2,
  CONFIG_CHANGE = 3,
};

struct ReplicatedCommand {
  CommandType type;
  std::vector<std::string> args;

  std::string Serialize() const;

  static ReplicatedCommand Deserialize(std::string_view data);
};

struct ConfigChangeCommand {
  ClusterConfig target;

  std::string Serialize() const;
  static ConfigChangeCommand Deserialize(std::string_view data);
};

}  // namespace dfly
