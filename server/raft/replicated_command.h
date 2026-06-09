#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dfly {

enum class CommandType : uint8_t {
  SET = 0,
  DEL = 1,
  EXPIRE = 2,
};

struct ReplicatedCommand {
  CommandType type;
  std::vector<std::string> args;

  std::string Serialize() const;

  static ReplicatedCommand Deserialize(std::string_view data);
};

}  // namespace dfly
