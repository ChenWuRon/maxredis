// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>
#include <vector>

namespace dfly {

class CommandSerializer {
 public:
  // Serializes a command with its arguments into standard RESP wire format.
  // Input: vector of strings representing the command and arguments.
  // Output: RESP-encoded string (Array of Bulk Strings).
  // Example: {"SET", "a", "1"} -> "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
  static std::string Serialize(const std::vector<std::string>& args);
};

}  // namespace dfly
