// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/command_serializer.h"

#include <absl/strings/str_cat.h>

namespace dfly {

std::string CommandSerializer::Serialize(const std::vector<std::string>& args) {
  std::string result = absl::StrCat("*", args.size(), "\r\n");
  for (const auto& arg : args) {
    absl::StrAppend(&result, "$", arg.size(), "\r\n", arg, "\r\n");
  }
  return result;
}

}  // namespace dfly
