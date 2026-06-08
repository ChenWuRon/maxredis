// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "server/persistence/aof_writer.h"

namespace dfly {

class PersistenceManager {
 public:
  PersistenceManager();

  // Opens the AOF file. Returns true on success.
  bool Open(std::string_view path);

  // Serializes a command via CommandSerializer and appends it to the AOF file.
  void RecordCommand(const std::vector<std::string>& args);

  // Flushes buffered AOF data to disk.
  void Flush();

  // Reads the full content of the AOF file into 'out'. Returns false if the file
  // cannot be read (e.g. does not exist, empty, or I/O error).
  bool Load(std::string* out);

 private:
  AofWriter aof_writer_;
};

}  // namespace dfly
