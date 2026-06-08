// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "persistence/persistence_manager.h"

#include "server/command_serializer.h"

namespace dfly {

PersistenceManager::PersistenceManager() {
}

bool PersistenceManager::Open(std::string_view path) {
  return aof_writer_.Open(path);
}

void PersistenceManager::RecordCommand(const std::vector<std::string>& args) {
  std::string resp = CommandSerializer::Serialize(args);
  aof_writer_.Append(resp);
}

void PersistenceManager::Flush() {
  aof_writer_.Flush();
}

}  // namespace dfly
