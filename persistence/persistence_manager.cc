// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "persistence/persistence_manager.h"

#include <cstdio>

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
  aof_writer_.Flush();
}

void PersistenceManager::Flush() {
  aof_writer_.Flush();
}

bool PersistenceManager::Load(std::string* out) {
  FILE* file = fopen("appendonly.aof", "r");
  if (!file) {
    return false;
  }

  fseek(file, 0, SEEK_END);
  long size = ftell(file);
  if (size <= 0) {
    fclose(file);
    return false;
  }

  fseek(file, 0, SEEK_SET);
  out->resize(size);
  size_t n = fread(out->data(), 1, size, file);
  fclose(file);

  return n == size_t(size) && !out->empty();
}

}  // namespace dfly
