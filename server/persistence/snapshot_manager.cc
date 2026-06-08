// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/persistence/snapshot_manager.h"

#include <cstdio>

#include "base/logging.h"

namespace dfly {

namespace {

void WriteU32(std::string* out, uint32_t v) {
  out->append(reinterpret_cast<const char*>(&v), sizeof(v));
}

void WriteU64(std::string* out, uint64_t v) {
  out->append(reinterpret_cast<const char*>(&v), sizeof(v));
}

bool ReadU32(const char*& pos, const char* end, uint32_t* v) {
  if (pos + sizeof(uint32_t) > end)
    return false;
  memcpy(v, pos, sizeof(uint32_t));
  pos += sizeof(uint32_t);
  return true;
}

bool ReadU64(const char*& pos, const char* end, uint64_t* v) {
  if (pos + sizeof(uint64_t) > end)
    return false;
  memcpy(v, pos, sizeof(uint64_t));
  pos += sizeof(uint64_t);
  return true;
}

bool ReadString(const char*& pos, const char* end, std::string* s) {
  uint32_t len;
  if (!ReadU32(pos, end, &len))
    return false;
  if (pos + len > end)
    return false;
  s->assign(pos, len);
  pos += len;
  return true;
}

}  // namespace

std::string SnapshotEncoder::Encode(const SnapshotData& data) {
  std::string out;
  size_t total_size = sizeof(uint32_t);
  for (const auto& entry : data.entries) {
    total_size += sizeof(uint32_t) + entry.key.size();
    total_size += sizeof(uint32_t) + entry.value.size();
    total_size += sizeof(uint64_t);
  }
  out.reserve(total_size);

  WriteU32(&out, data.entries.size());

  for (const auto& entry : data.entries) {
    WriteU32(&out, entry.key.size());
    out.append(entry.key);
    WriteU32(&out, entry.value.size());
    out.append(entry.value);
    WriteU64(&out, entry.expire_ms);
  }

  return out;
}

bool SnapshotDecoder::Decode(std::string_view bytes, SnapshotData* data) {
  const char* pos = bytes.data();
  const char* end = pos + bytes.size();

  uint32_t num_entries;
  if (!ReadU32(pos, end, &num_entries))
    return false;

  data->entries.resize(num_entries);
  for (uint32_t i = 0; i < num_entries; i++) {
    if (!ReadString(pos, end, &data->entries[i].key))
      return false;
    if (!ReadString(pos, end, &data->entries[i].value))
      return false;
    if (!ReadU64(pos, end, &data->entries[i].expire_ms))
      return false;
  }

  return pos == end;
}

bool SnapshotManager::Save(std::string_view path, const SnapshotData& data) {
  std::string encoded = encoder_.Encode(data);

  FILE* file = fopen(path.data(), "wb");
  if (!file) {
    LOG(ERROR) << "Failed to open snapshot file for writing: " << path;
    return false;
  }

  size_t written = fwrite(encoded.data(), 1, encoded.size(), file);
  fclose(file);

  if (written != encoded.size()) {
    LOG(ERROR) << "Failed to write snapshot file: " << path;
    return false;
  }

  return true;
}

bool SnapshotManager::Load(std::string_view path, SnapshotData* data) {
  FILE* file = fopen(path.data(), "rb");
  if (!file) {
    LOG(ERROR) << "Failed to open snapshot file for reading: " << path;
    return false;
  }

  fseek(file, 0, SEEK_END);
  long size = ftell(file);
  if (size <= 0) {
    fclose(file);
    return false;
  }

  fseek(file, 0, SEEK_SET);
  std::string buf(size, '\0');
  size_t n = fread(buf.data(), 1, size, file);
  fclose(file);

  if (n != size_t(size))
    return false;

  return decoder_.Decode(buf, data);
}

}  // namespace dfly
