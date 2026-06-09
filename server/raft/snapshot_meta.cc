// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/snapshot_meta.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>

#include "base/logging.h"

namespace dfly {

namespace {

bool WriteAndFsync(const std::string& tmp_path, const std::string& content) {
  FILE* fp = fopen(tmp_path.c_str(), "w");
  if (!fp) {
    PLOG(WARNING) << "Failed to open " << tmp_path << " for writing";
    return false;
  }

  size_t written = fwrite(content.data(), 1, content.size(), fp);
  if (written != content.size()) {
    PLOG(WARNING) << "Failed to write all bytes to " << tmp_path
                  << " (wrote " << written << " of " << content.size() << ")";
    fclose(fp);
    unlink(tmp_path.c_str());
    return false;
  }

  if (fflush(fp) != 0) {
    PLOG(WARNING) << "fflush failed for " << tmp_path;
    fclose(fp);
    unlink(tmp_path.c_str());
    return false;
  }

  if (fdatasync(fileno(fp)) != 0) {
    PLOG(WARNING) << "fdatasync failed for " << tmp_path;
    fclose(fp);
    unlink(tmp_path.c_str());
    return false;
  }

  if (fclose(fp) != 0) {
    PLOG(WARNING) << "fclose failed for " << tmp_path;
    unlink(tmp_path.c_str());
    return false;
  }

  return true;
}

bool AtomicRename(const std::string& tmp_path, const std::string& target_path) {
  if (rename(tmp_path.c_str(), target_path.c_str()) != 0) {
    PLOG(WARNING) << "rename(" << tmp_path << " -> " << target_path << ") failed";
    unlink(tmp_path.c_str());
    return false;
  }
  return true;
}

std::string ReadFile(const std::string& path) {
  std::ifstream ifs(path);
  if (!ifs.is_open()) {
    return "";
  }
  std::ostringstream oss;
  oss << ifs.rdbuf();
  return oss.str();
}

}  // namespace

SnapshotMetaStorage::SnapshotMetaStorage(std::string path) : path_(std::move(path)) {
}

bool SnapshotMetaStorage::Load() {
  if (path_.empty()) {
    return true;
  }

  std::string data = ReadFile(path_);
  if (data.empty()) {
    return Flush();
  }

  return Deserialize(data);
}

bool SnapshotMetaStorage::Flush() {
  if (path_.empty()) {
    return true;
  }

  std::string tmp_path = path_ + ".tmp";
  std::string content = Serialize();

  auto slash = path_.rfind('/');
  if (slash != std::string::npos) {
    std::string dir = path_.substr(0, slash);
    if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
      PLOG(WARNING) << "mkdir(" << dir << ") failed";
      return false;
    }
  }

  if (!WriteAndFsync(tmp_path, content)) {
    return false;
  }

  return AtomicRename(tmp_path, path_);
}

void SnapshotMetaStorage::SetMeta(SnapshotMeta m) {
  meta_ = m;
  Flush();
}

std::string SnapshotMetaStorage::Serialize() const {
  return "{\"index\":" + std::to_string(meta_.index) +
         ",\"term\":" + std::to_string(meta_.term) +
         ",\"timestamp_ms\":" + std::to_string(meta_.timestamp_ms) + "}\n";
}

bool SnapshotMetaStorage::Deserialize(const std::string& data) {
  auto find_field = [&](const std::string& name) -> uint64_t* {
    auto pos = data.find("\"" + name + "\"");
    if (pos == std::string::npos)
      return nullptr;
    auto colon = data.find(':', pos);
    if (colon == std::string::npos)
      return nullptr;
    auto start = colon + 1;
    while (start < data.size() && (data[start] == ' ' || data[start] == '\t'))
      start++;
    char* end = nullptr;
    uint64_t val = strtoull(data.c_str() + start, &end, 10);
    if (end == data.c_str() + start)
      return nullptr;
    return new uint64_t(val);
  };

  {
    auto pos = data.find("\"index\"");
    if (pos != std::string::npos) {
      auto colon = data.find(':', pos);
      if (colon != std::string::npos) {
        auto start = colon + 1;
        while (start < data.size() && (data[start] == ' ' || data[start] == '\t'))
          start++;
        char* end = nullptr;
        uint64_t val = strtoull(data.c_str() + start, &end, 10);
        if (end != data.c_str() + start) {
          meta_.index = val;
        }
      }
    }
  }

  {
    auto pos = data.find("\"term\"");
    if (pos != std::string::npos) {
      auto colon = data.find(':', pos);
      if (colon != std::string::npos) {
        auto start = colon + 1;
        while (start < data.size() && (data[start] == ' ' || data[start] == '\t'))
          start++;
        char* end = nullptr;
        uint64_t val = strtoull(data.c_str() + start, &end, 10);
        if (end != data.c_str() + start) {
          meta_.term = val;
        }
      }
    }
  }

  {
    auto pos = data.find("\"timestamp_ms\"");
    if (pos != std::string::npos) {
      auto colon = data.find(':', pos);
      if (colon != std::string::npos) {
        auto start = colon + 1;
        while (start < data.size() && (data[start] == ' ' || data[start] == '\t'))
          start++;
        char* end = nullptr;
        uint64_t val = strtoull(data.c_str() + start, &end, 10);
        if (end != data.c_str() + start) {
          meta_.timestamp_ms = val;
        }
      }
    }
  }

  return true;
}

}  // namespace dfly
