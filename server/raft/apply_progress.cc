// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/apply_progress.h"

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

ApplyProgress::ApplyProgress(std::string path) : path_(std::move(path)) {
}

bool ApplyProgress::Load() {
  if (path_.empty()) {
    return true;
  }

  std::string data = ReadFile(path_);
  if (data.empty()) {
    return Flush();
  }

  return Deserialize(data);
}

bool ApplyProgress::Flush() {
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

void ApplyProgress::Update(LogIndex index) {
  DCHECK_GE(index, last_applied_);
  last_applied_ = index;
  Flush();
}

std::string ApplyProgress::Serialize() const {
  return "{\"last_applied\":" + std::to_string(last_applied_) + "}\n";
}

bool ApplyProgress::Deserialize(const std::string& data) {
  auto pos = data.find("\"last_applied\"");
  if (pos == std::string::npos)
    return true;

  auto colon = data.find(':', pos);
  if (colon == std::string::npos)
    return true;

  auto start = colon + 1;
  while (start < data.size() && (data[start] == ' ' || data[start] == '\t'))
    start++;

  char* end = nullptr;
  uint64_t val = strtoull(data.c_str() + start, &end, 10);
  if (end != data.c_str() + start) {
    last_applied_ = val;
  }

  return true;
}

}  // namespace dfly
