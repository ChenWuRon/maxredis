// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/manifest.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <sstream>

namespace dfly {

ManifestManager::ManifestManager(std::string dir) : dir_(std::move(dir)) {
}

bool ManifestManager::Load() {
  // Ensure directory exists.
  if (mkdir(dir_.c_str(), 0755) != 0 && errno != EEXIST) {
    PLOG(WARNING) << "mkdir(" << dir_ << ") failed";
    return false;
  }

  std::string path = ManifestPath();
  std::ifstream ifs(path);
  if (!ifs.is_open()) {
    // File doesn't exist — create with defaults.
    return Save();
  }

  std::ostringstream oss;
  oss << ifs.rdbuf();
  std::string content = oss.str();

  // Parse: {"current_segment": N}
  auto pos = content.find("\"current_segment\"");
  if (pos == std::string::npos)
    return Save();  // malformed, reset

  pos = content.find(':', pos);
  if (pos == std::string::npos)
    return Save();

  pos++;
  while (pos < content.size() && (content[pos] == ' ' || content[pos] == '\t'))
    pos++;

  char* end = nullptr;
  uint64_t val = strtoull(content.c_str() + pos, &end, 10);
  if (end == content.c_str() + pos)
    return Save();

  current_segment_ = static_cast<uint32_t>(val);
  return true;
}

bool ManifestManager::Save() {
  std::string path = ManifestPath();
  std::string tmp_path = path + ".tmp";

  std::string content = "{\"current_segment\":" + std::to_string(current_segment_) + "}\n";

  FILE* fp = fopen(tmp_path.c_str(), "w");
  if (!fp) {
    PLOG(WARNING) << "Failed to open " << tmp_path;
    return false;
  }

  fwrite(content.data(), 1, content.size(), fp);
  fflush(fp);
  fdatasync(fileno(fp));
  fclose(fp);

  if (rename(tmp_path.c_str(), path.c_str()) != 0) {
    PLOG(WARNING) << "rename(" << tmp_path << " -> " << path << ") failed";
    unlink(tmp_path.c_str());
    return false;
  }

  return true;
}

std::string ManifestManager::ManifestPath() const {
  return dir_ + "/manifest.json";
}

}  // namespace dfly
