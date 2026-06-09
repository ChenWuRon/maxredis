// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_storage.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>

#include "base/logging.h"

namespace dfly {

namespace {

constexpr std::string_view kFieldCurrentTerm = "current_term";
constexpr std::string_view kFieldVotedFor = "voted_for";

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

RaftStorage::RaftStorage(std::string path) : path_(std::move(path)) {
}

bool RaftStorage::Load() {
  if (path_.empty()) {
    return true;
  }

  std::string data = ReadFile(path_);
  if (data.empty()) {
    // File doesn't exist or is empty — start fresh.
    return Flush();
  }

  return Deserialize(data);
}

bool RaftStorage::Flush() {
  if (path_.empty()) {
    return true;
  }

  std::string tmp_path = path_ + ".tmp";
  std::string content = Serialize();

  // Ensure parent directory exists.
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

void RaftStorage::set_current_term(Term term) {
  DCHECK_GE(term, current_term_);
  current_term_ = term;
  Flush();
}

void RaftStorage::set_voted_for(NodeId node_id) {
  voted_for_ = std::move(node_id);
  Flush();
}

void RaftStorage::Clear() {
  current_term_ = 0;
  voted_for_.clear();
  Flush();
}

std::string RaftStorage::Serialize() const {
  return "{\"current_term\":" + std::to_string(current_term_) +
         ",\"voted_for\":\"" + EscapeJson(voted_for_) + "\"}\n";
}

bool RaftStorage::Deserialize(const std::string& data) {
  // Minimal JSON parser for: {"current_term":N,"voted_for":"..."}
  auto find_field = [&](const std::string& name) -> size_t {
    auto pos = data.find("\"" + name + "\"");
    if (pos == std::string::npos)
      return std::string::npos;
    auto colon = data.find(':', pos);
    if (colon == std::string::npos)
      return std::string::npos;
    // Skip whitespace after colon
    auto start = colon + 1;
    while (start < data.size() && (data[start] == ' ' || data[start] == '\t'))
      start++;
    return start;
  };

  // Parse current_term
  auto pos = find_field(std::string(kFieldCurrentTerm));
  if (pos != std::string::npos) {
    char* end = nullptr;
    uint64_t val = strtoull(data.c_str() + pos, &end, 10);
    if (end != data.c_str() + pos) {
      current_term_ = val;
    }
  }

  // Parse voted_for (handle escaped quotes)
  pos = find_field(std::string(kFieldVotedFor));
  if (pos != std::string::npos && pos < data.size() && data[pos] == '"') {
    bool escaped = false;
    size_t end_quote = std::string::npos;
    for (size_t i = pos + 1; i < data.size(); i++) {
      if (escaped) {
        escaped = false;
        continue;
      }
      if (data[i] == '\\') {
        escaped = true;
        continue;
      }
      if (data[i] == '"') {
        end_quote = i;
        break;
      }
    }
    if (end_quote != std::string::npos && end_quote > pos + 1) {
      voted_for_ = UnescapeJson(data.substr(pos + 1, end_quote - pos - 1));
    }
  }

  return true;
}

std::string RaftStorage::EscapeJson(const std::string& raw) {
  std::string out;
  out.reserve(raw.size());
  for (char c : raw) {
    switch (c) {
      case '"':  out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n";  break;
      case '\r': out += "\\r";  break;
      case '\t': out += "\\t";  break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
          out += buf;
        } else {
          out += c;
        }
    }
  }
  return out;
}

std::string RaftStorage::UnescapeJson(const std::string& escaped) {
  std::string out;
  out.reserve(escaped.size());
  for (size_t i = 0; i < escaped.size(); i++) {
    if (escaped[i] == '\\' && i + 1 < escaped.size()) {
      switch (escaped[i + 1]) {
        case '"':  out += '"';  i++; break;
        case '\\': out += '\\'; i++; break;
        case 'n':  out += '\n'; i++; break;
        case 'r':  out += '\r'; i++; break;
        case 't':  out += '\t'; i++; break;
        case 'u': {
          if (i + 5 < escaped.size()) {
            char hex[5] = {escaped[i+2], escaped[i+3], escaped[i+4], escaped[i+5], 0};
            char* end = nullptr;
            long cp = strtol(hex, &end, 16);
            if (end == hex + 4 && cp > 0 && cp < 0x80) {
              out += static_cast<char>(cp);
            }
            i += 5;
          }
          break;
        }
        default: out += escaped[i]; break;
      }
    } else {
      out += escaped[i];
    }
  }
  return out;
}

}  // namespace dfly
