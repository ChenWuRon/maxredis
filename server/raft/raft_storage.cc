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
constexpr std::string_view kFieldConfigState = "config_state";
constexpr std::string_view kFieldJointOldVoters = "joint_old_voters";
constexpr std::string_view kFieldJointNewVoters = "joint_new_voters";
constexpr std::string_view kFieldJointOldVersion = "joint_old_version";
constexpr std::string_view kFieldJointNewVersion = "joint_new_version";

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

void RaftStorage::SetState(Term term, const NodeId& voted_for) {
  DCHECK_GE(term, current_term_);
  current_term_ = term;
  voted_for_ = voted_for;
  // Single atomic tmp+fsync+rename: no crash window between "term durable"
  // and "vote durable".
  Flush();
}

void RaftStorage::SetJointConfigState(ConfigState state, const JointConfig& joint) {
  config_state_ = state;
  joint_config_ = joint;
  Flush();
}

void RaftStorage::Clear() {
  current_term_ = 0;
  voted_for_.clear();
  config_state_ = ConfigState::kStable;
  joint_config_ = JointConfig{};
  Flush();
}

std::string RaftStorage::JoinToken(const std::unordered_set<NodeId>& set) {
  std::string result;
  for (const auto& id : set) {
    if (!result.empty())
      result += ',';
    result += EscapeJson(id);
  }
  return result;
}

std::string RaftStorage::Serialize() const {
  return "{\"current_term\":" + std::to_string(current_term_) +
         ",\"voted_for\":\"" + EscapeJson(voted_for_) + "\"" +
         ",\"config_state\":" + std::to_string(static_cast<uint8_t>(config_state_)) +
         ",\"joint_old_voters\":\"" + JoinToken(joint_config_.old_config.voters) + "\"" +
         ",\"joint_new_voters\":\"" + JoinToken(joint_config_.new_config.voters) + "\"" +
         ",\"joint_old_version\":" +
         std::to_string(joint_config_.old_config.version) +
         ",\"joint_new_version\":" +
         std::to_string(joint_config_.new_config.version) + "}\n";
}

bool RaftStorage::Deserialize(const std::string& data) {
  // Minimal JSON parser for the fields we serialize.
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

  auto parse_uint = [&](const std::string& name, uint64_t* out) -> bool {
    auto pos = find_field(name);
    if (pos == std::string::npos)
      return false;
    char* end = nullptr;
    uint64_t val = strtoull(data.c_str() + pos, &end, 10);
    if (end != data.c_str() + pos) {
      *out = val;
      return true;
    }
    return false;
  };

  auto parse_string = [&](const std::string& name, std::string* out) -> bool {
    auto pos = find_field(name);
    if (pos == std::string::npos || pos >= data.size() || data[pos] != '"')
      return false;
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
    if (end_quote == std::string::npos)
      return false;
    *out = UnescapeJson(data.substr(pos + 1, end_quote - pos - 1));
    return true;
  };

  uint64_t v = 0;
  if (parse_uint(std::string(kFieldCurrentTerm), &v))
    current_term_ = v;

  std::string s;
  if (parse_string(std::string(kFieldVotedFor), &s))
    voted_for_ = s;

  if (parse_uint(std::string(kFieldConfigState), &v))
    config_state_ = (v == 0) ? ConfigState::kStable : ConfigState::kJoint;

  auto parse_voters = [&](const std::string& name, std::unordered_set<NodeId>* out) {
    if (!parse_string(name, &s))
      return;
    size_t start = 0;
    while (start < s.size()) {
      size_t comma = s.find(',', start);
      if (comma == std::string::npos) {
        if (!s.substr(start).empty())
          out->insert(UnescapeJson(s.substr(start)));
        break;
      }
      if (comma > start)
        out->insert(UnescapeJson(s.substr(start, comma - start)));
      start = comma + 1;
    }
  };
  parse_voters(std::string(kFieldJointOldVoters), &joint_config_.old_config.voters);
  parse_voters(std::string(kFieldJointNewVoters), &joint_config_.new_config.voters);
  if (parse_uint(std::string(kFieldJointOldVersion), &v))
    joint_config_.old_config.version = v;
  if (parse_uint(std::string(kFieldJointNewVersion), &v))
    joint_config_.new_config.version = v;

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