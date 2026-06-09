// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/wal_writer.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>

#include "base/logging.h"

namespace dfly {

namespace {

constexpr size_t kHeaderSize = sizeof(RecordHeader);

}  // namespace

WalWriter::~WalWriter() {
  if (file_) {
    Close();
  }
}

bool WalWriter::Open(const std::string& path) {
  if (file_) {
    LOG(WARNING) << "WalWriter already open";
    return false;
  }

  // Ensure parent directory exists.
  auto slash = path.rfind('/');
  if (slash != std::string::npos) {
    std::string dir = path.substr(0, slash);
    if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
      PLOG(WARNING) << "mkdir(" << dir << ") failed";
      return false;
    }
  }

  file_ = fopen(path.c_str(), "wb");
  if (!file_) {
    PLOG(WARNING) << "Failed to open " << path << " for writing";
    return false;
  }

  file_size_ = 0;
  buf_.clear();
  VLOG(1) << "WalWriter opened " << path;
  return true;
}

void WalWriter::Append(const LogEntry& entry) {
  DCHECK(file_) << "WalWriter must be opened before Append";

  RecordHeader header;
  header.index = entry.index;
  header.term = entry.term;
  header.size = static_cast<uint32_t>(entry.command.size());

  buf_.append(reinterpret_cast<const char*>(&header), kHeaderSize);
  buf_.append(entry.command.data(), entry.command.size());
}

bool WalWriter::Flush() {
  if (!file_)
    return false;
  if (buf_.empty())
    return true;

  size_t written = fwrite(buf_.data(), 1, buf_.size(), file_);
  if (written != buf_.size()) {
    PLOG(WARNING) << "fwrite failed: wrote " << written << " of " << buf_.size();
    return false;
  }

  file_size_ += written;

  if (fflush(file_) != 0) {
    PLOG(WARNING) << "fflush failed";
    return false;
  }

  if (fdatasync(fileno(file_)) != 0) {
    PLOG(WARNING) << "fdatasync failed";
    return false;
  }

  buf_.clear();
  return true;
}

void WalWriter::Close() {
  if (!file_)
    return;

  Flush();
  fclose(file_);
  file_ = nullptr;
  file_size_ = 0;
  buf_.clear();
}

}  // namespace dfly
