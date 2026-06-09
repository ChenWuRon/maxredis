// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/snapshot_writer.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>

#include "base/logging.h"

namespace dfly {

SnapshotWriter::SnapshotWriter(std::string path)
    : path_(std::move(path)), tmp_path_(path_ + ".tmp") {
}

SnapshotWriter::~SnapshotWriter() {
  if (fp_) {
    fclose(fp_);
    unlink(tmp_path_.c_str());
  }
}

bool SnapshotWriter::Open() {
  auto slash = path_.rfind('/');
  if (slash != std::string::npos) {
    std::string dir = path_.substr(0, slash);
    if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
      PLOG(WARNING) << "mkdir(" << dir << ") failed";
      return false;
    }
  }

  fp_ = fopen(tmp_path_.c_str(), "wb");
  if (!fp_) {
    PLOG(WARNING) << "Failed to open " << tmp_path_ << " for writing";
    return false;
  }

  // Write placeholder header now — we'll overwrite num_records at Finalize.
  uint32_t magic = kSnapshotMagic;
  uint32_t zero = 0;
  if (fwrite(&magic, sizeof(magic), 1, fp_) != 1) {
    PLOG(WARNING) << "Failed to write magic";
    fclose(fp_);
    fp_ = nullptr;
    return false;
  }
  if (fwrite(&zero, sizeof(zero), 1, fp_) != 1) {
    PLOG(WARNING) << "Failed to write placeholder num_records";
    fclose(fp_);
    fp_ = nullptr;
    return false;
  }

  return true;
}

bool SnapshotWriter::Add(const SnapshotRecord& record) {
  if (!fp_)
    return false;

  uint32_t key_len = record.key.size();
  uint32_t value_len = record.value.size();
  uint64_t expire_at = record.expire_at;

  if (fwrite(&key_len, sizeof(key_len), 1, fp_) != 1) {
    PLOG(WARNING) << "Failed to write key_len";
    return false;
  }
  if (fwrite(&value_len, sizeof(value_len), 1, fp_) != 1) {
    PLOG(WARNING) << "Failed to write value_len";
    return false;
  }
  if (fwrite(&expire_at, sizeof(expire_at), 1, fp_) != 1) {
    PLOG(WARNING) << "Failed to write expire_at";
    return false;
  }
  if (key_len > 0 && fwrite(record.key.data(), 1, key_len, fp_) != key_len) {
    PLOG(WARNING) << "Failed to write key data";
    return false;
  }
  if (value_len > 0 && fwrite(record.value.data(), 1, value_len, fp_) != value_len) {
    PLOG(WARNING) << "Failed to write value data";
    return false;
  }

  return true;
}

bool SnapshotWriter::AddBatch(const SnapshotRecord* records, size_t count) {
  for (size_t i = 0; i < count; i++) {
    if (!Add(records[i]))
      return false;
  }
  return true;
}

bool SnapshotWriter::Finalize(uint32_t num_records) {
  if (!fp_)
    return false;

  // Flush all buffered record data to disk before seeking.
  if (fflush(fp_) != 0) {
    PLOG(WARNING) << "fflush failed for " << tmp_path_;
    fclose(fp_);
    fp_ = nullptr;
    unlink(tmp_path_.c_str());
    return false;
  }

  // Seek to byte 4 (right after magic) and overwrite num_records.
  if (fseek(fp_, 4, SEEK_SET) != 0) {
    PLOG(WARNING) << "fseek to byte 4 failed in " << tmp_path_;
    fclose(fp_);
    fp_ = nullptr;
    unlink(tmp_path_.c_str());
    return false;
  }

  if (fwrite(&num_records, sizeof(num_records), 1, fp_) != 1) {
    PLOG(WARNING) << "Failed to write num_records";
    fclose(fp_);
    fp_ = nullptr;
    unlink(tmp_path_.c_str());
    return false;
  }

  // fsync
  if (fflush(fp_) != 0) {
    PLOG(WARNING) << "fflush failed for " << tmp_path_;
    fclose(fp_);
    fp_ = nullptr;
    unlink(tmp_path_.c_str());
    return false;
  }
  if (fdatasync(fileno(fp_)) != 0) {
    PLOG(WARNING) << "fdatasync failed for " << tmp_path_;
    fclose(fp_);
    fp_ = nullptr;
    unlink(tmp_path_.c_str());
    return false;
  }

  if (fclose(fp_) != 0) {
    PLOG(WARNING) << "fclose failed for " << tmp_path_;
    fp_ = nullptr;
    unlink(tmp_path_.c_str());
    return false;
  }
  fp_ = nullptr;

  // rename
  if (rename(tmp_path_.c_str(), path_.c_str()) != 0) {
    PLOG(WARNING) << "rename(" << tmp_path_ << " -> " << path_ << ") failed";
    unlink(tmp_path_.c_str());
    return false;
  }

  return true;
}

}  // namespace dfly
