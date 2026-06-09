// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/snapshot_loader.h"

#include <sys/stat.h>

#include <cstdio>

#include "base/logging.h"
#include "server/raft/snapshot_writer.h"

namespace dfly {

SnapshotLoader::SnapshotLoader(std::string dir) : dir_(std::move(dir)) {
  if (!dir_.empty() && dir_.back() != '/')
    dir_ += '/';
  meta_path_ = dir_ + "snapshot.meta";
  bin_path_ = dir_ + "snapshot.bin";
  meta_storage_ = SnapshotMetaStorage(meta_path_);
}

bool SnapshotLoader::ValidateMeta() {
  // Rule 1: snapshot.meta must exist.
  struct stat st;
  if (stat(meta_path_.c_str(), &st) != 0) {
    VLOG(1) << "ValidateMeta: " << meta_path_ << " not found";
    return false;
  }
  return true;
}

bool SnapshotLoader::ValidateBin() {
  // Rule 2: snapshot.bin must exist.
  struct stat st;
  if (stat(bin_path_.c_str(), &st) != 0) {
    VLOG(1) << "ValidateBin: " << bin_path_ << " not found";
    return false;
  }

  // Rule 5: file size > 0.
  if (st.st_size == 0) {
    VLOG(1) << "ValidateBin: " << bin_path_ << " is empty";
    return false;
  }

  // Validate magic number.
  FILE* fp = fopen(bin_path_.c_str(), "rb");
  if (!fp) {
    VLOG(1) << "ValidateBin: cannot open " << bin_path_;
    return false;
  }

  uint32_t magic;
  bool ok = (fread(&magic, sizeof(magic), 1, fp) == 1) &&
            (magic == kSnapshotMagic);
  fclose(fp);

  if (!ok) {
    VLOG(1) << "ValidateBin: bad magic in " << bin_path_;
    return false;
  }

  return true;
}

SnapshotLoadStatus SnapshotLoader::Load(LoadedSnapshot* out) {
  if (!ValidateMeta())
    return SnapshotLoadStatus::NoSnapshot;

  if (!meta_storage_.Load()) {
    LOG(WARNING) << "SnapshotLoader: failed to load " << meta_path_;
    return SnapshotLoadStatus::Corrupted;
  }

  // Rule 3: meta.index > 0.
  if (meta_storage_.meta().index == 0) {
    VLOG(1) << "SnapshotLoader: meta.index == 0";
    return SnapshotLoadStatus::NoSnapshot;
  }

  // Rule 4: meta.term > 0.
  if (meta_storage_.meta().term == 0) {
    VLOG(1) << "SnapshotLoader: meta.term == 0";
    return SnapshotLoadStatus::Corrupted;
  }

  if (!ValidateBin())
    return SnapshotLoadStatus::Corrupted;

  if (out) {
    out->meta = meta_storage_.meta();
    out->bin_path = bin_path_;
  }

  return SnapshotLoadStatus::OK;
}

}  // namespace dfly
