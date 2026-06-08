// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "persistence/aof_writer.h"

#include <cstdio>

#include "base/logging.h"

namespace dfly {

AofWriter::AofWriter() {
}

AofWriter::~AofWriter() {
  if (file_) {
    Flush();
    fclose(file_);
  }
}

bool AofWriter::Open(std::string_view path) {
  DCHECK(!file_);

  file_ = fopen(path.data(), "w");
  if (!file_) {
    LOG(ERROR) << "Failed to open AOF file: " << path;
    return false;
  }
  return true;
}

void AofWriter::Append(std::string_view record) {
  DCHECK(file_);
  buf_.append(record.data(), record.size());
}

void AofWriter::Flush() {
  DCHECK(file_);

  if (buf_.empty())
    return;

  size_t written = fwrite(buf_.data(), 1, buf_.size(), file_);
  if (written != buf_.size()) {
    LOG(ERROR) << "Failed to write to AOF file";
  }
  fflush(file_);
  buf_.clear();
}

}  // namespace dfly
