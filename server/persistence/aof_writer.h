// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>
#include <string_view>

#include "util/fibers/synchronization.h"

namespace dfly {

class AofWriter {
 public:
  AofWriter();
  ~AofWriter();

  // Opens the AOF file for writing. Returns true on success.
  bool Open(std::string_view path);

  // Appends a RESP-formatted record to the internal buffer.
  void Append(std::string_view record);

  // Flushes the buffered data to disk.
  void Flush();

 private:
  FILE* file_ = nullptr;
  std::string buf_;
  // Guard against concurrent mutation of buf_ from multiple fibers (the server
  // calls RecordCommand from many connection fibers at once). Without this the
  // shared std::string races and corrupts the heap under load.
  util::fb2::Mutex mu_;
};

}  // namespace dfly
