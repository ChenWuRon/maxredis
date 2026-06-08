// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>
#include <string_view>

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
};

}  // namespace dfly
