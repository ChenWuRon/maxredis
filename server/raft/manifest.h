// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>

#include "base/logging.h"

namespace dfly {

// Manages a manifest.json file that tracks the current WAL segment ID.
// The manifest is stored at {dir}/manifest.json.
// Format: {"current_segment": N}
class ManifestManager {
 public:
  explicit ManifestManager(std::string dir);

  // Loads manifest from disk. If file doesn't exist, creates it with segment=0.
  // Returns true on success.
  bool Load();

  // Saves current state to disk atomically (write + rename).
  bool Save();

  uint32_t current_segment() const {
    return current_segment_;
  }

  void set_current_segment(uint32_t segment) {
    current_segment_ = segment;
  }

  const std::string& dir() const {
    return dir_;
  }

 private:
  std::string ManifestPath() const;

  std::string dir_;
  uint32_t current_segment_ = 0;
};

}  // namespace dfly
