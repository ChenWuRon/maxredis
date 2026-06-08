// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cstdint>

#include "util/fibers/fibers.h"

namespace dfly {

class Service;

class SnapshotFiber {
 public:
  SnapshotFiber(Service* service);
  ~SnapshotFiber();

  void Start(uint32_t time_threshold_sec, uint32_t cmd_threshold);
  void Stop();
  void NotifyWrite();

 private:
  void Run();

  Service* service_;
  util::fb2::Fiber fiber_;
  std::atomic<bool> shutdown_{false};
  std::atomic<uint32_t> write_count_{0};
  uint32_t time_threshold_sec_ = 0;
  uint32_t cmd_threshold_ = 0;
  uint64_t last_snapshot_time_ms_ = 0;
};

}  // namespace dfly
