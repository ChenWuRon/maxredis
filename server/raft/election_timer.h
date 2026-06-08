// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cstdint>
#include <random>

#include "util/fibers/fibers.h"

namespace dfly {

class RaftNode;

class ElectionTimer {
 public:
  explicit ElectionTimer(RaftNode* node);
  ~ElectionTimer();

  void Start();
  void Reset();
  void Stop();

  bool IsRunning() const;

 private:
  void Run();

  RaftNode* node_;
  util::fb2::Fiber fiber_;
  std::atomic<bool> shutdown_{false};
  std::atomic<bool> active_{false};
  std::mt19937 rng_;
  std::uniform_int_distribution<int> dist_{150, 300};
};

}  // namespace dfly
