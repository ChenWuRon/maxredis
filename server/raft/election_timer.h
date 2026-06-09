// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <random>

#include "server/raft/timer.h"
#include "util/fibers/fibers.h"

namespace dfly {

// Implements ITimer with a fiber-based asynchronous wait.
// The timeout is randomly chosen in [150, 300] ms per Raft spec.
class ElectionTimer : public ITimer {
 public:
  ElectionTimer();
  ~ElectionTimer();

  // ITimer interface.
  void Start(std::function<void()> cb) final;
  void Reset() final;
  void Stop() final;

  // Returns whether the timer fiber is running.
  bool IsRunning() const;

  // Returns whether the timer is active (ready to fire on next timeout).
  bool IsActive() const;

  // Deactivates the timer without joining the fiber.
  // Safe to call from within the timer fiber itself.
  void Deactivate();

 private:
  void Run();

  std::function<void()> cb_;
  util::fb2::Fiber fiber_;
  std::atomic<bool> shutdown_{false};
  std::atomic<bool> active_{false};
  std::atomic<uint64_t> epoch_{0};
  std::mt19937 rng_;
  std::uniform_int_distribution<int> dist_{150, 300};
};

}  // namespace dfly
