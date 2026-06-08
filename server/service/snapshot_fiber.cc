// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/service/snapshot_fiber.h"

#include <absl/time/clock.h>

#include "server/service/main_service.h"

namespace dfly {

using namespace std::chrono_literals;

SnapshotFiber::SnapshotFiber(Service* service) : service_(service) {
}

SnapshotFiber::~SnapshotFiber() {
  Stop();
}

void SnapshotFiber::Start(uint32_t time_threshold_sec, uint32_t cmd_threshold) {
  time_threshold_sec_ = time_threshold_sec;
  cmd_threshold_ = cmd_threshold;

  if (time_threshold_sec_ == 0 && cmd_threshold_ == 0)
    return;

  last_snapshot_time_ms_ = absl::GetCurrentTimeNanos() / 1000000;
  fiber_ = util::fb2::Fiber("snapshot_fiber", [this] { Run(); });
}

void SnapshotFiber::Stop() {
  shutdown_.store(true, std::memory_order_relaxed);
  if (fiber_.IsJoinable())
    fiber_.Join();
}

void SnapshotFiber::NotifyWrite() {
  write_count_.fetch_add(1, std::memory_order_release);
}

void SnapshotFiber::Run() {
  while (!shutdown_.load(std::memory_order_relaxed)) {
    uint64_t now = absl::GetCurrentTimeNanos() / 1000000;
    uint32_t cmds = write_count_.load(std::memory_order_acquire);

    bool time_trigger = time_threshold_sec_ > 0 &&
                         (now - last_snapshot_time_ms_) >= time_threshold_sec_ * 1000ULL;
    bool cmd_trigger = cmd_threshold_ > 0 && cmds >= cmd_threshold_;

    if (time_trigger || cmd_trigger) {
      service_->CreateSnapshot();
      last_snapshot_time_ms_ = absl::GetCurrentTimeNanos() / 1000000;
      write_count_.store(0, std::memory_order_release);
    }

    util::ThisFiber::SleepFor(1000ms);
  }
}

}  // namespace dfly
