// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/election_timer.h"

namespace dfly {

ElectionTimer::ElectionTimer() : rng_(std::random_device{}()) {
}

ElectionTimer::~ElectionTimer() {
  Stop();
}

void ElectionTimer::Start(std::function<void()> cb) {
  if (fiber_.IsJoinable())
    return;
  cb_ = std::move(cb);
  active_.store(true, std::memory_order_release);
  shutdown_.store(false, std::memory_order_release);
  epoch_.store(1, std::memory_order_release);
  fiber_ = util::fb2::Fiber("election_timer", [this] { Run(); });
}

void ElectionTimer::Reset() {
  active_.store(true, std::memory_order_release);
  epoch_.fetch_add(1, std::memory_order_release);
}

void ElectionTimer::Stop() {
  active_.store(false, std::memory_order_release);
  shutdown_.store(true, std::memory_order_release);
  epoch_.fetch_add(1, std::memory_order_release);
  // Don't join if called from the timer fiber itself.
  if (fiber_.IsJoinable() && !fiber_.IsActive())
    fiber_.Join();
}

bool ElectionTimer::IsRunning() const {
  return fiber_.IsJoinable();
}

bool ElectionTimer::IsActive() const {
  return active_.load(std::memory_order_acquire);
}

void ElectionTimer::Deactivate() {
  active_.store(false, std::memory_order_release);
  epoch_.fetch_add(1, std::memory_order_release);
}

void ElectionTimer::Run() {
  while (!shutdown_.load(std::memory_order_acquire)) {
    uint64_t saved = epoch_.load(std::memory_order_acquire);

    // Fast path for Stop: if not active, sleep briefly and recheck.
    if (!active_.load(std::memory_order_acquire)) {
      util::ThisFiber::SleepFor(std::chrono::milliseconds(10));
      continue;
    }

    int timeout_ms = dist_(rng_);
    util::ThisFiber::SleepFor(std::chrono::milliseconds(timeout_ms));

    if (shutdown_.load(std::memory_order_acquire))
      break;

    // If Reset() was called during sleep, restart the wait.
    if (epoch_.load(std::memory_order_acquire) != saved)
      continue;

    // Timer expired — fire callback.
    // Mark as inactive so the timer won't fire again until Reset().
    active_.store(false, std::memory_order_release);
    if (cb_)
      cb_();
  }
}

}  // namespace dfly
