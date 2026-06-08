// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/election_timer.h"

#include "server/raft/raft_node.h"

namespace dfly {

using namespace std::chrono_literals;

ElectionTimer::ElectionTimer(RaftNode* node)
    : node_(node), rng_(std::random_device{}()) {
}

ElectionTimer::~ElectionTimer() {
  Stop();
}

void ElectionTimer::Start() {
  if (fiber_.IsJoinable())
    return;
  active_.store(true, std::memory_order_release);
  shutdown_.store(false, std::memory_order_release);
  fiber_ = util::fb2::Fiber("election_timer", [this] { Run(); });
}

void ElectionTimer::Reset() {
  active_.store(true, std::memory_order_release);
}

void ElectionTimer::Stop() {
  active_.store(false, std::memory_order_release);
  shutdown_.store(true, std::memory_order_release);
  if (fiber_.IsJoinable())
    fiber_.Join();
}

bool ElectionTimer::IsRunning() const {
  return fiber_.IsJoinable();
}

void ElectionTimer::Run() {
  while (!shutdown_.load(std::memory_order_acquire)) {
    if (!active_.load(std::memory_order_acquire)) {
      util::ThisFiber::SleepFor(10ms);
      continue;
    }

    int timeout_ms = dist_(rng_);
    util::ThisFiber::SleepFor(std::chrono::milliseconds(timeout_ms));

    if (!active_.load(std::memory_order_acquire) || shutdown_.load(std::memory_order_acquire))
      continue;

    active_.store(false, std::memory_order_release);
    node_->OnElectionTimeout();
  }
}

}  // namespace dfly
