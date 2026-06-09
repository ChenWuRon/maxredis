// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>

namespace dfly {

class ITimer {
 public:
  virtual ~ITimer() = default;

  // Starts the timer with the given callback.
  // The callback is invoked when the timer fires.
  // If the timer is already running, this is a no-op.
  virtual void Start(std::function<void()> cb) = 0;

  // Resets the timer, re-randomizing the timeout.
  // The callback will not fire until the new timeout elapses.
  virtual void Reset() = 0;

  // Stops the timer. The callback will not be invoked.
  virtual void Stop() = 0;
};

}  // namespace dfly
