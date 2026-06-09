// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cstdint>

#include "util/fibers/fibers.h"

namespace dfly {

// Readers-writer lock for snapshot consistency.
// During snapshot (BeginWrite/EndWrite), all concurrent reads (writes to DbSlice)
// are blocked. Multiple reads can proceed simultaneously.
// Uses fiber-friendly yield to avoid busy-wait spinning.
class SnapshotBarrier {
 public:
  // Called by write operations before mutating DbSlice.
  void BeginRead() {
    while (writing_.load(std::memory_order_acquire)) {
      util::ThisFiber::Yield();
    }
    readers_.fetch_add(1, std::memory_order_relaxed);
    // Re-check after increment to handle race with BeginWrite.
    if (writing_.load(std::memory_order_acquire)) {
      readers_.fetch_sub(1, std::memory_order_relaxed);
      while (writing_.load(std::memory_order_acquire)) {
        util::ThisFiber::Yield();
      }
      readers_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  void EndRead() {
    readers_.fetch_sub(1, std::memory_order_release);
  }

  // Called by SaveSnapshot before exporting data.
  // Blocks until all in-flight writes complete.
  // Subsequent writes are blocked until EndWrite.
  void BeginWrite() {
    writing_.store(true, std::memory_order_release);
    while (readers_.load(std::memory_order_acquire) > 0) {
      util::ThisFiber::Yield();
    }
  }

  void EndWrite() {
    writing_.store(false, std::memory_order_release);
  }

 private:
  std::atomic<uint32_t> readers_{0};
  std::atomic<bool> writing_{false};
};

}  // namespace dfly
