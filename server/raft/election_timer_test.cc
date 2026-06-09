// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/election_timer.h"

#include <gmock/gmock.h>

#include <atomic>

#include "base/gtest.h"
#include "server/raft/raft_node.h"

namespace dfly {

using namespace testing;

class ElectionTimerTest : public Test {
};

// --- RaftNode::OnElectionTimeout unit tests ---

TEST_F(ElectionTimerTest, OnElectionTimeoutFromFollower) {
  RaftNode node;
  EXPECT_EQ(RaftRole::Follower, node.role());
  EXPECT_EQ(0u, node.term());

  node.OnElectionTimeout();
  EXPECT_EQ(RaftRole::Candidate, node.role());
  EXPECT_EQ(1u, node.term());
}

TEST_F(ElectionTimerTest, OnElectionTimeoutFromCandidateNoOp) {
  RaftNode node;
  node.BecomeCandidate();
  EXPECT_EQ(RaftRole::Candidate, node.role());

  node.OnElectionTimeout();
  EXPECT_EQ(RaftRole::Candidate, node.role());
}

TEST_F(ElectionTimerTest, OnElectionTimeoutFromLeaderNoOp) {
  RaftNode node;
  node.BecomeCandidate();
  node.BecomeLeader();
  EXPECT_EQ(RaftRole::Leader, node.role());

  node.OnElectionTimeout();
  EXPECT_EQ(RaftRole::Leader, node.role());
}

// --- ElectionTimer unit tests ---

TEST_F(ElectionTimerTest, InitiallyNotRunning) {
  ElectionTimer timer;
  EXPECT_FALSE(timer.IsRunning());
}

TEST_F(ElectionTimerTest, StartMakesRunning) {
  ElectionTimer timer;
  timer.Start([] {});
  EXPECT_TRUE(timer.IsRunning());
  timer.Stop();
  EXPECT_FALSE(timer.IsRunning());
}

TEST_F(ElectionTimerTest, DoubleStartIsSafe) {
  ElectionTimer timer;
  timer.Start([] {});
  timer.Start([] {});  // should be a no-op
  EXPECT_TRUE(timer.IsRunning());
  timer.Stop();
}

TEST_F(ElectionTimerTest, StopWithoutStartIsSafe) {
  ElectionTimer timer;
  timer.Stop();  // no-op, no crash
}

TEST_F(ElectionTimerTest, ResetWithoutStartIsSafe) {
  ElectionTimer timer;
  timer.Reset();  // no-op, no crash
}

TEST_F(ElectionTimerTest, InitiallyNotActive) {
  ElectionTimer timer;
  EXPECT_FALSE(timer.IsActive());
}

TEST_F(ElectionTimerTest, StartMakesActive) {
  ElectionTimer timer;
  timer.Start([] {});
  EXPECT_TRUE(timer.IsActive());
  timer.Stop();
}

TEST_F(ElectionTimerTest, DeactivateMakesInactive) {
  ElectionTimer timer;
  timer.Start([] {});
  EXPECT_TRUE(timer.IsActive());
  timer.Deactivate();
  EXPECT_FALSE(timer.IsActive());
  // Fiber still running.
  EXPECT_TRUE(timer.IsRunning());
  timer.Stop();
}

TEST_F(ElectionTimerTest, ResetMakesActive) {
  ElectionTimer timer;
  timer.Start([] {});
  timer.Stop();
  EXPECT_FALSE(timer.IsActive());

  timer.Reset();
  EXPECT_TRUE(timer.IsActive());
}

TEST_F(ElectionTimerTest, FireDeactivates) {
  std::atomic<bool> fired{false};

  util::fb2::Fiber f("timer_test", [&fired] {
    ElectionTimer timer;
    timer.Start([&fired] { fired.store(true, std::memory_order_release); });

    // Wait for the timer to fire.
    util::ThisFiber::SleepFor(std::chrono::milliseconds(500));

    // After firing, the timer should NOT be active.
    EXPECT_FALSE(timer.IsActive());
    // But the fiber is still running.
    EXPECT_TRUE(timer.IsRunning());

    timer.Stop();
  });
  f.Join();

  EXPECT_TRUE(fired.load(std::memory_order_acquire));
}

// --- Timer callback tests (run inside a fiber for cooperative scheduling) ---

TEST_F(ElectionTimerTest, TimerTriggersCallback) {
  std::atomic<int> fire_count{0};

  util::fb2::Fiber test_fiber("timer_test", [&fire_count] {
    ElectionTimer timer;
    timer.Start([&fire_count] { fire_count.fetch_add(1, std::memory_order_release); });

    // Wait long enough for the timer to fire (max timeout 300ms, add margin).
    util::ThisFiber::SleepFor(std::chrono::milliseconds(500));
    timer.Stop();
  });
  test_fiber.Join();

  EXPECT_GE(fire_count.load(std::memory_order_acquire), 1);
}

TEST_F(ElectionTimerTest, StopPreventsCallback) {
  std::atomic<bool> fired{false};

  util::fb2::Fiber test_fiber("timer_test", [&fired] {
    ElectionTimer timer;
    timer.Start([&fired] { fired.store(true, std::memory_order_release); });

    // Stop immediately — the fiber should exit without firing.
    timer.Stop();
  });
  test_fiber.Join();

  EXPECT_FALSE(fired.load(std::memory_order_acquire));
}

TEST_F(ElectionTimerTest, ResetDefersTheTimeout) {
  std::atomic<int> fire_count{0};

  util::fb2::Fiber test_fiber("timer_test", [&fire_count] {
    ElectionTimer timer;
    timer.Start([&fire_count] { fire_count.fetch_add(1, std::memory_order_release); });

    // Wait a bit, reset, wait, reset, then wait for final timeout.
    // Use generous margin: worst case is 300ms + 300ms = 600ms total sleep,
    // so 500ms from last reset (200ms into the test) = 700ms total gives 100ms slack.
    util::ThisFiber::SleepFor(std::chrono::milliseconds(100));
    timer.Reset();
    util::ThisFiber::SleepFor(std::chrono::milliseconds(100));
    timer.Reset();
    util::ThisFiber::SleepFor(std::chrono::milliseconds(500));

    timer.Stop();
  });
  test_fiber.Join();

  EXPECT_EQ(1, fire_count.load(std::memory_order_acquire));
}

TEST_F(ElectionTimerTest, StartAfterStopRestarts) {
  std::atomic<int> fire_count{0};

  util::fb2::Fiber test_fiber("timer_test", [&fire_count] {
    ElectionTimer timer;

    // First cycle.
    timer.Start([&fire_count] { fire_count.fetch_add(1, std::memory_order_release); });
    util::ThisFiber::SleepFor(std::chrono::milliseconds(400));
    timer.Stop();

    EXPECT_EQ(1, fire_count.load(std::memory_order_acquire));

    // Second cycle — restart with same timer.
    fire_count.store(0, std::memory_order_release);
    timer.Start([&fire_count] { fire_count.fetch_add(1, std::memory_order_release); });
    util::ThisFiber::SleepFor(std::chrono::milliseconds(400));
    timer.Stop();

    EXPECT_EQ(1, fire_count.load(std::memory_order_acquire));
  });
  test_fiber.Join();
}

// --- RaftNode + ElectionTimer integration tests ---

TEST_F(ElectionTimerTest, BecomeFollowerResetsTimer) {
  RaftNode node("n1");
  EXPECT_FALSE(node.election_timer().IsRunning());

  // First BecomeFollower should lazy-start the timer.
  node.BecomeFollower(1);
  EXPECT_TRUE(node.election_timer().IsRunning());
  EXPECT_TRUE(node.election_timer().IsActive());

  // Second BecomeFollower should reset it (still running and active).
  node.BecomeFollower(2);
  EXPECT_TRUE(node.election_timer().IsRunning());
  EXPECT_TRUE(node.election_timer().IsActive());
}

TEST_F(ElectionTimerTest, BecomeCandidateDeactivatesTimer) {
  RaftNode node("n1");
  node.BecomeFollower(1);
  EXPECT_TRUE(node.election_timer().IsRunning());
  EXPECT_TRUE(node.election_timer().IsActive());

  node.BecomeCandidate();
  // The timer fiber should still be running (no self-join crash).
  EXPECT_TRUE(node.election_timer().IsRunning());
  // But should be deactivated (won't fire until Reset).
  EXPECT_FALSE(node.election_timer().IsActive());
}

TEST_F(ElectionTimerTest, BecomeLeaderDeactivatesTimer) {
  RaftNode node("n1");
  node.BecomeFollower(1);
  EXPECT_TRUE(node.election_timer().IsRunning());
  EXPECT_TRUE(node.election_timer().IsActive());

  node.BecomeCandidate();
  node.BecomeLeader();
  // Timer fiber still running, but inactive.
  EXPECT_TRUE(node.election_timer().IsRunning());
  EXPECT_FALSE(node.election_timer().IsActive());
}

TEST_F(ElectionTimerTest, FollowerTimerTriggersElection) {
  util::fb2::Fiber test_fiber("timer_test", [] {
    RaftNode node("n1");
    node.BecomeFollower(5);
    EXPECT_EQ(RaftRole::Follower, node.role());
    EXPECT_EQ(5u, node.term());

    // Wait for the election timer to fire (max 300ms + margin).
    util::ThisFiber::SleepFor(std::chrono::milliseconds(500));

    // Single-node cluster: self-vote reaches majority, becomes Leader.
    EXPECT_EQ(RaftRole::Leader, node.role());
    EXPECT_EQ(6u, node.term());
  });
  test_fiber.Join();
}

TEST_F(ElectionTimerTest, HeartbeatsResetElectionTimer) {
  util::fb2::Fiber test_fiber("timer_test", [] {
    RaftNode node("n1");
    node.BecomeFollower(5);

    // Send heartbeats repeatedly to reset the timer.
    for (int i = 0; i < 5; i++) {
      HeartbeatRequest req{5, "Leader"};
      node.OnHeartbeat(req);
      util::ThisFiber::SleepFor(std::chrono::milliseconds(50));
    }

    // After the last heartbeat, wait a bit (but not enough for timer to fire).
    util::ThisFiber::SleepFor(std::chrono::milliseconds(100));

    // Since we kept resetting, the timer should not have fired yet.
    EXPECT_EQ(RaftRole::Follower, node.role());

    // Now wait long enough for the timer to fire after the last reset.
    util::ThisFiber::SleepFor(std::chrono::milliseconds(400));

    // Single-node cluster becomes Leader, not Candidate.
    EXPECT_EQ(RaftRole::Leader, node.role());
  });
  test_fiber.Join();
}

}  // namespace dfly
