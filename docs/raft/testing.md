# Raft Testing Guide

## Test Suites (102 tests total)

| Suite | Tests | Location | Coverage |
|-------|-------|----------|----------|
| `raft_node_test` | 62 | `server/raft/raft_node_test.cc` | Core state machine, role transitions, commit/apply, fast commit path, log replication |
| `election_timer_test` | 25 | `server/raft/election_timer_test.cc` | Timer start/reset/stop, fiber behavior, randomized timeouts, epoch cancellation |
| `raft_role_test` | 12 | `server/raft/raft_role_test.cc` | Role transitions with mock Transport, Follower→Candidate→Leader→Follower |
| `raft_engine_test` | 1 | `server/raft/raft_engine_test.cc` | FastCommitPath single-node |
| `raft_integration_test` | 2 | `server/raft/raft_integration_test.cc` | Full pipeline: CommandEncoder→CommandLog→RaftNode→StateMachine |

## Running Tests

```bash
# Build all raft tests
cmake --build build -j$(nproc) --target raft_node_test \
                                       raft_engine_test \
                                       raft_integration_test \
                                       raft_role_test \
                                       election_timer_test

# Run individually
./build/raft_node_test
./build/raft_engine_test
./build/raft_integration_test
./build/raft_role_test
./build/election_timer_test

# Run all with verbose logging
./build/raft_node_test --v=1
./build/raft_node_test --v=2   # more detailed RPC logs
```

## Test Patterns

### Role Transitions (`raft_node_test.cc`)

Tests verify term, voted_for, and role at each state transition:

```cpp
// Pattern: set up node at known state, trigger transition, check invariants
RaftNode node("N1", ...);
node.SetRole(RaftRole::Follower);

// Trigger election
node.OnElectionTimeout();

EXPECT_EQ(node.role(), RaftRole::Candidate);
EXPECT_EQ(node.term(), node_id + 1);  // term incremented
EXPECT_EQ(node.voted_for(), node.node_id());  // self-vote
```

**Test cases:**
- `FollowerToCandidate` — term bumps, self-vote, role change
- `CandidateToLeader` — majority vote grants leadership
- `LeaderStepDown` — higher term heartbeat forces step-down to Follower

### Commit/Apply Pipeline (`raft_node_test.cc`)

Tests that applied results never skip, re-apply, or lose ordering:

```cpp
// Pattern: define expected apply order, run pipeline, verify
std::vector<int> apply_order;

node.SetOnAppliedCallback([&](const LogEntry& log) {
    apply_order.push_back(log.index);
});

node.AdvanceCommitIndex();
node.ApplyCommittedLogs();
EXPECT_EQ(apply_order, /* expected sequence */);
```

**Test cases:**
- `AdvanceCommitIndex` — commit_index moves correctly
- `ApplyCommittedLogs` — entries applied at most once
- `ApplyOrderPreserved` — strictly sequential in index order

### Transport Tests (`raft_role_test.cc`)

Mock Transport injects controlled RPC responses:

```cpp
raft_role_test.cc:12  // constructs mock transport, nodes, runs role election
```

### ElectionTimer (`election_timer_test.cc`)

Fiber-based asynchronous tests:

```cpp
// Pattern: start timer, wait less than timeout, verify no fire
timer.Start(callback);
ctx.AdvanceTo(now + 100ms);  // < min timeout
EXPECT_FALSE(fired);

// wait past timeout, verify fire
ctx.AdvanceTo(now + 301ms);  // > max timeout
EXPECT_TRUE(fired);
```

**Test cases:**
- `ResetCancelsPreviousEpoch` — concurrent reset skips stale callback
- `StopTerminatesFiber` — Stop() joins fiber cleanly
- `RandomizedTimeoutRange` — 10 samples all within [150, 300]ms

### Integration Tests (`raft_integration_test.cc`)

End-to-end pipeline validation with `InMemoryKV`:

```cpp
// Pattern: full roundtrip from command submit to state machine effect
RaftEngine engine;
engine.Init(scheduler);
auto result = engine.SubmitCommand(&cid, args);
EXPECT_EQ(result.op, ApplyOp::OK);
EXPECT_EQ(kv_storage["a"], "1");
```

**Test cases:**
- `SetCommandApply` — single write → stored correctly
- `MultipleCommandsApply` — sequential writes ordered correctly

## Fixtures

- `server/raft/raft_node_test.cc` — `RaftNodeTest` with `TestScheduler`, `CommandLog`, `TestStateMachine`
- `server/raft/raft_role_test.cc` — bare `RaftNode` + `MockTransport` with `RaftNode::SetTransport()`
- `server/raft/election_timer_test.cc` — `ElectionTimerTest` with `SingleThreadScheduler`
- `server/raft/raft_engine_test.cc` — `RaftEngineTest` with `InMemoryKV`
- `server/raft/raft_integration_test.cc` — `RaftIntegrationTest` with `InMemoryKV`

## Adding Tests

1. Add test method to existing file or create new `*_test.cc`
2. Register in `server/CMakeLists.txt` with `add_raft_test(target_name)`
3. Include the test-specific fixture header

```cmake
# server/CMakeLists.txt
add_raft_test(raft_new_test)
```
