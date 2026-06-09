# Data Flow: `SET a 1`

This document traces a `SET a 1` command through the entire Raft pipeline.

## Pipeline

```
Client: SET a 1
    │
    ▼ 1. CommandEncoder
ReplicatedCommand{ SET, ["SET","a","1"] }
    │
    ▼ 2. Serialize
"SET a 1"
    │
    ▼ 3. LogEntry → CommandLog::Append
[1] {term=1, index=1, command="SET a 1"}
    │
    ▼ 4. RaftNode::ReplicateLog()  (single-node path)
    │
    ├─ PeerCount() == 0 → fast path
    │
    ▼ 5. RaftNode::AdvanceCommitIndex()
commit_index: 0 → 1
    │
    ▼ 6. RaftNode::ApplyCommittedLogs()
    │
    ├─ last_applied: 0 → 1
    │
    ▼ 7. IStateMachine::ApplyLogEntry(entry)
    │
    ├─ Parse "SET a 1" → key="a", value="1"
    │
    ▼ 8. IStateMachine::Set("a", "1")
kv["a"] = "1" ✅
```

## Detailed Step-by-Step

### 1. Submit from Redis

`RaftEngine::SubmitCommand()` is called with the CommandId for SET and `["SET", "a", "1"]` as args.

### 2. Encode

`CommandEncoder::Encode()` maps the CommandId + args to a `ReplicatedCommand`:

```cpp
ReplicatedCommand{ type=SET, args=["SET","a","1"] }
```

If encoding fails (e.g. read-only command), falls through to `KvStateMachine::Apply()` for direct execution.

### 3. Role Check

```cpp
if (group_.node().role() != RaftRole::Leader)
    return {ApplyOp::ERROR, 0};
```

Non-leader nodes reject write commands.

### 4. Single-Node Fast Path

```cpp
if (group_.node().peer_manager().PeerCount() == 0)
    return FastCommitPath(*cmd);
```

No peers → skip Transport and commit directly.

### 5. FastCommitPath

```cpp
LogEntry entry(term, 0, "SET a 1");
log_.Append(entry);
```

Entry stored at index 1 with current term.

### 6. AdvanceCommitIndex

```cpp
// Single-node: 1 node, majority = 1
indexes = [1]  // LastIndex
candidate = indexes[0] = 1
1 > commit_index(0) → commit_index = 1
```

### 7. ApplyCommittedLogs

```cpp
while (last_applied(0) < commit_index(1) && ...) {
    last_applied_++;  // 1
    entry = log_storage_->Get(1);  // "SET a 1"
    state_machine_->ApplyLogEntry(entry);
}
```

### 8. StateMachine::ApplyLogEntry

```cpp
string_view cmd = "SET a 1";
name = "SET", key = "a", val = "1";
Set(0, "a", "1");   // store
return {ApplyOp::OK, 1};
```

### 9. Return Result

```cpp
ApplyResult{ op=OK, affected_rows=1 }
```

## Multi-Node Path (future)

With peers present, Step 4 instead calls `ReplicateLog()`:

```
4. RaftNode::ReplicateLog()
    │
    ├─ Build AppendEntriesRequest with all log entries
    │
    ├─ For each peer:
    │   transport_->SendAppendEntries(peer_id, req)
    │       → LocalTransport → peer->OnAppendEntries(req)
    │           → peer validates + appends + responds
    │
    ├─ AdvanceCommitIndex()  (majority-based)
    │
    └─ ApplyCommittedLogs()
```

## Log Output (VLOG(1))

```
RaftNode N1 term=1: Follower -> Candidate          (SetRole)
N1 starts election term=1 last_log=0/0             (StartElection)
N1 received VoteGranted from N2                    (StartElection)
N1 election won term=1                             (TryBecomeLeader)
RaftNode N1 term=1: Candidate -> Leader            (SetRole)
SubmitCommand: SET a 1                             (SubmitCommand)
FastCommitPath: appended SET a 1 log_size=1        (FastCommitPath)
N1 commit_index 0 -> 1                             (AdvanceCommitIndex)
N1 apply[1] term=1 cmd=SET a 1                     (ApplyCommittedLogs)
```
