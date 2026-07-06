# Raft 共识算法面试准备（结合本项目源码）

> 项目描述："基于 Raft 共识算法实现分布式 KV 存储系统，完成 Leader Election、Log Replication、ReadIndex、Snapshot、Joint Consensus 等核心功能，实现数据强一致性。"
>
> 本文所有分析对应到具体文件与行号，不脱离实现讲理论。

---

## 0. 项目真实定位（面试前必读）

本项目 Raft **不是** etcd/braft 那种"全异步、Pipeline、多 in-flight、批量提交"的工业级实现，而是**同步 RPC、单 fiber 串行驱动**的准生产实现。核心证据（`server/raft/raft_node.cc:756-762`）：
>
```cpp
for (size_t i = 0; i < peer_ids.size(); i++) {
  AppendEntriesResponse rsp = transport_->SendAppendEntries(peer_ids[i], req);  // 同步阻塞
  peer_last_log_index_[i] = rsp.last_log_index;
}
AdvanceCommitIndex();
```

`SendAppendEntries` 同步返回（`transport.h:26`、`local_transport.cc:40`）→ 复制是串行请求-响应式，非 pipeline。
### 0.1 这句话到底在说什么（零背景通俗解释）

**① 同步 RPC**
RPC = 远程过程调用，即"发请求让另一台机器干活"，这里是 Leader 让 Follower 存日志。**同步**指 `AppendEntriesResponse rsp = SendAppendEntries(...)` 这一行**会卡住，一直等 Follower 回复了才继续**——因为要拿到 `rsp`，程序必须停在这行等对方回话。

```
同步（你的实现）              异步（etcd/braft）
Leader → peer[0] 等回复       Leader → peer[0]（不等）
Leader → peer[1] 等回复            → peer[1]（不等）
Leader → peer[2] 等回复            → peer[2]（不等）
（一个个排队）                回头一起收结果
```

**② 串行（for 循环）**
你用 `for` 循环，一个 peer 一个 peer 地发，发完等回复再发下一个。假设 4 个 follower、每次网络往返(RTT) 10ms：
- 你的实现（串行同步）：10+10+10+10 = **40ms**
- 异步并发：4 个同时发，最慢的回来即可 ≈ **10ms**

peer 越多越慢，因为是排队处理。

**③ Pipeline / 多 in-flight**
in-flight = "在途"，指已发出、还没收到回复的请求数。
- 你的实现：任何时刻**最多 1 个** in-flight（发一个必须等它回来）。
- Pipeline（流水线）：不等回复，连续把多批日志都发出去，允许**很多个 in-flight 同时存在**。

好比寄快递——你：寄一个蹲着等送达再寄下一个；Pipeline：一口气投 10 个，谁到谁通知。

**④ 批量提交(batch commit)**
把多条日志攒成一批一起复制/确认/提交，减少 RPC 和 fsync 次数。
> 你在 **apply 环节**有批处理（`raft_node.cc:856` `kBatchSize=128`），但**复制发送环节**没 pipeline，所以是"部分批量"。

**⑤ 单 fiber 串行驱动**
fiber(协程) = 比线程更轻量的执行流，程序自己调度，无需 OS 切线程。**单 fiber 串行驱动** = 选举/复制/提交/apply 基本都跑在**一条执行流**上，一件做完才做下一件。
- 好处：不用加锁、无数据竞争 bug（README "No mutexes"）。
- 代价：这条执行流被"同步等待"卡住时（等 Follower 回复），整个 Raft 都停着 → "一个 peer 网络卡住会阻塞整个选举/复制 fiber"。

**一句话**：你的 Raft 复制是"发一个、等一个、再发下一个"的排队模式，而非工业级"一股脑全发、谁回算谁"的流水线。前者简单可靠但慢，后者快但复杂。

### 0.2 答题策略
诚实承认同步模型 + 说清瓶颈与优化方向，比背八股更像真正项目作者。必须主动坦白/讲清的点：
1. **复制是同步的**，无 Pipeline（最大瓶颈，优化方向）。
2. **`AdvanceCommitIndex` 的 Figure 8 当前 term 检查已补全**（`raft_node.cc` `AdvanceCommitIndex` / `AdvanceCommitIndexJoint`）：只有 `GetTerm(candidate) >= current_term` 才推进 commit_index，拒绝靠副本计数提交旧 term entry。这是能讲"我识别到 Figure 8 隐患并修复"的加分项。

标准话术：
> "我的复制是**同步 RPC + for 循环串行**发送，任何时刻只有一个 in-flight。peer 多或网络延迟高时是瓶颈。工业级实现（etcd/braft）用异步 Pipeline 让多个请求在途，这是我识别到的下一步优化方向。"
**答题策略**：诚实承认同步模型 + 说清瓶颈与优化方向，比背八股更像真正项目作者。两个必须主动坦白的点：
1. **复制是同步的**，无 Pipeline（最大瓶颈）。
2. **`AdvanceCommitIndex` 缺 Figure 8 的当前 term 检查**（安全性简化）。

---

## 1. 整体架构

### 1.1 真实分层

```
redis-cli (RESP)
    │
    ▼
Service (main_service.cc)          ← 命令分发、RESP 编解码、AOF
    │  engine_.SubmitCommand / engine_.Get / engine_.Schedule
    ▼
RaftEngine (raft_engine.cc)        ← 写路径入口：判断 Leader、编码命令、快/慢路径
    │  group_.node()
    ▼
RaftNode (raft_node.cc, 901行)     ← Raft 核心：选举/复制/提交/ReadIndex/Joint
    │  log_storage_          │ state_machine_
    ▼                        ▼
ILogStorage                 IStateMachine (KvStateMachine)
(CommandLog /               (kv_state_machine.cc)
 SegmentLogStorage)              │  shard_set_->Await(sid, ...)
    │  WAL 落盘                   ▼
    ▼                        EngineShardSet → DbSlice (真正的 KV 哈希表)
segment_*.log
```

源码锚点：
- `Service::Set` → `engine_.SubmitCommand`（`main_service.cc:292`）
- `RaftEngine::SubmitCommand` → 编码 + `ReplicateLog()`（`raft_engine.cc:21-43`）
- `RaftNode::ReplicateLog` → AppendEntries + `AdvanceCommitIndex` + `ApplyCommittedLogs`（`raft_node.cc:699-766`）
- `ApplyCommittedLogs` → `state_machine_->ApplyLogEntry`（`raft_node.cc:891`）
- `KvStateMachine::Set` → `shard_set_->Await(sid, ...)` 写 DbSlice（`kv_state_machine.cc:49-60`）

### 1.2 各模块职责

| 模块 | 职责 | 关键代码 |
|---|---|---|
| Service | RESP 协议、命令注册、AOF、快照定时 | `RegisterCommands()` `main_service.cc:517` |
| RaftEngine | 写网关：非 Leader 拒绝、单节点 FastCommitPath、多节点 ReplicateLog | `raft_engine.cc:28-42` |
| RaftNode | 角色状态机、选举、复制、commit、apply、ReadIndex、Joint | 全文 |
| RaftStorage | 持久化 current_term + voted_for（fsync + 原子 rename） | `raft_storage.cc:128-137` |
| ILogStorage | Raft 日志抽象，1-indexed，sentinel[0] | `log_storage.h` |
| KvStateMachine | 命令 apply 到分片 KV，Save/LoadSnapshot | `kv_state_machine.cc` |
| DbSlice/EngineShard | 真正哈希表存储，按 shard 分片 | `Shard(key, size)` |

### 1.3 为什么这样分层
"接口隔离 + 依赖倒置"：`RaftNode` 只依赖 `ILogStorage`（`log_storage.h:17`）与 `IStateMachine`（`state_machine.h:32`）两个纯虚接口，带来：
1. **可测试性**：单测用 `CommandLog` + `InMemoryKV` 替换真实存储（`raft_integration_test.cc:25`）。
2. **存储可插拔**：`CommandLog`（内存）/ `SegmentLogStorage`（WAL）同接口。
3. **共识与状态机解耦**：Raft 只管"多数派复制并提交"，KV 语义交给状态机。

### 1.4 写为什么必须经 Raft
`RaftEngine::SubmitCommand`（`raft_engine.cc:28-42`）：只有 `CO::WRITE` 命令编码成日志；非 Leader 返回 `ERROR` → `READONLY`（`main_service.cc:295`）；写必须先 Append 日志、复制多数派、提交后才 apply。只有两阶段才能保证 Leader 崩溃后新 Leader 一定含已提交写（Leader Completeness），实现强一致。

### 1.5 线性一致性（Linearizability）是什么

线性一致性 = 系统表现得像只有**一台机器**在处理所有请求，执行结果与全局的**真实时间顺序**一致。

**写一致性**：客户端发来 `SET x 1`，只要 Leader 返回 OK，意味着：
- 这条日志已被**多数派节点**复制（`AdvanceCommitIndex`，`raft_node.cc:786`）
- 已被**成功 apply** 到状态机（`ApplyCommittedLogs`，`raft_node.cc:899`）
- 之后**任何节点**被选为新 Leader，这条写一定在其日志里（Leader Completeness）
→ 不会出现"以为写成功、换 Leader 后数据丢失"。

**读一致性**：客户端发来 `GET x`，返回的必须是"此刻起之后所有可能的写结果"。如果刚 SET 完立刻 GET，必须读到最新值，不能读到旧值。这就是为什么需要 ReadIndex。

**简单类比**：
```
时间线：  ──SET x=1──(返回OK)──GET x──
                          ↑
                   如果此时读到 x=null，就是违反线性一致性
```
要保证读到的一定是 1，GET 必须确认"我读的节点确实是最新状态"。

### 1.6 本项目如何保证线性一致性

**写路径**：
1. 客户端写 → Leader 本地追加 LogStorage
2. `ReplicateLog()` 向所有 Follower 同步发 AppendEntries（`raft_node.cc:717`）
3. `AdvanceCommitIndex()` 等多数派的 `last_log_index >= 当前 index`（`raft_node.cc:786-838`）
4. `ApplyCommittedLogs()` 批量 apply，按 index 顺序执行（`raft_node.cc:899`）
5. 返回 OK

**读路径**（`Service::Get`，`main_service.cc:334-361`）：
```cpp
// 默认 Local 读（弱一致，可能读到过期值）
// 开启 --linearizable_read 后走 ReadIndex（强一致）

if (absl::GetFlag(FLAGS_linearizable_read)) {
    LogIndex ri = engine_.ReadIndex();   // ① 确认 Leader 身份
    if (ri == 0) {                       // ② 退位或不够 quorum → 返回错误
        return cntx->SendError("READONLY ...");
    }
    // ③ ReadIndex 内部调了 WaitForApplied(ri)
    // 确保本节点已 apply 到 read_index
}
// ④ 从本地 DbSlice 读数据（已是最新）
engine_.Schedule(0, key, cb);  // → es->db_slice.Find(key)
```

⚠️ **关键事实**：
- `FLAGS_linearizable_read` **默认为 false**，GET 直接读本地 DbSlice（弱一致但快）。
- 开启后，`Service::Get` 先调 `engine_.ReadIndex()` 做 Leader 身份确认 + WaitForApplied，再读本地数据。
- 读的不走 Raft 日志复制，只确认"我此刻还是合法 Leader 且数据已是最新"。
- 写一定是强一致的（多数派复制后才返回），读的一致性由 flag 控制。

**回答模板**：
> "六层：Service→RaftEngine→RaftNode→LogStorage/StateMachine（纯虚接口解耦）→DbSlice。写必须经 SubmitCommand 编码、日志追加、多数派复制、AdvanceCommitIndex、ApplyCommittedLogs 五步后返回，保证 Leader Completeness。读默认 kLocal 直接查本地分片，最快但允许读到旧值；开启 linearizable_read 后走 ReadIndex 协议：发 ReadIndex RPC 确认 quorum 仍认我当 Leader + WaitForApplied 确保本地已应用所有已提交日志，再查本地分片，实现强一致读。"

**追问**：为什么 Engine/Node 分开？（Node 纯共识可复用，Engine 负责 KV 命令编码与快慢路径）；单节点为何有 FastCommitPath？（`PeerCount==0` 无需复制，省一轮 RPC，`raft_engine.cc:45-53`）；为什么读不写日志？（读不改状态机，写日志没有意义，ReadIndex 等价于"确认我还活着"）

#### Peer 是什么

Peer = **集群里除自己以外的其他节点**。你是 A 节点，集群里还有 B、C、D，那 B、C、D 就是你的 peers。

在代码里 `GetPeerIds()`（`raft_node.cc:81`）做的事就是从配置中排除自己：

```cpp
std::vector<NodeId> RaftNode::GetPeerIds() const {
    for (const auto& v : cluster_config_.voters) {
        if (v != node_id_)              // 不是我，才是 peer
            result.push_back(v);
    }
    return result;
}
```

**哪些场景用到 peers**：

| 场景 | 操作 | 源码 |
|------|------|------|
| 复制日志 | Leader 遍历所有 peer，逐个发 AppendEntries | `ReplicateLog()` `raft_node.cc:774` |
| 发心跳 | Leader 遍历所有 peer，逐个发 Heartbeat | `SendHeartbeatToPeers()` `raft_node.cc:380` |
| 选举拉票 | Candidate 遍历所有 peer，逐个发 VoteRequest | `StartElection()` `raft_node.cc:271` |
| ReadIndex | Leader 遍历所有 peer，确认 quorum 认不认识自己 | `ReadIndex()` `raft_node.cc:457` |

**peer 数量决定了单节点还是多节点**（`raft_engine.cc:35`）：

```cpp
if (group_.node().peer_manager().PeerCount() == 0) {
    return FastCommitPath(*cmd);   // 没有 peer = 单节点，跳过复制
}
// 有 peer = 多节点，走完整 Raft 复制
```

> peer 和 voter 的区别：voter 包含自己 + 所有 peers。`cluster_config_.voters` 是所有有投票权节点的集合，`self + peers = voters`。

#### Quorum 是什么

Quorum = **多数派** = `N / 2 + 1`（向下取整加 1）。Raft 所有关键决策都依赖 quorum，是本项目的基石概念。

| 场景 | Quorum 公式 | 3 节点 | 5 节点 | 源码 |
|------|------------|--------|--------|------|
| **选举** | `voters+1 的总数的过半` | 2/3 | 3/5 | `raft_node.cc:316` |
| **提交** | `voters+1 的总数的过半` | 2/3 | 3/5 | `raft_node.cc:805-811` |
| **ReadIndex** | `voters+1 的总数的过半` | 2/3 | 3/5 | `raft_node.cc:454-455` |

**为什么 quorum 能防止脑裂（Split Brain）**：

Raft 在同一 term 内最多只有一个 Leader。候选者当选需要拿到 **quorum 张票**。假设 5 节点：

```
A 想当 Leader → 至少需要 3 票
B 想当 Leader → 至少也需要 3 票
3 + 3 = 6 > 5（总节点数）
```

任意两个 quorum **必然有交集**。而 Raft 规定一个节点在同一个 term 里**只能投一票**（`raft_node.cc:222-226`）：

```cpp
const NodeId& voted = storage_.voted_for();
if (!voted.empty() && voted != request.candidate_id) {
    return {..., false};  // 已经投过别人了，拒绝
}
```

所以 A 和 B **不可能同时拿到 quorum** → 不可能同时存在两个 Leader → 脑裂不会发生。

**为什么多数派提交才安全**：

```
日志:  [1] [2] [3] [4]
节点A: ✅ ✅ ✅   ← Leader 把 index 3 复制到 B 后就 commit 了
节点B: ✅ ✅ ✅
节点C: ✅

如果 Leader(A) 挂了，B 和 C 重新选举：
- B 的日志比 C 长 → B 更容易拿到选票
- 新 Leader B 的日志必然包含已被旧 Leader commit 的 index 3
→ "commit 过的数据绝对不会丢"
```

**核心代码 — 多数派提交计算**（`raft_node.cc:786-811`）：
```cpp
// 收集所有节点的 last_log_index（自己 + 所有 peer）
std::vector<LogIndex> indexes;
indexes.push_back(log_storage_->LastIndex());  // 自己
for (auto idx : peer_last_log_index_)
    indexes.push_back(idx);                     // 各 peer

std::sort(indexes.rbegin(), indexes.rend());    // 降序排列
size_t majority = total / 2 + 1;                // N/2+1
LogIndex candidate = indexes[majority - 1];     // 第 majority 大的值

// Figure 8 安全：只允许 current term 的条目推进 commit
if (candidate > commit_index_ && 
    candidate_term != 0 && candidate_term >= storage_.current_term()) {
    commit_index_ = candidate;
}
```

> **理解要点**：`indexes[majority - 1]` 的含义是"至少有 majority 个节点的日志已经到了这个位置或更远"。因为排序后取第 majority 大的值，意味着至少有 majority 个数 ≥ 这个值——正好满足 quorum 条件。

#### Split Vote 活锁

Split vote（票数分散）= 多个 Follower **同时**超时变 Candidate、同时拉票、票分散到多人手里，**谁都拿不到 quorum**，选举一直失败→退位→重新计时→又来一轮，形成**活锁**（livelock）。

**为什么会发生**：

```
5 节点，Leader 挂了：

F1 超时 → Candidate, term=3, 自投1票
F2 超时 → Candidate, term=3, 自投1票     ← 同时！
F3 超时 → Candidate, term=3, 自投1票

F4、F5 各有 1 票，面对 3 个 Candidate：
  F1: 2票, F2: 2票, F3: 1票  → 都不够 quorum(3)

→ 全部退为 Follower，重新等超时...
→ 又同时到期，又分散...         ← 永远选不出
```

**活锁 vs 死锁**：死锁是大家都卡住不动（互相等），活锁是大家都在动但**永远做不出有效进展**（选不出 Leader）。

**Raft 的解法：随机超时**（`election_timer.h:48`）：

```cpp
std::uniform_int_distribution<int> dist_{150, 300};  // [150, 300]ms 随机
```

每次定时器 `Reset()` 后，`Run()` 循环重新随机取值（`election_timer.cc:63`）：

```cpp
int timeout_ms = dist_(rng_);                         // 例如: 180ms
util::ThisFiber::SleepFor(std::chrono::milliseconds(timeout_ms));
```

5 个 Follower 各自随机出 180ms、250ms、160ms、290ms、210ms → **160ms 那个最先到期**，趁别人还在睡就把票收了，拿 quorum 当选。

**什么情况下仍然可能 Split Vote**：如果两个节点随机到很接近的值（比如 160ms 和 165ms），160ms 那个发起选举后，165ms 那个 5ms 后也醒了，也发起选举。此时如果前一个还没收够票，票又可能分散。但这种**概率很低**，而且重新随机后又会有新的"先醒者"，不会一直循环。

**答法**："Split vote 是选举活锁。Raft 通过随机选举超时 `[150,300]ms` 来打破同步，让一个节点先超时先发起选举，收够 quorum 票。每次 `Reset()` 重新随机，所以即使偶然一次分散票，下一轮几乎必然有一个人先到。本质是用随机化把碰撞概率降到接近 0。"

#### 永久脑裂与 Split Vote 的区别

容易混淆的两个概念：

| | Split Vote（选举活锁） | 永久脑裂（网络分区） |
|---|---|---|
| **原因** | 随机值碰巧接近，多 Candidate 同时竞选 | 网络物理断开，集群被切成两个分区 |
| **现象** | 谁都选不出 Leader，一直在重试 | 两个分区各自选出 Leader，各自接受写入 |
| **后果** | 集群不可用，但不丢数据 | 同一 key 有两个不同值，恢复后数据冲突 |
| **是否可恢复** | 自动恢复（下一次随机必有先醒者） | **不可自动恢复**，需人工介入合并数据 |

**永久脑裂（网络分区）示例**：

```
  ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐
  │  A   │    │  B   │    │  C   │  ✕ │  D   │    │  E   │
  └──────┘    └──────┘    └──────┘  网  └──────┘    └──────┘
   分区1 = {A,B,C}, 3/5 节点       络  分区2 = {D,E}, 2/5 节点
                         断
分区1: A 续任 Leader, quorum=2 ✓, 写入 SET x=1
分区2: D 超时变 Candidate, E 投票 → D 成 Leader, 写入 SET x=2

恢复后: 同一个 key x, 值到底是 1 还是 2？→ 冲突！必须人工处理。
```

**Raft 如何防止永久脑裂**：quorum 机制天然杜绝。

- 分区 1 有 3/5 节点 ≥ quorum(3) → **可以正常服务**
- 分区 2 只有 2/5 节点 < quorum(3) → **选不出 Leader，无法写入**

少数派分区虽然活着，但只能读旧数据或返回 READONLY 错误，不会产生数据冲突。**5 节点允许挂 2 台**（`N-1-允许挂数 → 5-1-2=2` 还够 majority）。如果拆成 2+2+1，任何分区都不满足 quorum → 整个集群不可用（Raft 的 CAP 取舍：选一致性牺牲可用性）。

#### 为什么 Split Vote 不会永久化：双重保险

**① 随机超时打破对称**

无随机 → 所有节点同时 150ms 到期 → 同时变 Candidate → 永远平票。
有随机（`election_timer.h:48`, `dist_{150,300}`）→ 每次 `Reset()` 重新随机取值，必然有一个人先醒。即使本轮偶发碰撞，下一轮重新随机又会有新的先醒者。

**② 一票制 + voted_for 持久化**

每个节点一个 term 只能投一票（`raft_node.cc:221-226`）：

```cpp
const NodeId& voted = storage_.voted_for();
if (!voted.empty() && voted != request.candidate_id) {
    return {..., false};  // 已投过，拒绝后来的候选人
}
```

且投票结果立刻持久化，`set_voted_for()` 带 `Flush()` 原子写盘（`raft_storage.cc:134-137`）。即使节点崩溃重启，也**不会失忆**——恢复后 `Load()` 读出 `voted_for`（`raft_storage.cc:50`），知道自己在这个 term 已投过票。没有持久化的话，节点崩溃→重启→忘票→重新投给另一个候选人→同一 term 可能有两个 candidate 都"拿到"同一节点的一票。

**"无限平票概率趋 0"**：每轮随机独立，一轮 split vote 概率已很低（所有 Candidate 的随机值恰好接近到在 RTT 内分不出先后）；这低概率事件无限次重复发生的概率 → 0。所以可能出现一次两次 split vote，但**不可能永远选不出 Leader**。

### 1.7 RaftEngine 详解

#### 定位

`RaftEngine`（`raft_engine.h:19`）**不是 Raft 共识算法的实现**，而是 Raft 模块对外的**写网关**——它是 `Service`（RESP 协议层）和 `RaftNode`（共识核心）之间的连接层。它拥有两个核心成员：

```
RaftEngine {
    KvStateMachine kv_;       // 状态机：真正执行 SET/DEL/GET 的地方
    RaftGroup group_;         // 共识组：包含 RaftNode、日志存储、快照管理器
}
```

#### 核心方法

| 方法 | 作用 | 源码 |
|------|------|------|
| `SubmitCommand(cid, args)` | 写入口：编码命令、检查 Leader、走快/慢路径 | `raft_engine.cc:21-43` |
| `FastCommitPath(cmd)` | 单节点优化：追加日志 → 提升 commit → 直接 apply | `raft_engine.cc:45-53` |
| `Get(db, key, consistency)` | 读入口：可选 `kLocal`（直接读）或 `kLinearizable`（先 ReadIndex 再读） | `raft_engine.cc:59-68` |
| `ReadIndex()` | 代理到 `RaftNode::ReadIndex()`，确认 Leader 身份 | `raft_engine.cc:70-72` |
| `BootstrapSingleNode()` | 单节点集群快速提升为 Leader，无需等待选举超时 | `raft_engine.h:34-36` |

#### 写请求完整链路（`SubmitCommand`）

```
Service::Set()
    │
    v
SubmitCommand(cid, args)
    │
    ├── ① CommandEncoder::Encode → ReplicatedCommand{SET, ["key","val"]}
    │      (如果命令不是 CO::WRITE 类型，直接 Apply 到状态机，不走 Raft)
    │
    ├── ② 检查 Leader: role() != Leader? → 返回 ERROR
    │
    ├── ③ PeerCount==0? (单节点)
    │      └── FastCommitPath: LogStorage::Append → AdvanceCommitIndex → ApplyCommittedLogs
    │
    └── ④ 多节点:
           LogStorage::Append(LogEntry{term, command})
           └── RaftNode::ReplicateLog()
                 ├── 对每个 peer 同步发 AppendEntries
                 ├── AdvanceCommitIndex() (多数派检查 + Figure 8 安全)
                 └── ApplyCommittedLogs() (批量 apply 128 条)
```

关键代码 — 准入检查（`raft_engine.cc:28-31`）：
```cpp
if (group_.node().role() != RaftRole::Leader) {
    return {ApplyOp::ERROR, 0};  // 非 Leader 拒绝写入
}
```

关键代码 — 快慢路径分叉（`raft_engine.cc:35-42`）：
```cpp
if (group_.node().peer_manager().PeerCount() == 0) {
    return FastCommitPath(*cmd);   // 单节点，直接提交
}
// 多节点，追加日志后用 ReplicateLog 复制到 peers
LogEntry entry(group_.node().term(), 0, cmd->Serialize());
group_.log_storage()->Append(entry);
return group_.node().ReplicateLog();
```

#### 读请求链路（`Get`）

```
Service::Get()                         ← main_service.cc:334
    │
    ├── FLAGS_linearizable_read?       ← 由启动 flag 控制
    │       ├── NO  → 直接读本地 DbSlice (kLocal, 可能读到过期数据)
    │       └── YES → engine_.ReadIndex()
    │                    │
    │                    ├── Fast Path: leader_lease 未过期
    │                    │       → 直接取 commit_index_, WaitForApplied(), 返回
    │                    │
    │                    └── Slow Path: 向所有 peer 发 ReadIndex RPC
    │                            ├── 收集 > majority 响应 → 成功
    │                            ├── 收到更高 term → 退位为 Follower
    │                            └── ExtendLeaderLease + WaitForApplied()
    │
    v
engine_.Schedule(key, cb)
    └── cb 在对应的 EngineShard 上执行 → db_slice.Find(key)
```

**要点**：`Service::Get` 直接读本地 `DbSlice`，不走 `RaftEngine::Get`。ReadIndex 只是做**读前的 Leader 身份确认**，确认通过后读的还是本地数据。

#### 为什么需要 RaftEngine 这一层

1. **命令编码与调度**：`CommandEncoder::Encode` 把 RESP 命令转为 `ReplicatedCommand`，决定哪些命令需要走 Raft（只有 `CO::WRITE` 的才复制）。
2. **快慢路径裁决**：单节点跳过 RPC 往返直接 commit，多节点走完整复制流程。
3. **依赖组装**：把 `RaftNode`、`ILogStorage`、`IStateMachine` 三者串起来——RaftNode 负责共识，LogStorage 负责持久化日志，StateMachine 负责执行命令。
4. **对外统一入口**：`Service` 不需要知道 RaftNode 的存在，只需要调 `SubmitCommand` 和 `Get`。

---

## 2. Leader Election

### 2.1 Election Timer（`election_timer.cc` + `.h`）
- 独立 fiber，随机 **[150,300]ms**（`election_timer.h:48`）。
- **epoch 无锁 Reset**：`Reset()` 只 `epoch_.fetch_add(1)`（`:26-29`），fiber 醒来比较 `epoch_ != saved` 则重新计时（`:70-71`）。
- 触发后 `active_=false`（`:75`），fire 一次 `StartElection`。

### 2.2 Heartbeat（`raft_node.cc:387-409`）
Leader 独立 `heartbeat_fiber_`，**50ms** 一次（`raft_node.h:351`），顺带 `ExtendLeaderLease`。Follower `OnHeartbeat` → `election_timer_.Reset()`（`:326`）。**50ms 心跳 vs 150~300ms 超时 = 3~6 倍余量**，防误选举。

### 2.3 Candidate / VoteRequest / VoteResponse
`StartElection`（`:230-275`）：`BecomeCandidate`（term+1、投自己、voted_for=自己 `:151-158`）→ 构造 VoteRequest → 同步遍历 peer 收票 → `TryBecomeLeader` 判过半。

`OnRequestVote`（`:190-228`）四规则：
1. `term < cur` 拒绝；2. `term > cur` 先 BecomeFollower；3. 已投别人拒绝；4. **日志新旧比较**（`(last_log_term,last_log_index)` 字典序，`:213-222`）—— 保证 Leader Completeness。

### 2.4~2.7 关键问答
- **为什么随机超时**：相同超时会同时变 Candidate、瓜分选票 → split vote 活锁。随机让某节点先超时先拿票。
- **为什么不永久脑裂**：随机超时打破对称 + 一票制（voted_for 持久化 `raft_storage.cc:134`），无限平票概率趋 0。
- **一个 term 一个 Leader（Election Safety）**：过半当选 + 一票制 → 两过半集合相交 → 矛盾。
- **两节点同时选举**：Follower 只投先到者，赢者发心跳让另一方 BecomeFollower，否则各自随机超时重试。

### 2.8 选举时序图
```
N1 election_timer 先超时(170ms)
 ├─ BecomeCandidate: term=1, voted_for=N1, vote_count=1
 ├──VoteRequest(term=1,lastLog=0/0)──► N2 → granted
 ├──VoteRequest──► N3 → granted (votes=3)
 ├─ TryBecomeLeader: 3>=majority(2) ✓ → BecomeLeader, StartHeartbeat(50ms)
 └──Heartbeat(50ms)──► N2,N3 持续 Reset 其定时器 → 稳定
```

**回答模板**：
> "选举定时器独立 fiber，随机 [150,300]ms，用 epoch 原子自增实现无锁 Reset。Leader 每 50ms 发心跳，3~6 倍余量防误触发。OnRequestVote 严格按论文做 term 检查、一票制、日志新旧比较。随机超时打破对称避免 split vote；过半+一票制保证 Election Safety。"

**追问**：epoch vs cancel fiber（epoch 最轻量无竞态，代价是重置延迟一个周期）；voted_for 不持久化后果（重启可能同 term 再投他人，破坏 Election Safety）；**同步收票 peer 卡住会阻塞选举 fiber（已知局限，应异步并发）**。

### 2.9 Epoch 原子自增实现无锁 Reset

**问题**：Follower 每收到 Leader 的 Heartbeat，需要重置选举定时器。如果 Leader 心跳 50ms 一次，Follower 每隔 50ms 就要"取消"正在计时的定时器，重新倒计时。传统的做法是 cancel fiber + 重建 fiber，涉及 fiber join / 析构，开销较大。

**本项目的解法**：用一个 `std::atomic<uint64_t> epoch_` 配合 `fetch_add`，不用 cancel fiber，不用 mutex。

**数据结构**（`election_timer.h:44-48`）：
```cpp
std::atomic<bool> active_{false};
std::atomic<uint64_t> epoch_{0};
std::uniform_int_distribution<int> dist_{150, 300};
```

**Reset 只做一件事：epoch++**（`election_timer.cc:26-29`）：
```cpp
void ElectionTimer::Reset() {
    active_.store(true, std::memory_order_release);
    epoch_.fetch_add(1, std::memory_order_release);  // 仅原子自增
}
```

**Run() 用 epoch 做版本号检查**（`election_timer.cc:53-78`）：
```cpp
void ElectionTimer::Run() {
    while (!shutdown_) {
        uint64_t saved = epoch_.load(std::memory_order_acquire);  // ① 记住当前版本号

        if (!active_) { SleepFor(10ms); continue; }               // ② 不活跃就等着

        int timeout_ms = dist_(rng_);                              // ③ 随机超时
        SleepFor(timeout_ms);                                     // ④ 休眠

        if (shutdown_) break;

        // ⑤ 醒来后检查：epoch 变了说明期间有 Reset()
        if (epoch_.load(std::memory_order_acquire) != saved)
            continue;  // 回到循环头，重新随机、重新计时

        // ⑥ epoch 没变 = 真的超时了，触发回调
        active_.store(false);
        cb_();  // → StartElection()
    }
}
```

**工作流程**：

```
时间线：
Follower                            Leader
  │
  │ Run(): saved=epoch=5
  │ SleepFor(200ms) ─────────┐
  │                          │       Heartbeat 到达
  │                          │       election_timer_.Reset()
  │                          │         fetch_add(1) → epoch=6
  │                          │       （没有 cancel fiber，只是变了数字）
  │      ←── 200ms 后醒来 ──┘
  │ epoch==6 != saved(5)
  │ → continue 回到循环头
  │ → 重新 dist_(rng_) 随机 → SleepFor(180ms)
  │ → 这次 180ms 没被 Reset → 真正触发 OnElectionTimeout()
```

**为什么是无锁的**：

- `Reset()` 只有一行原子操作 `fetch_add(1)`，**无 mutex，无锁**
- `Run()` 只读一次 `epoch_`（`load(acquire)`），休眠后比较
- 两个操作之间没有临界区，不存在竞争窗口

**Compare-And-Swap（CAS）对比**：

| 方案 | 做法 | 问题 |
|------|------|------|
| Mutex | `lock → cancel_fiber → new_fiber → unlock` | 有锁，fiber join 开销 |
| CAS epoch（本项目） | `fetch_add(1)` | 重启需等到下一次醒来，最多延迟一个 `[150,300]ms` 周期 |

**trade-off**：epoch 方案的代价是"重置不是即时的"——Reset 后要等当前睡眠周期结束才生效。但选举场景下 Leader 心跳 50ms，定时器最长 300ms，延迟一个周期完全不构成问题。

**相同的模式也用于 Stop 和 Deactivate**：
```cpp
void Stop()    { shutdown_=true; epoch_.fetch_add(1); }  // 唤醒 fiber 让它退出
void Deactivate() { active_=false; epoch_.fetch_add(1); } // 停用定时器，但不退出 fiber
```

**答法**：
> "Follower 的选举定时器用 `epoch_` 原子自增做无锁 Reset。核心思想是把'重置'转换为'版本号变化'：`Reset()` 只做 `fetch_add(1)`，fiber 在 `Run()` 里每次睡眠前记录 `saved=epoch`，醒来后比较 `epoch!=saved` 就知道有人调了 Reset，回到循环头重新随机计时。全程无 mutex，符合项目 'No mutexes' 的设计原则。代价是 Reset 要等当前睡眠周期结束才生效，延迟最多一个 `[150,300]ms`，在心跳 50ms 的选举场景下不构成问题。"

---

## 3. Log Replication

### 3.1 日志是什么：从零理解

#### 一句话理解

Raft 的"日志"就是一本**只追加不修改的流水账**。每个操作记一行，每行有个序号：

```
序号:  1          2          3          4
内容:  SET a 1    DEL b      SET c 2    SET a 3
```

所有节点上的这本流水账**必须一模一样**（同样的序号对应同样的内容）。Leader 负责把新行抄给 Followers，抄到多数派都确认了，这一行才算"生效"。

#### 每一行长什么样：LogEntry（`raft_types.h:41`）

```cpp
struct LogEntry {
    Term term = 0;       // 第几任 Leader 写的这行
    LogIndex index = 0;  // 序号（第几行）
    std::string command; // 内容，如 "SET a 1"
};
```

三个字段分别回答了三个问题：

| 字段 | 问题 | 举例 | 有什么用 |
|------|------|------|----------|
| `index` | 这是第几条？ | 3 | 比较日志谁更长（`last_log_index`）；按序号顺序 apply |
| `term` | 谁当 Leader 时写的？ | Leader 任期 2 | 选举时比较日志新旧：term 大的优先当选 |
| `command` | 写的是什么？ | `"SET a 1"` | Apply 到状态机时执行这个字符串 |

> **关键认知**：比较谁日志"更新"，看的是 `term`（任期），不是 `index`（序号）。因为前任 Leader 可能写了 100 条但都没提交就被推翻了，现任 Leader 只写了 1 条但已生效。

---

### 3.2 日志怎么存：CommandLog（`command_log.h:17`）

#### 为什么需要设计一个存储层

日志可以存内存（快但断电丢），也可以存磁盘文件（安全但慢）。如果 Raft 核心代码直接操作文件，换个存储方式就得改 Raft 代码，耦合太死。

所以先定义了一个**接口** `ILogStorage`（`log_storage.h:17`），规定日志存储"能做哪些事"：

```
      ILogStorage（接口：规定能做哪些事）
        │
        ├── CommandLog      → 存内存（测试/单节点用）
        └── SegmentLogStorage → 存磁盘文件（生产用）
```

`RaftNode` 只持有 `ILogStorage*` 指针，不关心底层是内存还是文件。换存储实现时 Raft 代码一行不用改。

#### 存储结构：vector + 偏移量

```
entries_ 数组:  [哨兵] [条目1] [条目2] [条目3] [条目4]
                  ↑
              占位符，编号0，永远不用

base_index_ = 0（未压缩时）
```

**为什么有个"哨兵"（`entries_[0]`）占位？**

Raft 日志编号从 1 开始（不是从 0 开始）。如果 `entries_[0]` 放编号 1 的条目，那 `entries_[1]` 放编号 2……每次访问都要算 `entries_[index-1]`，容易写错。

塞一个占位符到 `entries_[0]` 之后，**数组下标 = 日志编号**，直接 `entries_[3]` 就是 3 号日志，不用 -1。

创建时只放哨兵（`command_log.cc:13-15`）：
```cpp
CommandLog::CommandLog() {
    entries_.emplace_back(0, 0, "");  // 索引0占位，term=0, 内容为空
}
```

#### 日常操作

**追加一行**（`command_log.cc:60-64`）：

```cpp
// 调用者只需传 term 和 command，index 自动分配
LogEntry entry(term=node.term(), command="SET a 1");
log.Append(entry);

// Append 内部：
//   entry.index = LastIndex() + 1;   ← 自动编号
//   entries_.push_back(entry);
```

**查一行**（`command_log.cc:42-45`）：

```cpp
// 查第3条
const LogEntry* e = log.Get(3);
// 内部: entries_[3 - base_index_] = entries_[3]   ← 直接数组下标
```

**删除尾部**（`command_log.cc:82-90`）：

如果 Leader 告诉我"你第 5 条往后都不对"，就截断：

```cpp
log.TruncateFrom(4);  // 保留1-4条，删掉5及之后的

// 内部：
//   physical = 4 - base_index_; // 逻辑索引4 → 数组下标
//   entries_.resize(physical + 1); // +1 保留哨兵
```

#### 关键概念：base_index_（压缩偏移）

日志会越来越多（十万、百万条），内存放不下。Snapshot（快照）把状态机的完整数据存成一个文件后，**快照之前的日志就可以删了**。

但删除后，剩余日志的编号不能变——外部还在用老编号访问。于是用 `base_index_` 记录"删到哪了"：

```
压缩前 (base_index_=0):
  entries_: [哨兵0] [1] [2] [3] [4] [5]   ← 5条日志
                      ↑                 ↑
               FirstIndex()=1    LastIndex()=5

CompactUpTo(3) 之后:  ← 把快照覆盖的 1-3 号删掉
  entries_: [哨兵0] [4] [5]                ← 只剩2条
              ↑      ↑              ↑
          base_index_=3  FirstIndex()=4  LastIndex()=5
```

三个公式（`command_log.cc:30-33`）：
```cpp
FirstIndex() = base_index_ + 1;           // 4 = 3 + 1
LastIndex()  = base_index_ + size() - 1;  // 5 = 3 + 2 - 1
Get(n)  →  entries_[n - base_index_];     // Get(5) → entries_[5-3] = entries_[2]
```

**压缩前** `n - 0 = n`，下标 = 编号。
**压缩后** `n - 3` 做偏移，外部用编号 5 访问，内部映射到 `entries_[2]`。

#### 特殊处理：快照锚点（SnapshotAnchor `log_storage.h:39`）

压缩删掉 1-3 号日志后，如果 Leader 发来 `AppendEntries(prevLogIndex=3, prevLogTerm=1)`，Follower 需要知道 3 号的 term 是多少——但它已经被删了。

所以保留一个"锚点"记住最后一个被快照覆盖的条目的 index 和 term：

```cpp
struct SnapshotAnchor {
    LogIndex index;  // 3（最后被删的编号）
    Term term;       // 1（它的 term）
};

// GetTerm 查 term 时优先看锚点：
Term GetTerm(LogIndex index) const {
    if (index == anchor_.index)  return anchor_.term;  // 命中锚点
    const LogEntry* e = Get(index);                     // 否则查数组
    return e ? e->term : 0;
}
```

---

### 3.3 关键索引
| 索引 | 含义 | 实现 |
|---|---|---|
| commit_index | 多数派复制、可 apply 最高 index | `commit_index_` / `AdvanceCommitIndex` |
| last_applied | 已 apply 最高 index | `last_applied_` / `apply.meta` |
| matchIndex | 每 peer 已复制最高 index | `peer_last_log_index_[]`（`raft_node.h:348`）|
| nextIndex | 下次发起始 index | 隐式 `peer_last_log_index_[i]+1`（`:724`）|

> ⚠️ 无显式 `nextIndex[]` 快速回退数组，每次从 `FirstIndex()` **全量发送**（`:748-752`）。

### 3.4 Leader 发送 `ReplicateLog`（`:699-766`）
单节点快路径 → 判断需否 InstallSnapshot → 构造 AppendEntries（全量 GetRange）→ 同步发每个 peer 记 matchIndex → `AdvanceCommitIndex` → `ApplyCommittedLogs`。

### 3.5 Follower `OnAppendEntries`（`:589-641`）
1. `term<cur` 拒绝返回 last_log_index（`:591`）；
2. `term>=cur` BecomeFollower；
3. **gap 检查** `prev_log_index>LastIndex` 拒绝（`:605`）；
4. **一致性检查** prevLogTerm 不匹配拒绝返回 `prev_log_index-1`（`:611-614`）；
5. **冲突截断** `TruncateFrom` 再 Append（`:617-624`）→ Log Matching；
6. `commit_index=min(leader_commit,my_last)` 再 apply（`:633-638`）。

### 3.6 提交 `AdvanceCommitIndex`（`:768-799`）

把每个节点的日志进度列出来，排序，取**第 majority 大的那个**作为安全提交位置。

```cpp
indexes = [LastIndex(), peer matchIndex...];
sort(降序);
candidate = indexes[majority - 1];   // 第 majority 大
if (candidate > commit_index_) commit_index_ = candidate;
```

**例 1**：3 节点 [5,4,3] 降序，majority=2，取 indexes[1]=4 → 至少 2 节点复制到 4 → 提交到 4。

**例 2**：5 节点，各自进度如下：

```
自己(Leader): 100    peer A: 98
peer B:       97     peer C: 95    peer D: 90
```

合并降序 `[100, 98, 97, 95, 90]`，`majority = 5/2+1 = 3`，取 `indexes[3-1] = 97`。

**97 的含义**：至少有 3 个节点（自己 + A + B）的日志进度 ≥ 97。index ≤ 97 的所有日志都已在多数派上生效 → 可以安全提交到 97。

**为什么不是取最大或最小**：

```
[100, 50, 50, 50, 50]  ← 5节点
```

- 取最大值 100 → 只有自己确认，挂了就丢（不安全）
- 取最小值 50 → 太保守，明明可能有 3 个人到了 80
- **取第 3 大** → 恰好"强者的最短板"：至少 majority 个人 ≥ 这个值

**极端情况**：

```
[100, 100, 100, 100, 100] → indexes[2] = 100 → 全部追平，提交到 100
[100, 50,  50,  50,  50]  → indexes[2] = 50  → 多数派只有 50，只能提交到 50
```

### 3.7 为什么多数派才 commit
少数派 commit 后崩溃，剩余节点无该 entry，新 Leader 也无 → 已响应客户端的写永久丢失。过半保证两过半集合相交 + 选举日志检查 → 已提交 entry 必在未来 Leader 上。

### 3.8 为什么日志一致（Log Matching）
prevLogTerm 检查 + 失败回退 + 冲突截断以 Leader 为准。

**回答模板**：
> "Leader 落日志后广播 AppendEntries，Follower 做 gap/prevLogTerm 检查和冲突截断实现 Log Matching。commit 推进把自己和所有 matchIndex 降序取第 majority 位。多数派才 commit 保证已提交 entry 出现在任何未来 Leader。"

**高危追问**：
- **有 nextIndex 快速回退吗？** 没有，每次全量从 FirstIndex 发送，靠 Follower 返回 last_log_index 记 matchIndex，快速回退是优化方向。
- **能提交前一 term 的 entry 吗？（Figure 8）** 不能直接靠计数，须提交当前 term entry 间接提交。**本项目 `AdvanceCommitIndex` 缺 `GetTerm(candidate)==current_term` 检查，是真实简化/缺口，需修复。**
- **apply 为何按 index 顺序？** State Machine Safety，`last_applied_+1` 递增（`:859`）。

---

## 4. ReadIndex

### 4.1~4.2 为什么需要
被隔离的旧 Leader 直接读内存会返回 stale 数据。ReadIndex 让 Leader 返回读前**确认自己仍能联系多数派**，排除"被取代的旧 Leader"。

### 4.3 实现（`raft_node.cc:411-468`）
```cpp
if (role_ != Leader) return 0;
if (NowMs() < leader_lease_expire_) {          // 快路径
  read_index = commit_index_; WaitForApplied(read_index); return read_index;
}
read_index = commit_index_;                    // 慢路径
for (peer) { resp = SendReadIndex(peer);
  if (resp.success && resp.term==cur) success_count++;
  else if (resp.term>cur) { BecomeFollower(resp.term); return 0; } }
if (success_count < majority) return 0;
ExtendLeaderLease(); WaitForApplied(read_index); return read_index;
```
四步：记 commit_index → 广播确认 quorum → 更高 term 退位（`:449`）→ `WaitForApplied`（`:470-478`）等 apply 追上。

### 4.4 Lease Read（`:418-423,480-482`）
心跳每轮 `ExtendLeaderLease`（`:390`），`lease_ms_=100`。lease 内走快路径跳过 quorum RPC。**风险**：依赖时钟，长 GC/暂停可能误判 → 理论 stale read。

### 4.5 流程图
```
Get(linearizable) → ReadIndex()
  ├─ lease有效? ─yes─► read_index=commit_index → WaitForApplied → 读DbSlice
  │      no
  ├─ 广播 ReadIndexRequest → 收集 success
  ├─ success>=majority? ─no─► return 0
  ├─ ExtendLeaderLease → WaitForApplied(read_index)
  └─ 返回 → kv_.Get()
```

### 4.6 为什么比"读也写日志"快
read-as-log 需一整轮复制 + fsync。ReadIndex 快路径零 RPC 零磁盘，慢路径一轮轻量 RPC（无日志体、不落盘）。

> ⚠️ 线上默认 `Service::Get` 未走 ReadIndex，只有 `RaftEngine::Get(kLinearizable)` 才走。答题须说清。

**回答模板**：
> "GET 直接读会有旧 Leader 脏读。ReadIndex 记 commit_index，广播轻量 RPC 确认多数派，再 WaitForApplied 后读。Lease Read：心跳续 100ms lease，lease 内快路径跳过 RPC，代价是时钟依赖。相比读写日志省了 fsync 和日志膨胀。"

**追问**：Lease 时钟风险（monotonic clock + 保守 lease 缓解）；Follower read（向 Leader 请 read_index 再等自身 apply，`OnReadIndex` `:331` 已返回 commit_index 具备基础）。

---

## 5. Snapshot

### 5.1 为什么需要快照

Raft 日志只追加不删除。如果不做任何处理，三个问题会越来越严重：

| 问题 | 后果 |
|------|------|
| **磁盘撑爆** | 日志无限增长，迟早写满 |
| **重启太慢** | 重启后要重放（replay）所有日志才能重建状态，百万条日志可能需要几秒 |
| **新节点追赶压力大** | 新加入的节点要从第 1 条开始复制整个日志，数据量巨大 |

**Snapshot 的思路**：每隔一段时间，把状态机的最新的完整数据导出成一个文件。这个文件覆盖了之前所有日志的效果，所以它覆盖的那些日志就可以删了。

类比：就像游戏存档。玩到第 100 关，存个档。之后如果挂了，从第 100 关读档继续，不用从第 1 关重新打。

### 5.2 什么时候触发

`RaftSnapshotManager`（`snapshot_manager.h:25`）有一个后台 fiber，每秒检查一次：

```cpp
void SnapshotLoop() {
    while (!shutdown_) {
        ScheduleCreateIfNeeded();          // 检查是否需要创建快照
        for (int i = 0; i < 1000; i++)
            SleepFor(1ms);                 // 每秒检查一次，避免空转
    }
}
```

触发条件（`snapshot_manager.cc:76-88`）：

```cpp
bool ScheduleCreateIfNeeded() {
    LogIndex last_index = log_storage_->LastIndex();
    LogIndex snapshot_index = meta_storage_.meta().index;

    // 日志比上次快照多出 log_gap_ 条（默认 100000），就触发
    if (last_index < snapshot_index + log_gap_)
        return false;
    return CreateSnapshot();
}
```

触发阈值 `log_gap_` 默认 **100000** 条（`snapshot_manager.h:82`），可配置。

### 5.3 快照创建的完整流程

`CreateSnapshot()`（`snapshot_manager.cc:41-74`）分五步：

```
① barrier_.BeginWrite()          ← 暂停所有写操作
② state_machine_->SaveSnapshot() ← 把状态机数据导出到 snapshot.bin
③ meta_storage_.SetMeta(...)     ← 记录快照元信息到 snapshot.meta
④ log_storage_->CompactLogs()    ← 删除快照覆盖的日志
⑤ barrier_.EndWrite()            ← 恢复写操作
```

源码：
```cpp
bool CreateSnapshot() {
    // ① 暂停写
    barrier_.BeginWrite();

    // ② 导出状态机数据到文件
    bool ok = state_machine_->SaveSnapshot(dir_ + "snapshot.bin");

    if (ok) {
        // ③ 记录元信息：最后一条日志的 index 和 term
        meta_storage_.SetMeta({last_index, last_term, NowMs()});

        // ④ 删除快照覆盖的日志
        log_storage_->CompactLogs(last_index, last_term);
    }

    // ⑤ 恢复写
    barrier_.EndWrite();
    return ok;
}
```

### 5.4 SnapshotBarrier 怎么暂停写

`SnapshotBarrier`（`snapshot_barrier.h:18`）是一种**读者-写者锁**，但用 fiber yield 而非传统 mutex：

```
正常的写操作（BeginRead / EndRead）：                    
    BeginRead() → 写数据到 DbSlice → EndRead()           

创建快照时（BeginWrite / EndWrite）：                     
    BeginWrite() → 导出数据 → EndWrite()                      
```

**写操作侧的配合**（BeginRead）：

```cpp
void BeginRead() {
    while (writing_.load())        // 有人在写快照？等着
        ThisFiber::Yield();
    readers_++;                     // 注册"我在写"

    if (writing_.load()) {          // 再次检查（防竞态）
        readers_--;
        while (writing_.load())
            ThisFiber::Yield();     // 等待快照完成
        readers_++;
    }
}
```

**快照侧的阻塞**（BeginWrite）：

```cpp
void BeginWrite() {
    writing_ = true;                // 宣示"我要写快照了"
    while (readers_ > 0)            // 等所有正在执行写的操作完成
        ThisFiber::Yield();
}
```

关键设计：`writing_` 设成 true 后，新的写操作在 `BeginRead` 就会被挡住。然后 `BeginWrite` 只要等已经进入的写操作结束（`readers_` 降到 0），就能拿到一个**一致的、没有写操作在进行中的状态**，此时导出数据就是准确的。

**为什么用 fiber yield 而不是 mutex**：符合项目 "No mutexes" 的设计原则。mutex 会阻塞线程，fiber yield 只让出当前协程的执行权，同一线程的其他 fiber 可以继续跑。

### 5.5 快照后的日志压缩

`CompactLogs` 做了三件事（`log_storage.h:80-83`）：

```
① SetSnapshotAnchor(last_index, last_term)
    保留锚点：记录快照覆盖的最后一条日志的 index 和 term
    这样 AppendEntries 的 prevLogTerm 检查仍然有效

② CompactUpTo(last_index)
    删除 entries 数组中 index ≤ last_index 的条目
    更新 base_index_ 偏移量

③ 对于 SegmentLogStorage，还会删除 segment_*.log 文件
```

**为什么需要 SnapshotAnchor**：

压缩后，快照覆盖的那条日志（比如 index=100）已经从数组里删了。但如果 Leader 发来 `AppendEntries(prevLogIndex=100, prevLogTerm=5)`，Follower 必须知道 index=100 的 term 是 5，才能做一致性检查。

所以保留一个锚点（`log_storage.h:39-44`）：

```cpp
struct SnapshotAnchor { LogIndex index; Term term; };

// GetTerm 优先查锚点
Term GetTerm(LogIndex index) const {
    if (index == anchor_.index) return anchor_.term;
    return Get(index) ? Get(index)->term : 0;
}
```

### 5.6 恢复流程

节点重启后的恢复（`raft_node.cc:36-74` `SetStoragePath`）：

```
① 从 apply.meta 读 last_applied_
② 用 SnapshotLoader 检查 snapshot.bin 是否存在
③ 如果存在：
      state_machine_->LoadSnapshot(bin_path)  ← 把快照数据加载到状态机
      last_applied_ = max(last_applied_, snapshot_index)
      SetSnapshotAnchor(snapshot_index, snapshot_term)
④ ReplayUnappliedLogs()
      把快照之后、last_applied_ 到 LastIndex 之间的日志重新 apply
      （不会重复 apply 快照覆盖的部分）
```

**恢复后的数据状态**：

```
恢复前:
  snapshot.bin (覆盖 index 1-100000 的数据)
  WAL (index 100001-101000 的日志)

恢复后:
  状态机 = snapshot.bin 的数据 + apply index 100001-101000 得到的数据
  commit_index = 101000
  last_applied = 101000
```

### 5.7 InstallSnapshot：给落后 Follower 发快照

当 Leader 发现某个 Follower 的日志进度比快照的 index 还落后，发 AppendEntries 没意义（那些日志已经被压缩删掉了）。此时 Leader 走 InstallSnapshot RPC。

**触发判断**（`raft_node.h:247`）：

```cpp
static bool ShouldInstallSnapshot(LogIndex next_index, LogIndex snapshot_index) {
    return snapshot_index > 0 && next_index <= snapshot_index;
}
// next_index = Follower 的下一条需要的 index（即 matchIndex + 1）
// 如果 next_index 比快照还小 = Follower 错过的日志已经被压缩了 → 发快照
```

**Sender 分块发送**（`snapshot_sender.h:20`）：

```cpp
static constexpr size_t kChunkSize = 65536;  // 64KB 每块

// 把 snapshot.bin 按 64KB 切成多块，逐块发送 InstallSnapshot RPC
bool SendSnapshot(follower, group_id, term, leader_id,
                  last_included_index, last_included_term);
```

每块 RPC 带 offset（从哪里开始读）和 done（是不是最后一块）。

### 5.8 Follower 接收快照

`OnInstallSnapshot()`（`raft_node.cc:661-715`）：

```
① term 检查：stale term 拒绝
② 如果 term >= cur_term → BecomeFollower（承认对方是 Leader）
③ snapshot_receiver_->HandleChunk(req)
     → 按 offset 写到 snapshot.recv.tmp 文件
④ 如果 req.done == true（最后一块到了）：
     → fflush + fdatasync + 原子 rename(tmp → snapshot.bin)
     → state_machine_->LoadSnapshot(bin_path)        ← 加载到状态机
     → last_applied_ = commit_index_ = last_included_index
     → log_storage_->Clear()                          ← 清空旧日志
```

**Receiver 的崩溃安全**（`snapshot_receiver.h:18-19`）：

```cpp
// 写到临时文件 snapshot.recv.tmp
// 最后一块到达后 fsync + rename 为 snapshot.bin
// 如果中途崩溃，下次 Init() 删掉残留的 snapshot.recv.tmp
```

**回答模板**：
> "后台 fiber 每秒检查，日志超出快照 10 万条就触发。创建时 SnapshotBarrier 暂停写、导出全量 KV、写 meta、压缩日志（删 WAL 段+更新 base_index+保留 SnapshotAnchor 让一致性检查继续有效）。恢复时先 LoadSnapshot 加载全量，再 ReplayUnappliedLogs 只重放快照之后的增量日志。落后 Follower 走 InstallSnapshot 分块传输，每块 64KB，最后一块 fsync+原子 rename 保证崩溃安全。"

**追问**：SnapshotAnchor 为何关键（压缩后仍需 prevLogTerm 检查）；一致视图如何保证（BeginWrite 等 in-flight 写完成）；为何用 fiber yield 不用 mutex（"No mutexes" 设计原则，不阻塞线程）

---

## 6. Joint Consensus

### 6.1 要解决什么问题

集群节点的数量会变：加新机器扩容、下掉故障机器。这不是简单的"改个配置文件"，因为节点数量变了，**quorum 就变了**。

**直接切换的危险**：假设从 `{A,B,C}` (3 节点，majority=2) 变成 `{A,B,C,D,E}` (5 节点，majority=3)。

```
时刻 T1: A,B 收到新配置 → 认为集群是 5 节点，majority=3
时刻 T2: C,D,E 还是旧配置 → 认为集群是 3 节点，majority=2
```

旧节点和新节点在同一时刻**对 majority 的定义不同**：

```
分区 C,D,E（旧配置，majority=2）：
   C 自投一票 + D 投一票 = 2 票 ≥ majority(2) → C 当选 Leader！

分区 A,B（新配置，majority=3）：
   只有 A,B 两人，最多 2 票 < majority(3) → 选不出 Leader。
   （但这个例子反过来也可能，如果新配置节点多，新这边可能选出 Leader）
```

更坏的情况：如果 C,D,E 在旧配置下选出 Leader，A,B 在新配置下也选出 Leader → **脑裂**，两个 Leader 同时接受写入。

**Joint Consensus 的思路**：不直接切，先进入一个"中间态"，在这个中间态里，任何决议（选举、提交）需要**两张票**：旧配置的多数派 **且** 新配置的多数派。

### 6.2 三阶段状态机

```
Stable(C_old)          ← 当前配置，一切正常
    │
    │ BeginConfigChange(C_new) 第一次
    │ Append CONFIG_CHANGE 日志
    │
    ▼ 日志被 apply 后
Joint(C_old, C_new)     ← 中间态：双配置共存
    │                    选举需要 old 和 new 同时过半数
    │                    提交需要 old 和 new 同时过半数
    │
    │ BeginConfigChange(C_new) 第二次（target 必须和第一次相同）
    │ Append 最终 CONFIG_CHANGE 日志
    │
    ▼ 日志被 apply 后
Stable(C_new)          ← 新配置生效，结束
```

**对应的代码状态**（`raft_types.h:62`）：

```cpp
enum class ConfigState : uint8_t {
    kStable = 0,  // 正常状态（Cold 或 Cnew）
    kJoint = 1,   // 中间态（Cold + Cnew 共存）
};
```

**存储结构**（`raft_types.h:67-69`）：

```cpp
struct JointConfig {
    ClusterConfig old_config;  // 旧配置
    ClusterConfig new_config;  // 新配置
};
```

### 6.3 第一步：进入 Joint 态

`BeginConfigChange(target)` 第一次调用（`raft_node.cc:119-128`）：

```cpp
// Step 1:
joint_config_.old_config = cluster_config_;     // 记录旧配置
joint_config_.new_config = target;              // 记录新配置

// 把新配置编码为 CONFIG_CHANGE 日志，追加到日志里
ConfigChangeCommand cmd{target};
log_storage_->Append(LogEntry(term(), 0, cmd.Serialize()));

// 注意：此时 config_state_ 还是 kStable！
// 状态切换发生在 apply 这条日志的时候，不是在 Append 的时候
```

**为什么状态切换要等 apply 而不是 Append**：

Leader 追加日志 ≠ 这条日志被提交了。如果 Leader 在 commit 之前崩溃，新 Leader 可能根本没有这条日志。如果在 Append 时就把状态切到 Joint，而这条日志最终没被提交，就会产生状态不一致。

所以规则是：**状态随 apply 切换**（`raft_node.cc:927-934`）：

```cpp
// apply CONFIG_CHANGE 日志时：
if (config_state_ == ConfigState::kStable) {       // 当前是 Stable
    // → 第一步：进入 Joint
    joint_config_.old_config = cluster_config_;
    joint_config_.new_config = cmd.target;
    config_state_ = ConfigState::kJoint;           // 状态在这里切换
}
```

### 6.4 Joint 态下的双多数派

进入 Joint 态后，**选举和提交都要同时满足两个配置的 quorum**。

#### 选举：双多数派（`raft_node.cc:295-313`）

```cpp
bool TryBecomeLeader(const ElectionResult& result) {
    if (config_state_ == ConfigState::kJoint) {
        size_t old_majority = (old_voters + 1) / 2 + 1;
        size_t new_majority = (new_voters + 1) / 2 + 1;

        // 必须同时在旧配置和新配置中都拿到 majority
        if (old_config_votes_ >= old_majority &&
            new_config_votes_ >= new_majority) {
            BecomeLeader();  // 双多数派满足，可以当 Leader
            return true;
        }
        return false;  // 任一不满足就不能当选
    }
    // Stable 态：正常单多数派
}
```

**举例**：从 `{A,B,C}` 扩到 `{A,B,C,D,E}`，Joint 态下选举：

```
old_config = {A,B,C}，old_majority = 3/2+1 = 2
new_config = {A,B,C,D,E}，new_majority = 5/2+1 = 3

Candidate 需要：旧配置里至少 2 票，新配置里至少 3 票
→ 同时满足 → 当选
→ 保证了只有一个节点能同时拿到两个 quorum，不会脑裂
```

#### 提交：双多数派（`raft_node.cc:841-882`）

`AdvanceCommitIndexJoint()` 分别算两个配置的提交点，取较小值：

```cpp
// 分别对 old_config 和 new_config 执行多数派计算
LogIndex old_commit = calc_commit(old_config);  // old 的多数派位置
LogIndex new_commit = calc_commit(new_config);  // new 的多数派位置

// 取两者的最小值
LogIndex candidate = std::min(old_commit, new_commit);
```

`calc_commit` 和普通态的 `AdvanceCommitIndex` 一样：收集自己+配置内 peers 的 last_log_index，排序取第 majority 大。

**为什么取 min**：一条日志必须在新旧两个配置里**都**达到多数派才算安全提交。old_commit=100 代表旧配置里的多数派已到 100，new_commit=80 代表新配置里只有 80。取 min=80：只有 80 在两边都达到了多数派。

### 6.5 第二步：退出 Joint 态

`BeginConfigChange(target)` 第二次调用（`raft_node.cc:105-116`）：

```cpp
// 当前状态必须是 Joint，且 target 必须与第一次一致
if (config_state_ == ConfigState::kJoint) {
    if (target != joint_config_.new_config) return false;  // 必须一致

    // 追加第二条 CONFIG_CHANGE 日志
    ConfigChangeCommand cmd{target};
    log_storage_->Append(LogEntry(term(), 0, cmd.Serialize()));
    return true;
}
```

等这条日志被 apply 时（`raft_node.cc:919-924`）：

```cpp
// apply 第二条 CONFIG_CHANGE 时，当前 state 是 kJoint：
if (config_state_ == ConfigState::kJoint) {
    cluster_config_ = cmd.target;        // 新配置正式生效
    joint_config_ = JointConfig{};        // 清空 joint 配置
    config_state_ = ConfigState::kStable; // 回到 Stable
    peer_manager_.SetConfig(&cluster_config_);
}
```

### 6.6 完整时序示例

从 `{A,B,C}` 加 D,E 变成 `{A,B,C,D,E}`：

```
Step 0: Stable({A,B,C}), Leader = A

Step 1: A 调 BeginConfigChange({A,B,C,D,E}) 第一次
    → 追加 CONFIG_CHANGE 日志

Step 2: 日志复制到多数派({A,B,C}中 ≥ 2 个) → 提交 → apply
    → config_state_ 切为 kJoint
    → joint_config = {old:{A,B,C}, new:{A,B,C,D,E}}

Step 3: Joint 态下继续正常工作
    → 选举：需要 old(2/3) 和 new(3/5) 两类多数派
    → 提交：AdvanceCommitIndexJoint 取 min(old_commit, new_commit)

Step 4: A 调 BeginConfigChange({A,B,C,D,E}) 第二次（必须相同 target）
    → 追加第二条 CONFIG_CHANGE 日志

Step 5: 日志复制 → 提交 → apply
    → cluster_config_ = {A,B,C,D,E}
    → config_state_ 切回 kStable

Step 6: Stable({A,B,C,D,E})，扩容完成
```

**回答模板**：
> "直接切配置有新旧各自选主的脑裂窗口。我按论文实现 Stable→Joint→Stable 三阶段：Joint 阶段选举和提交都要 old 和 new 双多数派。选举在 TryBecomeLeader 里同时检查 old_votes ≥ old_major && new_votes ≥ new_major；提交在 AdvanceCommitIndexJoint 里分别算两个配置的多数派、取 min。双多数派保证了任意时刻最多一个 Leader。状态在 apply CONFIG_CHANGE 日志时才切换，不是在 BeginConfigChange 时切换，防止未提交日志导致的 config 不一致。"

**追问**：状态为什么在 apply 时切（非 Append 时防未提交崩溃）；第一条日志在 Stable 态 apply 进 Joint，第二条在 Joint 态 apply 回 Stable（参考 `:917-936`）；当前实现 learner 字段保留但投票未接入；BeginConfigChange 提前设 joint_config 有轻微冗余，应以 apply 时为准。

---

## 7. 请求流程（SET vs GET）

### 7.1 SET 完整链路
```
SET a 1 → redis_parser → Service::DispatchCommand → Service::Set (main_service.cc:283)
 → engine_.SubmitCommand (raft_engine.cc:21)
   ├─ CommandEncoder::Encode → "SET a 1"
   ├─ role!=Leader → READONLY
   ├─ PeerCount==0 → FastCommitPath
   ▼ (多节点)
   log_storage()->Append(LogEntry{term,0,"SET a 1"})
   ▼ node.ReplicateLog()
   ├─ 广播 AppendEntries → Follower.OnAppendEntries: 检查→Append→返回last_log_index
   ├─ AdvanceCommitIndex(多数派) → commit_index 推进
   ▼ ApplyCommittedLogs → state_machine_->ApplyLogEntry
     → KvStateMachine::Set → shard_set_->Await(Shard("a"), 写DbSlice)
   ▼ 返回 {OK,1}
 → cntx->SendStored() → +OK
 → persistence_manager_->RecordCommand (AOF) + snapshot_fiber_.NotifyWrite
```

### 7.2 GET 两种路径
- **默认 local**（`main_service.cc:311-327`，线上实际）：`engine_.Schedule` → `shard_set_->Add(Shard(key), 读DbSlice)`，**无 ReadIndex**，最快但可能 stale。
- **线性一致**（`raft_engine.cc:59-68`，需 kLinearizable）：`ReadIndex()`（快/慢路径）→ ri==0 返回 KEY_NOTFOUND → `kv_.Get`。

### 7.3 对比
| 维度 | SET | GET(linearizable) |
|---|---|---|
| 写日志 | 是 | 否 |
| fsync | 是(WAL) | 否 |
| 需多数派 | 是(复制) | 是(ReadIndex)/lease内零RPC |
| 改状态机 | 是 | 否 |
| 延迟来源 | 复制+fsync | 轻量RPC或0 |
| 非Leader | READONLY | 返回0 |

**回答模板**：
> "SET 走 SubmitCommand：编码日志→Append→ReplicateLog→多数派确认→ApplyCommittedLogs 里 Shard 定位分片写 DbSlice→+OK+AOF。GET 默认走 Schedule 直接读分片(local)；强一致走 RaftEngine::Get 先 ReadIndex。区别：SET 写日志+fsync+改状态机，GET 只确认 leadership。"

---

## 8. 网络 RPC

Transport 纯虚接口（`transport.h`），测试用 `LocalTransport` 进程内直调（`local_transport.cc`）。

| RPC | 请求 | 响应 | 何时发 | 失败条件 |
|---|---|---|---|---|
| RequestVote | term, candidate_id, last_log_index/term | term, vote_granted | 选举 | term落后/已投/日志旧 |
| AppendEntries | term, leader_id, prev_log_index/term, entries[], leader_commit | term, success, **last_log_index** | 复制&心跳 | term落后/gap/prevLogTerm不匹配 |
| InstallSnapshot | term, leader_id, last_included_index/term, offset, done, data | term, success | peer落后于snapshot | term落后/写盘失败 |
| ReadIndex | term, leader_id, request_id | term, success, commit_index | ReadIndex慢路径 | term落后/自己Candidate |
| TimeoutNow | term, leader_id | term, accepted | Leader转移 | term落后 |
| Heartbeat | term, leader_id | term, success | 每50ms | term落后 |

**统一 term 处理**：`req.term<cur` 拒绝回传自己 term；`req.term>=cur` BecomeFollower（`:198,320,598,651`）。
**multi-raft 路由**：RPC 带 group_id，`{group_id,node_id}` 做 key（`local_transport.cc:11`）。
> ⚠️ 同步模型下一个 peer 卡住阻塞驱动 fiber，工业实现应异步+超时。

**回答模板**：
> "六个 RPC 经 Transport 抽象，测试用进程内 LocalTransport。统一 term 规则：更小 term 拒绝回传自己 term，更大 term 立即 BecomeFollower。AppendEntries 回 last_log_index 供回退，InstallSnapshot 分块 offset/done，每请求带 group_id 支持 multi-raft。同步模型 peer 卡住会阻塞，异步化是方向。"

---

## 9. 一致性分析（五大安全性）

| 安全性 | 保证 |
|---|---|
| Election Safety | 一票制 voted_for 持久化(`raft_storage.cc:134`) + 过半当选(`:298`) → 两过半相交矛盾 |
| Leader Append-Only | Append 只 push_back(`command_log.cc:60`)，Leader 路径无 Truncate |
| Log Matching | prevLogTerm 检查 + 冲突截断(`raft_node.cc:609-624`) |
| Leader Completeness | 投票日志新旧检查(`:213-222`)：日志不够新拿不到多数派 |
| State Machine Safety | 按 index 递增 apply(`:858-892`)，commit 后才 apply，commit 需多数派 |

**Leader Completeness 推理链**：entry X 在 term T commit ⇒ 在多数派 Q；term>T 的 Leader L 需多数派 Q' 票；Q∩Q'≠∅ 存在节点 n；n 投 L 要求 L 日志≥n；n 有 X ⇒ L 有 X。∎

> ⚠️ 完整证明依赖论文 §5.4.2 / Figure 8："只能提交当前 term entry 间接提交旧 term entry"。**本项目 `AdvanceCommitIndex` 缺该检查**，是最值得主动坦白 + 说明修复方案的点。

**回答模板**：
> "五大安全性：Election Safety=一票制+过半+持久化；Append-Only=只追加；Log Matching=prevLogTerm+截断；Leader Completeness=投票日志新旧比较+两过半相交；State Machine Safety=按序 apply。我意识到 AdvanceCommitIndex 缺 Figure 8 的当前 term 检查，是要修复的安全缺口。"

---

## 10. 测试体系

`server/test/` 7 大分类，约 87 个 RaftNode 用例 + 各模块单测。

| 类别 | 目录 | 思路 | 断言 |
|---|---|---|---|
| Leader Election | raft_core/raft_node_test.cc | 控制投票响应验证阈值 | 2票当选/1票不当选(`:437,456`)|
| Log Replication | 同上 | Append 后验证复制/截断/gap | ReplicatesLog、RejectsPrevLogMismatch、CommitAdvancesWithMajority(`:613,668,818`)|
| Election Timer | raft_core/election_timer_test.cc | 随机区间/epoch/fiber | 25 用例 |
| Snapshot | raft_snapshot/ | writer/loader/meta/sender/receiver | CRC/原子 rename/分块 |
| Joint | raft_core/raft_group_test.cc | 双多数派 | old&new 同时过半 |
| RPC | raft_rpc/ | 序列化/operator== | 结构完整 |
| WAL 恢复 | raft_log/ | 写段→重开→扫描 | LastIndex/CRC 停扫描 |
| Apply 恢复 | raft_integration/raft_apply_recovery_test.cc | apply.meta+snapshot+WAL 三方 | 见下 |

**恢复测试精华**（`raft_apply_recovery_test.cc`）：
- PartialApplyThenReplay(`:375`)：apply.meta=50，日志100 → replay 51..100。
- SnapshotIndexDominatesApplyMeta(`:534`)：`last_applied=max(applyMeta,snapshotIndex)=500000`。
- DeltaReplayAfterSnapshot(`:604`)：快照1000+日志2000 → 只replay 1001..2000，k999 不存在。
- EndToEndSnapshotRecovery(`:706`)：10000快照+1000增量真实 SegmentLogStorage，统计 recovery_ms/snapshot_load_ms/wal_replay_ms(`:774-781`)。

**断言维度**：状态(`commit_index/last_applied/role/apply_progress`)、数据(`kv.Get().value()`)、计数(`kv.applied.size()/DbSize()`)、边界(`EXPECT_FALSE(...ok())`)。

**压测/Benchmark（现状）**：仓库**无独立压测脚本**，只有恢复时延计时。可说的真实数据：恢复时延(11K entries<10s 断言)；README 百万 QPS 来自底层 helio/io_uring，非 Raft 层实测。

建议补充指标：QPS(redis-benchmark)、Latency/P99(submit→reply)、Commit Delay(Append→commit 推进)、Recovery Time(已统计)。

> ⚠️ **不要报未实测的 QPS/P99**。说"量化了恢复时延，系统性 QPS/P99 压测在计划中"。

**回答模板**：
> "测试按 7 大类分层。选举测试控制投票响应验证票数边界；复制测试验证 prevLog 冲突/gap/多数派 commit；最有含金量的是恢复测试，用 apply.meta+snapshot.meta+WAL 三方组合模拟崩溃，验证 last_applied=max、只 replay 增量、快照外 key 不存在，并统计恢复时延。压测我目前量化了恢复时延，系统性 QPS/P99 在计划中。"

---

## 11. 性能优化（区分已做/可做）

| 优化 | 状态 | 提升 | 说明 |
|---|---|---|---|
| Batch AppendEntries | ✅部分 | 减少 RPC | 一次 GetRange 全量发送(`:751`)|
| Batch Apply | ✅ | 减少循环/flush | kBatchSize=128，每批 flush progress(`:856,895`)|
| Pipeline Replication | ❌ | 复制吞吐 | **同步逐 peer 等 ack(`:756-762`)，最大瓶颈** |
| Heartbeat | ✅基础 | 独立 fiber 50ms 续 lease | `HeartbeatLoop`(`:387`)，纯空心跳 |
| Snapshot | ✅ | 避免全量日志/加速恢复 | 后台 fiber+分块+读写屏障不阻塞读 |
| ReadIndex | ✅Lease | 读吞吐(lease 内零 RPC) | (`:418-423`)|
| Fiber 调度 | ✅核心 | 无锁/协程/免线程切换 | 全程 fb2::Fiber 无 mutex(README:470)|
| io_uring | ✅底层 | 异步 IO 减 syscall | helio(README:9,456)|
| 分片存储 | ✅ | 并行读写消除锁竞争 | `Shard(key)+Await(sid)`(`kv_state_machine.cc:52`)|
| WAL 分段+CRC | ✅ | 段级压缩/部分写检测 | `SegmentLogStorage+ComputeCrc32`(`:150`)|
| fsync 批量 | ✅部分 | buffer 累积一次 flush | `wal_writer.cc:101-127`|

**回答模板**：
> "已落地：apply 128 条批处理+批量 flush；ReadIndex Lease Read；后台 fiber 自动快照+分块 InstallSnapshot+读写屏障；WAL 分段+CRC+批量 fsync；KV 分片消除锁竞争；全程 fiber 无 mutex+io_uring。最大瓶颈是复制同步逐 peer 无 Pipeline，下一步引入异步发送+nextIndex 滑动窗口。"

---

## 12. 面试三轮拷问

**第一轮（是否真做过）**：整体架构/SET 链路；选举流程/随机超时；commit_index 推进；不丢数据。
→ 最像作者：说出函数名文件行号。暴露八股：只会"过半提交"答不出算法。

**第二轮（实现细节）**：election timer epoch；Lease 风险；SnapshotAnchor；Joint 双多数派两处代码；同步/异步复制。
→ 最像作者：主动暴露 trade-off。暴露八股：声称 Pipeline 却说不出 in-flight 窗口。

**第三轮（安全/极端/源码）**：
- Figure 8（**杀手锏**）：坦白 AdvanceCommitIndex 缺当前 term 检查+修复方案。
- Kill -9 恢复：term/voted_for fsync + WAL CRC 停部分写 + apply.meta + snapshot+delta replay。
- 网络分区旧 Leader：拿不到多数派 → 写卡住，ReadIndex 返回 0。
- 两 partition 各自 commit：不可能，commit 需过半两 partition 不能都过半。

**最像作者**：报得出默认参数(心跳50ms/选举[150,300]ms/lease100ms/batch128/chunk)；主动指出简化和 bug；解释"为什么"而非"是什么"。
**暴露八股**：把论文当实现(pre-vote/learner 未接入)；报假 QPS 追问崩；说做了 Pipeline 答不出同步异步。

---

### 12.5 核心概念速答

#### 三角色

Follower 被动接收 Leader 的 Heartbeat 和 AppendEntries，重置选举定时器；超时后变 Candidate 发起选举；赢得过半票变 Leader，负责接收客户端写、复制日志、推进 commit。角色切换统一走 `SetRole()`（`raft_node.cc:131`），管理定时器启停、心跳启停、vote_count 清零等。

#### 随机超时

`ElectionTimer`（`election_timer.cc:63`）每轮休眠 `[150,300]ms` 随机值。所有 Follower 同时超时的概率极低→先醒的先变 Candidate 抢票→避免 split vote 活锁。每次 `Reset()` 用 epoch 自增通知 fiber 重新随机（`:26-28`），全程无锁。

#### 一 term 一 Leader

一票制（`raft_node.cc:221-226`）：`voted_for` 非空且非请求者则拒绝。过半选举（`TryBecomeLeader :315-324`）：票数 >= N/2+1 才当选。同一 term 最多一人拿到过半数票→同 term 不可能有两个 Leader。

#### commit 条件

日志必须被多数派复制后才能提交。`AdvanceCommitIndex`（`:786-838`）：收集所有节点的 last_log_index 降序排列，取第 majority 大的值作为候选。**Figure 8 检查**（`:824-829`）：`candidate_term < current_term` 则拒绝。不能靠副本计数提交上一个 term 的日志，只有当前 term 的条目被多数派复制后，才顺带让之前的也变安全。

#### commit_index

等于 matchIndex（各 peer 的 `peer_last_log_index_[]`）+ 自己的 `LastIndex()`，降序排序后取第 majority 位的值。含义：至少有 majority 个节点已将此 index 及之前的日志复制到本地。这是 Raft 安全的数学基础——被多数派确认的日志在新 Leader 选举时必然还在多数派中。

#### Log Matching

Raft 的两个关键保证：① 不同日志里相同 index 的 entry 如有相同 term，则 command 必相同；② 任意 index 之前的日志完全一致。实现靠 Follower 在 `OnAppendEntries`（`:607-633`）中对 `prevLogTerm` 做匹配检查：不匹配→拒绝返回 `prev_log_index-1`→Leader 调整 nextIndex 重发→冲突时 `TruncateFrom` 截断再 Append。以 Leader 日志为准，最终一致。

#### Leader Completeness

被 commit 的日志永远不会丢。保证来自两个机制：① 投票时 Candidate 带 `last_log_term/index`（`:254-256`），Follower 比较日志新旧（`:228-239`），更旧者拒投；② 任何两个 quorum 必有交集（总票数过半+过半，和>总数）→ 获过半票的新 Leader 必然收到至少一个含所有已提交日志的节点的投票。

#### ReadIndex 强一致

Leader 记录当前 `commit_index_`→向所有 peer 发 ReadIndex RPC（`:457-460`）→收集响应：≥ majority 个 success 确认为合法 Leader；任何 peer 有更高 term 则退位（`:463-468`）→不够 majority 返回 0→成功后 `ExtendLeaderLease()`+`WaitForApplied(read_index)`（`:482-484`）确保本地已 apply 到该位→读到的数据一定是最新的。

#### Lease 风险

Leader lease 的快路径（`:436`）：`NowMs() < leader_lease_expire_` 直接读，跳过 ReadIndex RPC。但 `NowMs()` 依赖系统时钟，时钟漂移可能让本已不是 Leader 的节点仍认为 lease 有效，做出错误本地读。这是所有基于时钟的 lease 方案的固有风险，Raft 论文也不保证时钟正确性。

#### 读为何不写日志

读操作不改变状态机，写日志没意义（白消耗磁盘和网络）。ReadIndex 等价于"确认我此刻还是合法 Leader 且已 apply 所有已提交日志"——这不改任何状态，只是做一次身份验证和进度同步，确认通过后直接从本地 read。

#### Snapshot 触发

`RaftSnapshotManager` 的后台 fiber（`snapshot_manager.cc:91-101`）每 1 秒调用 `ScheduleCreateIfNeeded`：`last_index - snapshot_index >= log_gap_`（默认 `log_gap_=100000`，`:82`）时触发 `CreateSnapshot()`。触发阈值可配置。

#### 压缩后一致性

快照后日志被删除，但 AppendEntries 的 `prevLogIndex` 可能正好指向已删除位置。`SnapshotAnchor`（`log_storage.h:39-44`）保留被删的最后一条日志的 `(index, term)`。`GetTerm()` 先查 anchor `index` 是否匹配，命中则返回 anchor term。这样一致性检查（prevLogTerm 比对）在日志被压缩后依然有效。

#### InstallSnapshot 时机

`ShouldInstallSnapshot`（`raft_node.h:247`）：Follower 的 `next_index <= snapshot_index` 即触发。含义：Follower 需要的下一条日志已被 Leader 端压缩删除→无法通过 AppendEntries 追日志→只能把整个快照文件发给它。Leader 在 `ReplicateLog`（`:743-756`）里发 AppendEntries 之前先检查是否需要 InstallSnapshot。

#### 为何 Joint

直接切配置（Cold→Cnew）有新旧各自选主窗口→脑裂。假设 3→5 节点：切换瞬间，新配置节点认为 majority=3，旧配置节点认为 majority=2。如果网络同时分区，两个分区可各自选 Leader。Joint Consensus 引入中间态 Cold+new，任何决议（选举、提交）需两个配置**同时**过半数，杜绝这个窗口。

#### Joint 提交

`AdvanceCommitIndexJoint`（`:841-882`）：分别对 old_config 和 new_config 的 voter 集合执行多数派计算，得到 `old_commit` 和 `new_commit`，`candidate = min(old_commit, new_commit)`（`:865-867`）。取 min 是因为一条日志必须在两边都达到多数派才算安全——old 那边到了 100 但 new 那边只有 80，安全提交位只能是 80。加 Figure 8 的 term 检查（`:871-877`）。

#### Joint 选举

`TryBecomeLeader` 在 Joint 态的分支（`:296-313`）：`old_votes >= old_majority && new_votes >= new_majority`。必须同时拿到旧配置和新配置的过半数票才能当选。`StartElection`（`:264-269`）在 Joint 态下分别统计 `old_config_votes_` 和 `new_config_votes_`。

#### term/voted_for 持久化

`RaftStorage` 的写操作（`:128-137`）：序列化为 JSON→写 `.tmp` 文件→`fdatasync` 落盘→`rename(.tmp, meta.json)`。重启时 `Load()`（`:36-44`）恢复 term 和 voted_for。一票制依赖持久化：如果节点崩溃重启后忘票，就会在同一 term 投给另一个 Candidate，破坏 Election Safety。

#### Kill -9 恢复

① `meta.json` 已 fsync→term 和 voted_for 恢复（`raft_storage.cc:36`）。② `apply.meta` 恢复 last_applied（`raft_node.cc:46-48`）。③ 有 snapshot.bin 则 `LoadSnapshot` 恢复全量数据（`:52-71`）。④ WAL 逐条扫描，带 CRC32 校验，遇损坏停止（`segment_log_storage.cc:150`）。⑤ `ReplayUnappliedLogs` 重放快照后、last_applied 后的增量日志（`:884-897`）。

#### 分区旧 Leader

少数派分区的原 Leader 失去 quorum：写操作在 `ReplicateLog` 的 AppendEntries 循环中等不到多数派响应→返回失败。ReadIndex 在 `ReadIndex`（`:453-476`）中发 RPC 给 peers：网络不通或 peers 已在新分区选新 Leader（term 更高）→`success_count < majority` 返回 0→调用方返回 READONLY 错误。

#### AppendEntries 冲突

Follower 的 `OnAppendEntries`（`:607-633`）：① `prev_log_index > LastIndex`→gap，拒绝返回 last_log_index；② `prev_log_index` 位置存在但 `prev_log_term` 不匹配→冲突，拒绝返回 `prev_log_index-1`；③ 匹配成功→从冲突位 `TruncateFrom` 丢弃分歧部分→逐条 Append Leader 发来的 entries。整个过程以 Leader 日志为权威，Follower 无条件同步。

#### 心跳:选举比例

心跳间隔 `heartbeat_interval_ms_` = 50ms（`raft_node.h:358`）。选举超时随机 `[150,300]ms`（`election_timer.h:48`）。比例 3~6 倍：最短超时 150ms 也会看到至少 3 次心跳（50ms×3=150ms）。正常情况下心跳远在超时前到达并 Reset 定时器→Follower 几乎不可能因网络抖动误触发选举。

#### Leader Transfer

`StartTransfer`（`raft_node.cc:508-553`）：Leader 不再接受新写，强制 `ReplicateLog` 把当前日志推到 target。`IsTransferReady`（`:555-570`）检查 target 的 `match_index >= last_index` 已追上。追上后 `SendTimeoutNowToTarget`（`:589-605`）给 target 发 `TimeoutNow` RPC。target 收到后立即 `OnTimeoutNow`→`StartElection()`（`:360-378`）无缝接管。超时 `transfer_timeout_ms_=3000ms`（`:326`）自动取消。

#### FastCommitPath

`RaftEngine::SubmitCommand`（`raft_engine.cc:35`）：`PeerCount()==0`→走 `FastCommitPath`（`:45-53`）：`Append→AdvanceCommitIndex→ApplyCommittedLogs`。无 peer=无需 RPC 往返=单机延迟。多节点才走 `Append→ReplicateLog`（`:39-42`）。

#### apply 顺序

`ApplyCommittedLogs`（`:899-941`）：for 循环严格按 `last_applied_+1` 到 `commit_index_` 递增，一批 128 条。按 index 顺序 apply 保证 State Machine Safety——状态机的最终状态与日志的线性历史一致，与并发执行产生的结果不同。

#### WAL 部分写

`SegmentLogStorage` 扫描 WAL 段时，每条日志记录带 CRC32 校验。扫描到某条记录 CRC 不匹配→说明此处发生部分写（写了一半崩溃）→停止扫描，后续数据全部丢弃。恢复后 `TruncateFrom` 截断到最后一个有效位（`:150`）。

#### 原子写

所有关键元数据写操作遵循：写 `.tmp` 临时文件→`fdatasync` 确保数据落盘→`rename(.tmp, target)`（`:65`）。POSIX 保证 rename 是原子的（要么旧文件要么新文件，不会看到半写文件）。崩溃后要么 .tmp 没 rename（丢弃），要么 rename 完成（完整新文件）。

#### multi-raft 路由

`RaftGroupManager`（`raft_group_manager.h`）管理多个 `RaftGroup` 实例，每个 `RaftGroup` 是一个独立的 Raft 共识组。`ShardRouter` 用 DJB hash 将 key 映射到 `group_id`→不同 key 路由到不同 Raft 组→数据水平分片→每个组的 Raft 独立运行，组成员和 leader 可以不同。

#### fiber 优势

项目无 mutex（README:"No mutexes"）。所有状态变更跑在**单线程的 fiber 链**上，同一时刻只有一个 fiber 在执行→天然无数据竞争。fiber 切换是用户态操作，比 OS 线程切换快几个数量级。省去了锁的开销和并发 bug 的排查成本。代价是同步 RPC 的等待会阻塞整个 fiber（一个 peer 卡住，整个复制停）。

#### io_uring

Linux 的异步 IO 框架。通过 SQ（提交队列）和 CQ（完成队列）两个环形缓冲区，用户态和内核态共享内存通信。一次 `io_uring_enter` 系统调用可以提交多个 IO 请求、收割多个完成事件。相比 epoll（每个 IO 一次 syscall），大幅减少 syscall 次数和 context switch。

#### 分片消除锁竞争

key 通过 DJB hash 映射到 `EngineShard`（`kv_state_machine.cc Shard()`），每个 shard 绑定一个 proactor 线程。不同 shard 的操作在不同线程上执行→天然无竞争→无需加锁。同一个 shard 不同 key 的操作在同一线程排队→也无需锁。只在跨 shard 并行操作上有性能优势。

#### Figure 8

Raft 论文的 Figure 8 场景：Leader 不能靠副本计数提交上一个 term 的日志。如果旧 term 日志被提交但新 Leader 可能不包含它（因为新 Leader 当选需要最新日志），数据就丢了。本项目的实现（`:824-829`）要求 `candidate_term >= current_term` 才推进 commit_index。只有当前 term 的日志被多数派复制后，前一 term 的才"顺带"变安全。

#### 快照一致视图

`SnapshotBarrier`（`snapshot_barrier.h`）保证快照时刻的状态是"静止"的。`BeginWrite`：设 `writing_=true`→所有新写被 `BeginRead` 挡住→等 `readers_` 降到 0（所有已进入的写操作完成）。此时状态机不再被修改→`SaveSnapshot` 导出的是一致的数据。`EndWrite`：恢复 `writing_=false`→新写恢复。

#### commit vs applied

两个指针追踪日志执行进度。`commit_index_`：`AdvanceCommitIndex` 推进，标记"可以安全应用到状态机"的最高 index（已获得多数派确认）。`last_applied_`：`ApplyCommittedLogs` 推进，标记"已经真正执行"的最高 index（已写入 KV 存储）。正常情况下 `commit_index_ >= last_applied_`，差值就是待 apply 队列。

#### 最大局限

① 同步复制无 Pipeline：`ReplicateLog` 用 for 循环串行发 AppendEntries（`:756-762`），每个 peer 必须等回复才发下一个。任何时刻最多 1 个 in-flight 请求。peer 网络卡住→整个复制 fiber 阻塞→Leader 无法推进 commit。工业级实现（etcd/braft）用异步 Pipeline 允许多个 in-flight。② 已修复：~~缺 Figure 8 检查~~→当前实现已包含 term 检查。

---

## 13. 最终总结

### ① 一分钟
> "基于 Raft 实现分布式 KV。核心 RaftNode：Follower 随机 [150,300]ms 超时发起选举，过半当选；写由 Leader 编码日志、复制多数派、提交后 apply 到分片 KV；读通过 ReadIndex+Lease 保证线性一致无需写日志。还实现 Snapshot 压缩、InstallSnapshot、Joint Consensus。存储通过 ILogStorage/IStateMachine 解耦，底层 fiber+io_uring 无锁高并发。"

### ② 三分钟（加）
> "选举定时器 fiber 用 epoch 无锁 Reset。复制时 Follower 做 prevLogTerm 检查和冲突截断保证 Log Matching；commit 推进把 matchIndex 降序取第 majority 位。ReadIndex 快慢路径。Snapshot 后台 fiber 触发，压缩保留 SnapshotAnchor。成员变更 Stable→Joint→Stable 双多数派。term/voted_for/apply.meta 都 fsync+原子 rename，WAL 分段 CRC 精确恢复。"

### ③ 五分钟（再加）
安全性证明(Leader Completeness 推理链) + 恢复流程 + 测试体系 + **主动暴露两个 trade-off**（同步复制、AdvanceCommitIndex 缺 Figure 8 检查）。

### ④⑤ 33 道高频题（精简答案）
1. 三角色：Follower/Candidate/Leader。
2. 随机超时：避免 split vote 活锁。
3. 一 term 一 Leader：一票制+过半。
4. commit 条件：多数派复制且(应)当前 term。
5. commit_index：matchIndex 降序取第 majority 位。
6. Log Matching：prevLogTerm+截断。
7. Leader Completeness：投票日志检查+两过半相交。
8. ReadIndex 强一致：确认 leadership+WaitForApplied。
9. Lease 风险：时钟依赖。
10. 读为何不写日志：不改状态机。
11. Snapshot 触发：日志超 log_gap。
12. 压缩后一致性：SnapshotAnchor。
13. InstallSnapshot 时机：next_index≤snapshot_index。
14. 为何 Joint：避免新旧各自选主。
15. Joint 提交：双多数派。
16. term/voted_for 持久化：重启不破一票制。
17. Kill -9 恢复：fsync meta+CRC WAL+snapshot+delta。
18. 分区旧 Leader：写卡住 ReadIndex 返回 0。
19. AppendEntries 冲突：TruncateFrom 以 Leader 为准。
20. 心跳:选举比例：50ms:150~300ms=3~6倍。
21. Leader Transfer：追上后 TimeoutNow(`:490`)。
22. FastCommitPath：无 peer 省 RPC。
23. apply 顺序：State Machine Safety。
24. WAL 部分写：CRC32 停扫描(`:150`)。
25. 原子写：.tmp+fdatasync+rename(`:65`)。
26. multi-raft 路由：group_id key。
27. fiber 优势：无锁/轻切换/无竞争。
28. io_uring：异步 IO 减 syscall。
29. 分片消除锁竞争：Shard 定位并行。
30. 最大局限：同步复制无 Pipeline；缺 Figure 8 检查。
31. Figure 8：不能靠计数提交旧 term entry。
32. 快照一致视图：SnapshotBarrier。
33. commit vs applied：确认可 apply vs 已执行。

### ⑥ 最易追问源码
`AdvanceCommitIndex`(`:768`缺 term 检查)、`OnRequestVote`(`:190`)、`OnAppendEntries`(`:589`)、`ReadIndex`(`:411`)、`AdvanceCommitIndexJoint`+`TryBecomeLeader`(`:801,277`)、`SetStoragePath`+`ReplayUnappliedLogs`(`:36,836`)。

### ⑦ 必看源码
`raft_node.cc`(901行核心)、`election_timer.cc`(epoch)、`segment_log_storage.cc`(WAL/CRC/压缩)、`snapshot_manager.cc`+`snapshot_barrier.h`、`raft_apply_recovery_test.cc`。

### ⑧ 简历写法
✅ 可写：Leader Election(随机超时+epoch)、Log Replication(matchIndex 排序)、ReadIndex+Lease、Snapshot 压缩+分块 InstallSnapshot+读写屏障、Joint Consensus 双多数派、崩溃恢复(fsync+CRC WAL+snapshot+delta，11K<10s)、fiber+io_uring+分片。
❌ 别写：Pipeline Replication(同步)、具体 QPS/P99(未实测)、Pre-vote/完整 learner(未实现)。
⚠️ 强一致读须注明默认 GET 是 local。

---

## 核心竞争力

本项目质量足够进大厂面试，恢复测试和存储抽象设计扎实。**最大竞争力不是"实现多完整"，而是"能清楚说出每个简化的 trade-off 和已知缺陷"**——这是区分"项目作者"和"八股背诵者"的分水岭。主动抛出"AdvanceCommitIndex 缺 Figure 8 检查"和"复制是同步的"，会让面试官立刻确认你真写过、真懂。
