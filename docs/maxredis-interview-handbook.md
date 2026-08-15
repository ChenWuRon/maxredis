# maxredis 字节 / 京东基础架构组源码级面试作战手册

> 本手册所有源码引用均以 `git rev 49eef12 (code final)` 为基准。
> 所有"能说 / 不能说"的边界，都基于 L1~L4 真实性等级。

---

## 00 真实性等级（面试全程的总开关）

| 等级 | 定义 | 面试话术 |
|---|---|---|
| L1 | 生产运行路径 | "实现了，实际跑通了" |
| L2 | 已实现、主要靠测试触发 | "实现了，测试覆盖了" |
| L3 | Stub / 接口 / 未接线 | "接口存在，但没有接进主路径" |
| L4 | 明确缺陷 | 主动说，不要等被问出来 |

本项目各能力定级（详见 30 章审计表）：

- Raft Election / Replication / Commit / ReadIndex / Lease / CheckQuorum / Joint Consensus / Leader Transfer：**L1**（源码在 `server/raft/raft_node.cc`，多节点测试覆盖）
- WAL + CRC32C + torn write 截断 + replay：**L1/L2**（L1 无 `raft_dir` 的内存路径 + L2 有目录的持久化路径，均真实运行；但没有生产级大规模验证）
- Snapshot 创建侧（Sender / Manager / barrier / compact）：**L1~L2**
- Snapshot 接收侧（InstallSnapshot 生产初始化）：**L4** —— `RaftGroup::InitStorage` 从未调用 `node_.SetSnapshotReceiver()`，`snapshot_receiver_` 恒为 `nullptr`，生产节点收到 InstallSnapshot 一定拒绝
- RESP 兼容（`ParseMultiBulk` 正式路径）：**L1**；`RedisParser` 类：**L2/L3**
- Memcached：**L4**（parser 存在，store 命令断链）
- io_uring：主路径 **L1**；SQPOLL / RECVSEND_BUNDLE / 默认 multishot：**未启用**
- AOF：**L4**（`AofWriter::Flush` 只有 `fflush` 无 `fsync`；`Service::Expire` 不写 AOF）
- WaitForApplied 超时后仍返回：**L4**
- Benchmark：真实数据，但口径是"单连接 Python 串行 + 2 核 + 无 WAL fsync"，**不能**推出生产性能

---

## 01 项目定位

一句话：**用 C++/io_uring/Fiber 自研网络与调度底座，在上面从零实现了一个 Raft 一致性核心（含 WAL、snapshot、ReadIndex、成员变更），并做成兼容 Redis RESP 的分布式 KV。**

面试中的自我定位策略：

- 不要说"我做了个 Redis 替代品"。
- 要说"我做了个 **consensus + storage 的教学级生产化实验**"：Raft 部分是核心资产，KV/RESP 是验证载体。
- 强项：Raft 并发模型（锁内状态机 + 锁外 RPC）、WAL 恢复、真实跑通了 redis-cli。
- 软肋（要主动管理预期）：性能只有 4K QPS 级别、snapshot 接收端未接线、AOF 无 fsync。

---

## 02 项目介绍（4 个时长版本）

### 15 秒版

"我用 C++ 写了一个分布式 KV：底层用 io_uring + 协程做网络和调度，数据分片到单写者 shard；核心是从零实现的 Raft——选举、日志复制、ReadIndex 线性化读、成员变更、分段 WAL + CRC 校验 + 崩溃恢复，协议兼容 Redis RESP，用 redis-cli 就能访问。"

### 30 秒版

"（15 秒版）+ 工程上最花心思的是并发模型：Raft 状态机用 fiber mutex 串行化，但所有 RPC 都放在锁外发送，返回后再重新加锁按 term/role 校验有效性，避免持锁 RPC 的 AB-BA 死锁和临界区膨胀。持久化是 64MB 分段 WAL，每条记录带 CRC32C，启动扫描遇 torn write 截断尾部再 replay；apply.meta 的刷盘顺序严格在 WAL 之后。"

### 1 分钟版

"（30 秒版）+ 一致性细节：commit 只推进 current term 的多数派复制点（Figure 8 问题），旧 term entry 靠后续 current term entry 间接提交；Leader Lease 不是'时间没到我就是 leader'，而是每次多数派 ACK 才续租，配合 CheckQuorum（600ms 内拿不到多数派 ACK 就主动 step down）防止分区脑裂；成员变更走 Stable→Joint→Stable，joint 期间投票和 commit 都要同时满足新旧两个多数派；Leader Transfer 要求目标 match_index 追上 leader 再发 TimeoutNow。性能方面很坦诚：2 核 VM 单连接 Python 客户端测得 SET 4K / GET 4.7K QPS，这不是生产压测，链路里 fsync、多连接、Raft 复制都没有全开。"

### 3 分钟版（源码级）

"（1 分钟版）+ 拉一条完整链路：redis-cli 发 SET → `Connection::InputLoop` → `ParseMultiBulk`（dragonfly_connection.cc:473）→ `DispatchCommand` → `Service::Set`（main_service.cc:563）→ `RaftEngine::SubmitCommand` → `RaftNode::SubmitEntry`（raft_node.cc:1217）锁内 append 本地 WAL，然后 `ReplicateLog`（1233）：锁内按 `peer_hb_ok_` 对健康 peer 排序、构造每 peer 的 AppendEntries 请求，锁外串行发送，`low_latency` 模式下凑够多数派立刻 break（1337）——partitioned peer 不阻塞写关键路径；返回后重新加锁，先检查 `max_peer_term > current_term` 则 step down（1392），否则按 matchIndex 更新、nextIndex 回退 `min(next-1, follower_last+1)`（1410），`AdvanceCommitIndexLocked`（1440）取多数派位置但要求 candidate entry 是 current term（Figure 8），最后 `ApplyCommittedLogsLocked`（1568）批量 128 条 apply 到状态机，刷盘顺序先 WAL 后 apply.meta（1627-1629）。读路径：GET → ReadIndex（848），lease 有效走 commitIndex 快路径，无效向多数派发 ReadIndex RPC 确认后 `WaitForApplied`（921）等本地 apply 追上。这套'锁内状态机 + 锁外 RPC + 返回后重校验'的模式贯穿选举、心跳、复制、读索引四条路径，是我认为最有技术含量的部分。"

---

## 03 项目架构图（面试画白板用）

```
                 Client (redis-cli / Python socket)
                              │ RESP over TCP
                              ▼
┌─────────────────────────────────────────────────┐
│  Proactor (io_uring) 线程 × conn_threads(=2)      │
│    ┌──────────────────────────────────────┐      │
│    │ Fiber scheduler (boost::context)      │      │
│    │  ┌──────────┐  ┌──────────┐          │      │
│    │  │Conn Fiber│  │Conn Fiber│ ...      │      │
│    │  └────┬─────┘  └────┬─────┘          │      │
│    │       ▼             ▼                 │      │
│    │  ParseMultiBulk → DispatchCommand      │      │
│    │       ▼                               │      │
│    │  Service (SET/GET/DEL/EXPIRE/INFO...)  │      │
│    └───────┬───────────────────────┬───────┘      │
└────────────┼───────────────────────┼──────────────┘
             │ SubmitCommand          │ Schedule(key)
             ▼                        ▼
┌──────────────────────┐   ┌──────────────────────────┐
│ RaftEngine           │   │ EngineShardSet            │
│  └ RaftGroup         │   │  Shard0 ── Shard1 ...     │
│    └ RaftNode        │   │   (单写者 DbSlice)         │
│      ┌ mutex_ 串行化  │──▶│   FiberQueue 跨线程投递    │
│      │ heartbeat fiber│  └──────────────────────────┘
│      │ election timer │
│      ▼                │
│  SegmentLogStorage    │
│  (64MB segments,      │
│   CRC32C, manifest)   │
│  apply.meta / meta.json
│  SnapshotManager      │
└──────────┬───────────┘
           │ TCP RPC (magic/type/seq/len/CRC) / LocalTransport
           ▼
   其他节点 RaftNode（相同结构）
```

必须主动讲清楚的架构事实：

- **没有独立 Raft 线程**：heartbeat 是一个 fiber，选举是 timer 回调，都跑在 Proactor 线程的 fiber 调度器上。
- **没有独立 Connection 线程**：每条连接一个 fiber，共享所属 Proactor 的 OS thread。
- Fiber ≠ 并行：同一 Proactor 上的 fiber 只是协作式并发。
- 跨 shard / 跨线程用 FiberQueue 投递任务，DbSlice 是单写者模型。

---

## 04 SET 完整源码调用链（源码级）

```
redis-cli 发送 *3\r\n$3\r\nSET\r\n...\r\n
1. Connection::InputLoop (dragonfly_connection.cc:244)
2. Connection::ParseRedis (313) → ParseMultiBulk (473)
3. ConnectionContext::AddParsedCommand (503)
4. Connection::DispatchCommand (575) → service_->DispatchCommand
5. Service::DispatchCommand → 查 registry_ → Service::Set (main_service.cc:563)
6. Service::Set:
   a. engine_.SubmitCommand(cid, args) (572)
      → RaftEngine::SubmitCommand (raft_engine.cc)
      → CommandEncoder::Encode → cmd->Serialize()
      → group_.node().SubmitEntry(LogEntry) (raft_node.cc:1217)
7. RaftNode::SubmitEntry (1217):
   ├─ [锁内] 非 Leader 直接返回 ERROR（客户端收到 READONLY）
   ├─ [锁内] entry.term = current_term; index 由 log 分配
   ├─ [锁内] log_storage_->Append(...)（本地 WAL 先落盘，见 14 章）
   └─ 出锁 → ReplicateLog(low_latency=true) (1230)
8. ReplicateLog (1233) — 最多 8 轮收敛：
   ├─ Phase1 [锁内]：GetPeerIds → ResizePeerArrays → 按 peer_hb_ok_
   │   稳定排序健康 peer 优先 (1273-1278)；每 peer 构造 prev + entries
   │   （GetRange(next, kMaxAppendBatch=128)）
   ├─ Phase2 [锁外]：串行 SendAppendEntries；low_latency 下
   │   successes >= majority 立即 break (1337-1339)
   ├─ Phase3 [锁内]：
   │   ├─ max_peer_term > cur → BecomeFollowerLocked (1392-1395)
   │   ├─ role/term 变了 → 丢弃结果 (1397)
   │   ├─ 成功: matchIndex=prev+len, nextIndex=match+1
   │   ├─ 失败: nextIndex = min(next-1, hint+1) 指数回退 (1406-1417)
   │   ├─ AdvanceCommitIndexLocked (1421)
   │   └─ result = ApplyCommittedLogsLocked (1424)
   └─ 有失败且未收敛 → 下一轮
9. AdvanceCommitIndexLocked (1440):
   收集 self.last + peer_last_log_index，排序取 majority-1 位置，
   candidate 是 current term 才推进 commit（Figure 8，1467-1479）
10. ApplyCommittedLogsLocked (1568):
    while last_applied < commit_index:
      GetRange 批量 128 → 逐条 state_machine_->ApplyLogEntry(entry)
      → KvStateMachine → 按 key 路由到对应 Shard apply（跨线程投递）
      → 刷盘：apply_progress_.UpdateMemoryOnly → log_storage_->Flush()
         → apply_progress_.Flush()（顺序严格 WAL 先行，1627-1629）
11. 回到 Service::Set：
    SendStored → persistence_manager_->RecordCommand（AOF，无 fsync！）
    → snapshot_fiber_.NotifyWrite()
12. 连接 fiber 把 +OK 写回 socket → 客户端收到响应
```

**面试官会从这里问的 3 个点：**

1. "为什么 SET 要等 apply 完才回客户端？"——因为 `SubmitEntry` 同步返回 `ApplyCommittedLogsLocked` 的结果，是**同步写路径**：客户端感知的延迟 = WAL append + 多数派 RPC + state machine apply。这也解释了 benchmark 里 SET P50 241μs。
2. "AOF 和 Raft WAL 什么关系？"——本项目里 Raft WAL 是权威持久化（有 fsync 路径），AOF 是 KV 层的历史遗留旁路（无 fsync）。面试时主动点出这个双写问题，不要假装是精心设计。
3. "AOF 写到一半崩了怎么办？"——因为 AOF 无 fsync 且恢复以 Raft WAL/apply.meta 为准，AOF 不是恢复源，这是缺陷不是特性。

---

## 05 GET / ReadIndex 完整调用链

```
GET key
1. 同 SET 的解析路径 → Service::Get (main_service.cc:591)
2. if FLAGS_linearizable_read:  (599)
     ri = engine_.ReadIndex() → RaftNode::ReadIndex (raft_node.cc:848)
     ri == 0 → SendError("READONLY ... not leader")
3. RaftNode::ReadIndex (848):
   ├─ [锁内] 非 Leader → return 0
   ├─ [锁内] NowMs() < leader_lease_expire_ → 快路径：
   │    read_index = commit_index_（零 RPC，852-865）
   ├─ 否则慢路径：
   │    [锁内] 记 current_term + request_id + peers
   │    [锁外] 向所有 peer 串行 SendReadIndex
   │           peer 侧 OnReadIndex (638)：term 匹配且非 Candidate 即成功
   │    [锁内] 重校验：max_peer_term > cur → step down 返回 0 (896-900)
   │           role != Leader || term 变了 → 返回 0 (901)
   │           多数派确认才取 commit_index (904-913)
   │           并 ExtendLeaderLeaseLocked（多数派确认才续租！917-918）
   └─ WaitForApplied(read_index) (921)
4. WaitForApplied (926): 循环 { [锁内] ApplyCommittedLogsLocked;
     last_applied >= target 即返回 }
     —— 5 秒超时后【仍然返回】(937-941) ← L4，见 29 章
5. 回到 Service::Get：execute_async=1 → engine_.Schedule(0, key, cb)
   → 按 key 路由到 owner shard 的 FiberQueue → 单写者 DbSlice.Find
6. 结果通过 ConnectionContext 回写 RESP
```

**读路径为什么必须讲 wait-for-apply 而不是只讲 ReadIndex：**
ReadIndex 只保证"确认的 commit_index 是已提交的"，但本地状态机可能还没 apply 到那里。`WaitForApplied` 补上了"apply 追上"这一步——这才是线性化读的完整闭环。

---

## 06 Raft 状态机（画图 + 触发条件 + 源码位置）

```
              [Follower]
  election timer 300~1200ms 超时 (election_timer.h:51)
      │  BecomeCandidateLocked: SetState(term+1, 自己) 一次 fsync
      ▼
             [Candidate]  vote_count=1, 向所有 peer 发 RequestVote
      │ 多数派赞成 ──────────▶ [Leader]  BecomeLeaderLocked
      │ 更高 term 的消息 ─────▶ [Follower] (BecomeFollowerLocked)
      │ 本轮失败 ────────────▶ [Follower] 退回去等新 timer (540-545)
      │ timer 再超时 ────────▶ [Candidate] 新 term 再选 (BecomeCandidateNewTermLocked)
      ▼
[Leader]: 心跳 fiber 每 50ms (raft_node.h:461)
      │ 收到更高 term ─▶ step down
      │ CheckQuorum: 600ms 无多数派 ACK ─▶ StepDownLocked (779-783)
      │ Leader Transfer 收到 TimeoutNow ─▶ 立即 StartElection (650-672)
```

状态转换的硬规则（背诵）：

1. term 只增不减；`BecomeFollowerLocked` 在 `term > current` 时**一次 fsync 原子写 (term, voted_for="")**（318-324）——避免"新 term 已持久、旧票还在"的选举安全窗口。
2. Same-term step down **保留** voted_for（325-326），否则同 term 内可能重复投票。
3. Candidate 当选唯一入口 `TryBecomeLeaderLocked`（550）；joint 状态需新旧两个多数派。
4. 所有持久化投票（term/voted_for）都在 grant 之前落盘（441-443）。

---

## 07 Election 源码追问

**Q1：选举超时是多少？为什么是这个值？**
A：300~1200ms 均匀随机（`election_timer.h:51` 的 `dist_{300, 1200}`）。下限必须远大于心跳间隔 50ms + 心跳 RPC 超时（150ms），否则正常网络抖动会误触发选举；随机化是为了避免 split vote 后锁步。**雷区**：不要背成 150~300ms（旧文档值，代码已改）。

**Q2：你发 RequestVote 之前要做什么？**
A：三件事，都在锁内（StartElection Phase1，453-478）：① `BecomeCandidateLocked`——term+1 和 voted_for=self 合并成**一次 fsync**（335-342）；② 取 `last_log_index/last_log_term` 放进请求；③ 抓取当前 peer 列表。然后**出锁**再发 RPC。

**Q3：为什么投票请求要带 last_log_index / last_log_term？**
A：Log Up-to-Date 判断。接收方 `OnRequestVote`（406-447）：candidate 的 last term 更小，或 term 相同但 index 更小 → 拒绝。否则会出现日志更旧的节点当选、覆盖已提交数据的风险。

**Q4：如果选举失败，你是继续待在 Candidate 还是退回 Follower？为什么？**
A：退回 Follower（540-545）。如果留在 Candidate，两个同步的候选会永远互相拒绝——Candidate 会拒绝同 term 的投票请求，而 Follower 会授予更高 term 的票。退回 Follower + 随机 timer 是打破 split-vote 锁步的关键。这个设计是源码注释里明写的工程决策。

**Q5：RPC 返回时你发现 peer term 比你高，怎么办？**
A：Phase3 第一个检查：`max_peer_term > current_term` → `BecomeFollowerLocked(max_peer_term)`（509-511），并且**丢弃本次选举结果**，不管拿了多少票。

**Q6：你的投票是持久化的吗？**
A：是。`storage_.set_voted_for(...)` 在返回 grant 之前 fsync（441-443）。Raft 的 Election Safety 要求：已经投票的票必须在崩溃后仍然有效，否则同一 term 两个节点都可能拿到多数票。

---

## 08 Log Replication 源码追问

**Q1：提交一条 entry 到 quorum 的完整路径？**
A：`SubmitEntry`(1217) → `ReplicateLog`(1233) 的三阶段循环（最多 8 轮）→ `AdvanceCommitIndexLocked`(1440) → `ApplyCommittedLogsLocked`(1568)。见 04 章调用链。

**Q2：为什么 ReplicateLog 里要分 low_latency 和 catch-up 两种模式？**
A：`low_latency=true` 是客户端写路径：健康 peer 优先排序（1273-1278）+ 凑够多数派立刻 break（1337-1339），partitioned peer 不能拖慢写。`low_latency=false` 是心跳 catch-up 路径（805-806 调用）：给所有落后 peer 补日志，不 break，但**跳过**上一轮心跳没 ACK 的 peer（1346），避免死 peer 的 RPC 超时把心跳 fiber 饿死。这是"写延迟"和"追赶吞吐"的明确取舍。

**Q3：为什么心跳里还要补日志？**
A：`HeartbeatTickImpl` 里 peer 回带 `last_log_index`（634-635），如果 < leader last，`need_replicate=true`（753-754），心跳 fiber 出锁后调 `ReplicateLog(false)` 补发（805-806）。这样刚恢复的 follower 不用等下一次写也能追上，commit 也能在无写流量的情况下推进（heartbeat ACK 会更新 match 数组并调 `AdvanceCommitIndexLocked`，756）。

**Q4：Follower 收到 AppendEntries，prevLogIndex 不匹配怎么处理？**
A：`OnAppendEntries`（1084）三步：① `prev_log_index > my_last` → 拒绝并回带 my_last（1110-1113）；② `GetTerm(prev_log_index) != prev_log_term` → 拒绝并回带 `prev_log_index - 1` 作为回退 hint（1114-1118）；③ 逐条比对，第一个 term 冲突处 `TruncateFrom(entry.index - 1)` 再 append（1124-1138）。`GetTerm` 还覆盖 snapshot anchor 覆盖的已 compact 前缀（1108-1109 注释）。

**Q5：nextIndex 回退的边界是什么？**
A：`backoff = min(next - 1, hint + 1)`，且 `backoff < 1` 时钳到 1（1409-1413）；如果 `backoff <= last_snapshot_index_`，把 match 标记为 snapshot index，强制走 InstallSnapshot 路径（1415-1416）。三种回退来源：通用 -1、follower 的冲突 hint（-1 条）、snapshot 兜底。

**Q6：为什么 entry 要在锁内拷贝？**
A：`r.entries = log_storage_->GetRange(...)` 在锁内做拷贝（1303-1305），RPC 在锁外读的是副本，否则锁外读 log 时可能被并发 append/truncate 改掉——这是锁内状态机模型的必要纪律。

**Q7：为什么 leader 发请求时 leader_commit 是锁内捕获的旧值？**
A：`req.leader_commit = commit_index_` 在 Phase1 锁内捕获（1367），出锁后 commit 可能已推进；follower 侧会做 `min(leader_commit, my_last)` 且只增不减（1153-1156），所以旧值只是"少推进一次"，下次心跳/复制会补上——安全性无损。

---

## 09 Commit / Figure 8

**核心代码：`AdvanceCommitIndexLocked`（raft_node.cc:1440-1483）**

```
indexes = {self.last_index} ∪ {peer_last_log_index[]}
sort desc → candidate = indexes[majority-1]
if candidate > commit_index:
    term = GetTerm(candidate)
    if term != 0 && term < current_term:   ← Figure 8 检查
        return  // 不推进！
    commit_index = candidate
```

**面试标准回答：**
"commitIndex 的推进有两个条件：① 该位置被多数派复制（sort 后取 majority 位置）；② 该位置 entry 属于当前 term。第二个条件就是 Raft 5.4.2 / Figure 8：旧 term entry 即使被多数派复制，也不能仅凭复制数直接提交，因为它可能被未来更高 term 的 leader 覆盖（旧 leader 复制了它但没提交就死了，新 leader 用自己 term 的 entry 覆盖同一位置）。旧 term entry 只能等一个 current term entry 在它之后被提交时**间接提交**。"

**面试官追问："那旧 term 的 entry 什么时候才会被 apply？"**
A：`ApplyCommittedLogsLocked` 从 `last_applied+1` 顺序 apply 到 `commit_index`（1575），所以只要 commit_index 越过了旧 entry，它就会被 apply。间接提交的意思是：客户端命令最终会执行，只是"提交"这一动作不是它自己触发的。

**追问："你这个实现里，间接提交有没有问题？"**
A：有一个细节要诚实说：本实现把"不推进"当成了**返回**——即这一轮 commit 完全不动，而不是像标准算法那样向前找最近的 current-term entry。后果是"间接提交"只能发生在**之后某轮**里 current term entry 到达 majority 位置时（或者 leader 在自己 term append 了新 entry 后）。如果 leader 当选后自己 term 一条新 entry 都没 append（比如只读流量），旧 term 的已多数派复制 entry 会一直不提交。这是实现取舍，不是数据不安全，但延迟了提交。**（这是主动展示源码理解的加分点，不要说"我实现完全等同论文"）**

**追问："那心跳里推进 commit 也一样有 Figure 8 检查吗？"**
A：有。心跳 Phase3 调的是同一个 `AdvanceCommitIndexLocked`（756），检查逻辑共享，两条路径都不会绕过。

**追问："joint 状态下的 commit 怎么算？"**
A：`AdvanceCommitIndexJointLocked`（1485-1525）：对 old config 和 new config **分别**算多数派位置，取两者的 min 作为 candidate，再做同样的 current-term 检查。这是 joint consensus 的正确性要求：joint 期间 commit 必须同时被两个配置的多数派接受，防止新旧配置各自认为达到多数派。

---

## 10 ReadIndex / Lease / CheckQuorum（三个一起背，因为它们是同一套可见性设计）

### 三者的分工（面试官必问"是不是重复"）

- **ReadIndex**：线性化读的协议动作——读之前向多数派确认"我还是 leader / commit 到哪了"。
- **Lease**：ReadIndex 的**快路径优化**——多数派 ACK 续租 100ms（`lease_ms_=100`，raft_node.h:386），租约内读直接取 commitIndex，零 RPC。
- **CheckQuorum**：写/可见性方向的**安全兜底**——600ms（`check_quorum_ms_=600`，raft_node.h:390）拿不到多数派 ACK 就 step down。

**"Lease 和 CheckQuorum 是不是重复？"标准回答：**
不是。CheckQuorum 是被动的、只负责"分区 leader 尽快退位"，它不管读；Lease 是主动的、服务读快路径。两者都建立在同一事实源上：**多数派 ACK**（`last_majority_ack_ms_`）。Lease 的续租只在 `ack_count >= majority` 时发生（774-778），慢路径 ReadIndex 的续租也只在多数派确认后（917-918）——所以"时间没到"背后永远是"多数派真的见过我"，不是纯本地时钟。

### Lease 安全性的关键设计

1. **续租条件**：只有心跳多数派 ACK 或 ReadIndex 多数派确认才 `ExtendLeaderLeaseLocked`（946-948）。隔离分区里的 leader 心跳拿不到多数派，租约自然过期，读快路径关闭。
2. **单调时钟**：`NowMs()` 用 `std::chrono::steady_clock`（957-963），NTP 跳变不会凭空延长租约。
3. **租约到期 ≠ 降级**：过期只是"读要走慢路径"，真正降级靠 CheckQuorum 或收到更高 term。

### WaitForApplied 的 L4 必须主动讲

`ReadIndex()` 末尾 `WaitForApplied(read_index)`（921-922），内部 5 秒超时（`kWaitForAppliedTimeoutMs=5000`，raft_node.h 区域 40-44）后**仍然 return**（937-941）。即：分区状态下，一个拿不到多数派确认的读最终可能返回**本地可能过期的值**，而不是返回错误。

**标准话术**："ReadIndex + Lease 的读路径实现了，但有一个我明确知道的技术债：WaitForApplied 5 秒超时后代码仍然返回，调用方会继续用本地值响应。严格说这在分区场景下可能返回 stale read。正确做法是超时返回错误码，让 Service::Get 回 READONLY。我没改它，因为主流程没有专门的分区读压力测试，超时路径没被真实触发过——但如果面试官现在问，我会把超时返回 ri=0 修掉。"

### 面试官攻击点与防守

**攻**："你的 lease 只有 100ms，心跳 50ms，那 lease 过期后每个读都要走慢路径 RPC，性能不是崩了？"
**守**："不会。心跳每 50ms 一轮，只要多数派正常 ACK，每轮都会续租 100ms。租约窗口 100ms ≈ 2 个心跳周期，正常网络下读几乎永远命中快路径。100ms 是个保守值——短租约换来的是分区后最多 100ms 就关闭快路径，读的 stale window 更小。这是安全性和读延迟的取舍。"

**攻**："CheckQuorum 600ms 就 step down，但 follower 的选举超时是 300~1200ms，会不会出现 leader 退了但没人当选的真空期？"
**守**："会，这是刻意的。真空期内写入会被拒（READONLY），读也会因为 ReadIndex 慢路径凑不齐多数派而失败——宁可不可用，不可不一致。Raft 本来就把可用性让位于一致性。"

**攻**："ReadIndex 慢路径在 joint 状态下的多数派是怎么算的？"
**守**："`ReadIndex` 的慢路径用的是 `cluster_config_.voters`（904）——joint 状态下这是 old config。严格讲 joint 期间的领导权确认应该覆盖两个配置，我这里用的是 old config 多数派。这个我知道，属于成员变更窗口期的读确认简化，不是完整实现。**（这是源码事实：raft_node.cc:904 确实只用了 cluster_config_。主动说简化，不要说完整支持）**"

---

## 11 Joint Consensus

**代码三件套：**
- `BeginConfigChange`（208-238）：Step1 存 joint_config 并 append CONFIG_CHANGE entry；Step2 验证目标一致后 append finalize entry。
- `ApplyCommittedLogsLocked` 里 CONFIG_CHANGE 分支（1586-1612）：Step1 进入 kJoint 并**持久化 joint 状态**（`storage_.SetJointConfigState`，1606）；Step2 切回 kStable。
- `MaybeAutoFinalizeJointLocked`（1637-1653）：joint entry 提交后 leader 自动追加 finalize entry（学 etcd）。

**为什么必须 Stable→Joint→Stable（不能一步换配置）？**
一步替换时，old 和 new 两个多数派可能**同时**存在：old 多数派选出老 leader，new 多数派选出新 leader，双主。Joint 阶段要求投票（550-582）和 commit（1485-1525）都满足**两个配置各自的多数派**，把危险窗口压缩到"两个多数派集合的交集"上。

**源码级细节（加分点）：**
1. joint 配置作为硬状态持久化，重启后 `SetStoragePath` 恢复 `config_state_`，可以续跑 Step2（124-130）。
2. 投票计数 `old_config_votes_ / new_config_votes_` 分别统计（519-530）——一个 peer 可能同时在两个配置里，要分别计数。
3. auto-finalize 有 current-term 检查（1645）：joint entry 不是自己 term 提交的就不追加 finalize，防止新 leader 误操作。

**雷区**：不要说"我支持动态加减节点的运维操作"。本实现只有配置变更的协议机制（测试驱动），没有真正的运维 CLI/管理面去触发它。面试时说"机制实现了、测试覆盖了，管理面没有"。

---

## 12 Leader Transfer

**代码**：`StartTransfer`（965-1019）→ `HeartbeatTickImpl` 的 transfer 分支（787-800）→ `SendTimeoutNowToTarget`（1061-1082）→ 目标端 `OnTimeoutNow`（650-672）→ `StartElection`。

**流程**：
1. 校验：leader、target 是 peer、没有进行中的 transfer。
2. 立即 `ReplicateLog(false)` 给 target 补日志。
3. `IsTransferReadyLocked`（1026-1037）：`peer_last_log_index_[target] >= my last_index` 才发 TimeoutNow——**目标必须日志追平**，否则它当选后可能把 leader 未复制的 entry 判为无效，甚至需要回滚已提交（标准 Raft 的 transfer 前置条件）。
4. 目标收到 TimeoutNow：term 检查后**出锁**立即 `StartElection()`（669-670）——它不需要等随机 timer，所以转移快。
5. 超时保护：3 秒（`transfer_timeout_ms_=3000`，raft_node.h:399）没完成就 cancel（1051-1059）。
6. 中途任何角色变化（step down）都会 `CancelTransferLocked`（282、298、305）。

**用途话术**：滚动升级/停机维护时把领导权平滑交给指定节点，避免"随机选主"的不确定性和一次选举空窗。

**追问**："为什么要让目标立即选举，而不是直接传位？"
A：Raft 没有"传位"概念，leader 身份只能通过多数派投票获得。TimeoutNow 只是把目标的 timer 提前到 0，它仍然要完成一次完整选举（term+1、投票、多数派）——安全性和正常选举完全一致，只是不用等随机超时。

---

## 13 Snapshot

**创建侧（L1~L2，真实运行）**：
- 驱动：`RaftGroup::InitStorage` 里的 `raft_snapshot_driver` fiber 每 ~1s 调 `CreateSnapshotIfNeeded`（raft_group.cc）。
- 入口：`RaftNode::CreateSnapshotIfNeeded`（1655-1674）。
- **边界是 last_applied**（1663）：snapshot 只能覆盖已 apply 的 entry，绝不覆盖未提交的 log tail。为什么？snapshot 会 compact 掉 WAL 前缀，如果覆盖了未提交 entry，compact 后该 entry 无法再被复制/提交——数据丢失。
- 流程：`SnapshotBarrier` 冻结全 shard 一致性视图 → `StateSerializer::Export` 各 shard → 合并 → metadata → `CompactLogs`（segment_log_storage.cc:420）→ 删旧 segment、更新 manifest。

**接收侧（L4，必须主动承认）**：
- `OnInstallSnapshot`（1163-1215）完整实现了 chunk 接收、LoadSnapshot、commit_index/last_applied 更新、WAL Clear + SetSnapshotAnchor。
- **但是**：`snapshot_receiver_` 默认为 `nullptr`（raft_node.h:423），`SetSnapshotReceiver`（226-227）在整个非测试代码里**没有任何调用点**；`RaftGroup::InitStorage` 没有接它。所以生产初始化后的节点收到 InstallSnapshot 会直接 `return {..., false}`（1178-1181）。
- **话术**："snapshot 发送/创建路径是完整跑通的，接收路径的处理逻辑也写了、单测覆盖了，但接收端在生产初始化里没有挂 receiver——一个落后太多的 follower 目前不能通过 InstallSnapshot 追数据，这是我在 README 里列的技术债之首。"

**追问防守**："为什么 follower 落后太多必须走 snapshot？"
A：`ShouldInstallSnapshot(next, last_snapshot_index_)`（1288）：nextIndex 回退到 snapshot index 之下时，日志前缀已被 compact，AppendEntries 无法提供 prev entry，只能装 snapshot。而且 nextIndex 逐条回退 O(N) 收敛太慢，snapshot 是 O(1) 跳跃式追赶。

---

## 14 WAL

**格式**：`RecordHeader{index, term, size, crc32}` + payload；segment 64MB（`kMaxSegmentBytes`，segment_log_storage.h:135），满则 `RollSegment`（212-221：关旧段 → manifest 记 current_segment → 开新段）。

**Append 路径**（`SegmentLogStorage::Append`，271-311）：
1. 内存 entries_ 先 append（O(1) 摊还）
2. `writer_->Append` 写盘
3. 持久化策略二选一：`fsync_per_append_` 默认**每条 fsync**；`fsync_interval_ms>0` 时走 page cache + 后台 fiber 周期 fsync（raft_group.cc 的 wal_flush_fiber）——对应 Redis AOF 的 always/everysec。
4. `index_.Add` 记 (index → segment_id, offset)，GetTerm 接近 O(1)。
5. 超 64MB 滚段。

**Torn write 识别与截断**（`ScanSegment`，137-189）：
扫描顺序检查四件事，任一失败即**停止扫描，尾部从此截断**（靠后续 append 覆盖 / TruncateFrom 逻辑）：
1. 半条 header（`nread != kHeaderSize`，152-155）
2. `hdr.size == 0`（157）
3. index 非严格单调（161-165）
4. payload 短读（172-176）
5. CRC32C 不匹配（178-182）

**追问**："为什么 CRC 错就停，而不是跳过这条继续扫？"
A：WAL 是顺序结构，一条损坏意味着后面所有 offset 都不可信（无法确定下一条从哪里开始），继续扫描会读到垃圾当 header。Raft 语义下截断尾部是安全的：截掉的都是未确认的 entry，等 leader 重新复制即可。

**追问**："follower 的日志被截断后，term 会不会不一致？"
A：`TruncateFrom`（329-387）会同步更新内存 `last_term_`、on-disk `ftruncate` 到保留记录的末尾、删除后续 segment 文件、并更新 manifest。多段情况下还处理了跨段删除。WAL 一致性由内存三元组 (last_index, last_term, entries) 与磁盘文件共同维护。

**追问**："manifest 更新失败怎么办？"
A：**技术债，要主动说**：`RollSegment` / `TruncateFrom` 里的 `manifest_.Save()` 返回值没检查（L4）。manifest 存 current_segment，失败会导致重启后从旧段开始扫——扫描本身能靠 index 单调性自愈到正确段，但滚段序号可能错乱。正确做法是 Save 失败走 FATAL，因为这是不可恢复的元数据状态。

---

## 15 Crash Recovery

**启动顺序**（`RaftNode::SetStoragePath`，110-174 + `RaftGroup::InitStorage`）：
1. `RaftStorage(path + "meta.json").Load()`——恢复 term / voted_for / config_state
2. `ApplyProgress(path + "apply.meta").Load()`——恢复 last_applied
3. 有 snapshot：`SnapshotLoader` → `state_machine_->LoadSnapshot` → `SetSnapshotAnchor` → `PruneCompacted`（138-161）
4. `SegmentLogStorage::Open()` → `ScanSegment` 全段扫描（截断损坏尾部）
5. `commit_index_ = last_applied_`（171）——**commit 是 volatile 状态，重启置为 last_applied，等待新 leader 通过 AppendEntries 重建**。Follower 绝不自行 commit！
6. `ReplayUnappliedLogs`（1527-1550）：只 replay last_applied 之后的 entry；**多节点集群中非 leader 直接不 replay**（1542-1547），等 leader 决定；单节点例外（没有 peer 时自己就是唯一 voter）。

**安全原则（面试必背）**：
- **WAL 里有的 entry ≠ 已提交**。多数派复制才是提交依据，而"多数派"只有活的 leader 才能重新建立。follower 重启后若自行 commit WAL 尾部，可能提交了未来会被新 leader 覆盖的 entry——破坏 State Machine Safety。
- 幂等性：apply.meta 落后于 WAL 只会导致**重复 apply**（安全，要求命令幂等），apply.meta 领先于 WAL 才会跳过该 replay 的 entry（危险）——所以刷盘顺序严格 **WAL Flush → apply.meta Flush**（1627-1629）。

**追问**："kill -9 在 append 中途发生，重启会怎样？"
A：WAL 尾部要么是半条 header 要么是 CRC 错的完整记录，ScanSegment 停在那，之后的"垃圾尾部"不进入内存 index；下次 append 会从有效尾部继续写（append 模式），或者 leader 复制把冲突截掉。绝不会把 torn write 当有效 entry 去 apply。

**追问**："apply.meta 先 fsync 了，WAL 没 fsync，会发生什么？"
A：这正是顺序约束要防的：恢复时 last_applied 领先于 WAL 实际内容，`ReplayUnappliedLogs` 认为"没有需要 replay 的"——但那些"已 apply"的 entry 其实没持久化，机器掉电数据丢失。反过来（WAL 先）只多 replay，安全。这就是 `ApplyCommittedLogsLocked` 里 1627-1629 两行顺序的全部理由。

---

## 16 io_uring

**真实使用（helio/util/fibers/uring_proactor.cc）**：
- 主网络路径就是 io_uring：`io_uring_queue_init_params` 建 ring，SQE/CQE 提交与收割。
- 启用的高级特性：kernel >= 6.1 时 `DEFER_TASKRUN | TASKRUN_FLAG | SINGLE_ISSUER`（176）——减少 syscall 往返、减少 task_work 唤醒开销；`MSG_RING` 探测启用（202）；`IORING_FEAT_RECVSEND_BUNDLE` 存在时置 bundle 标志（210-213，但默认配置未开启 bundle 路径）。
- 依赖必需特性：SINGLE_MMAP / FAST_POLL / NODROP。
- buf_ring（provided buffer）、poll、ring fd 事件驱动 Proactor 唤醒。

**明确未启用的（话术模板："我知道有，但没开，原因是……"）**：
- **SQPOLL**：源码注释直接说"未短期计划"（182）。理由：需要注册所有 fd（含 socket），高负载下省 syscall，低负载下浪费一个内核线程还引入 idling 复杂度；2 核 VM 上不值得。
- **RECVSEND_BUNDLE**：只做了 feature 探测，默认配置没走 bundle 路径。
- **multishot recv**：默认不是开启状态，需要显式 flag。

**正确说法**："核心网络路径采用 io_uring，并按内核版本启用部分高级特性（DEFER_TASKRUN/SINGLE_ISSUER/MSG_RING）。SQPOLL、multishot、bundle 这些是已知但未启用的能力。"
**错误说法**："我完整使用了 io_uring 所有高级特性。"

**追问**："DEFER_TASKRUN 是干嘛的？"
A：正常情况下 io_uring 完成事件会唤醒处理线程（task_work），DEFER_TASKRUN 把唤醒推迟到"需要的时候"（下一次进内核比如 io_uring_enter），配合 TASKRUN_FLAG 精确控制谁去收割——高吞吐下能省掉大量无效唤醒和上下文切换，Dragonfly 社区测过它对吞吐有明显收益。SINGLE_ISSUER 是向内核声明只有单线程提交 SQE，内核可以省掉 SQ 上的锁/原子操作。

**追问**："io_uring 相比 epoll 在你的场景优势在哪？"
A：写路径 epoll 是"事件 + read/write"两次 syscall，io_uring 一个 SQE 链（recv → parse → send）批提交，syscall 次数大幅下降；配合 provided buffer 还能省每包 malloc。但要说实话：**本项目 4K QPS 的量级上 io_uring 不是性能瓶颈也不是性能来源**，这是为高并发做架构预埋，benchmark 测不出它的价值。

---

## 17 Fiber / Proactor

**核心**：helio（Dragonfly 的 IO 库）风格：`boost::context` 提供栈切换，`fb2::Fiber` 为调度单元，`fb2::Mutex`（fiber 友好的 mutex，阻塞时挂起 fiber 而不是 OS thread），`FiberQueue` 跨线程投递，`DispatchBrief` / `Await` 组织异步。

**一句话定位**："Fiber 把异步 IO 回调组织成近似同步代码，减少 callback 嵌套和状态机复杂度；它不提供 CPU 并行——同一 Proactor 上的 fiber 共享一个 OS thread。"

**面试必须强调的边界**：
- RaftNode 的 mutex_ 是 **fiber mutex**：加锁冲突时挂起当前 fiber，其他 fiber 继续跑——所以"锁内禁止 RPC"不是性能洁癖，而是正确性要求（RPC 挂起会把整个 Raft 状态机让出去，重入同一把锁就是死锁）。
- 心跳是 fiber（`StartHeartbeat`，826-837），选举是 timer 回调——**没有独立 Raft 线程**。
- 跨线程任务通过 FiberQueue 投递到目标 Proactor 的调度器（shard 路由）。

**追问**："fiber mutex 和 pthread mutex 的差别在哪？"
A：pthread mutex 冲突时 OS 线程被挂起，整个 Proactor 停摆（所有 fiber 都被卡住）；fiber mutex 冲突时只挂当前 fiber，调度器切到下一个就绪 fiber。代价是不能跨线程共享（要在不同 Proactor 线程用就是未定义行为/不安全），所以跨线程的数据结构用 FiberQueue 而不是锁。RaftNode 的所有状态天然只在发起方线程被访问，RPC 响应也回到同一线程，所以 fiber mutex 成立。

**追问**："一个慢客户端会不会饿死所有 fiber？"
A：写路径有 socket buffer 反压；解析/调度本身是协作式（Await 挂起）。当前实现没有 per-connection 的 QoS/配额，这是没做完的部分——如果面试官问公平性，承认没有显式调度公平性机制，靠 io_uring 的 NODROP + buffer 限制间接保护。

---

## 18 Shard 单写者

**代码**：`EngineShardSet`（60 行），`Service::Get` 的 `engine_.Schedule(0, key, cb)` → 按 key hash（XXH64）路由到 owner shard → FiberQueue 投递 → 该 Proactor 上的 consumer fiber 执行 → `DbSlice` 操作。写路径 `KvStateMachine::ApplyLogEntry` 同样按 key 路由。

**正确说法**："KV 数据面通过 shard 单写者模型避免共享锁：每个 key 固定路由到一个 shard，该 shard 上只有唯一执行上下文（consumer fiber）读写 DbSlice，所以 shard 内数据面无锁；跨 shard 投递用 FiberQueue。**不是整个系统无锁**——Raft 状态机用 mutex 串行化，FiberQueue 本身有内部同步。"

**错误说法**："整个系统无锁架构。"——立刻会被打。

**追问**："单写者模型的代价是什么？"
A：① 跨 shard 操作（如多 key 命令、全库 scan）需要 barrier（`RunBriefInParallel`），本质是同步点；② 热点 key 单 shard 成为瓶颈（不能靠锁竞争分散）；③ 每 key 路由固定 → 扩容要数据迁移。当前实现多 key 事务/一致性操作没有暴露，所以没踩到。

**追问**："FiberQueue 跨线程投递的延迟是多少量级？"——诚实回答：没单独测过；benchmark 全链路 P50 240μs 里包含一次（写）或零次（读走原连接线程？实际读也要投递到 owner shard）投递。别编数字。

---

## 19 RESP / redis-cli

**正式路径（L1）**：`Connection::InputLoop`（dragonfly_connection.cc:244）→ `ParseRedis`（313）→ `ParseMultiBulk`（473）→ `AddParsedCommand` → `DispatchCommand`（575）→ registry 分发。支持 SET/GET/DEL/EXPIRE/PING/INFO/SAVE/DEBUG，`redis-cli` 可直接访问。

**RedisParser 类（L2/L3）**：类存在、有单测，但正式解析走 `ParseMultiBulk`，RedisParser 只用于部分错误码映射（`SendProtocolError` 用它的 Result 枚举，40-56）。**不能说"完整复用了 RedisParser"**。

**正确说法**："服务端正式路径兼容 Redis RESP，可以用 redis-cli 访问已实现的常用命令。"
**错误说法**："完整兼容 Redis" / "兼容 Redis 全部命令" / "支持事务/发布订阅"。

**攻击与防守**："SET 带 NX/EX 参数支持吗？"
A：registry 里 SET 是固定 arity（-3）的直接 handler，没有实现 Redis 的完整 SET 选项解析（NX/XX/EX/PX/GET）。诚实回答：只实现了最基本的 SET key value 语义 + EXPIRE 单独命令。**不要现场吹参数解析。**

---

## 20 TCP RPC

**帧格式**：magic + type + seq + len + payload + CRC（`raft_codec.cc`）。**工程细节（可讲）**：
1. **连接复用**：节点间一条 TCP 连接承载 vote/heartbeat/append/readindex 多种 RPC，type 字段区分。
2. **seq 匹配**：请求-响应配对靠 seq，乱序/late response 不会答错人。
3. **timeout 后不立即删除连接**：retired connection 先标记，避免 late response 撞上"连接已销毁、新连接同 fd"的 generation race——这是 C10K 工程里的经典坑。
4. **CRC**：帧级校验，防网络/内存损坏。
5. LocalTransport（100 行）：同进程多节点测试用，与 TCP 同接口。

**追问**："RPC 超时后重发吗？"
A：本项目 RPC 层超时即失败返回，**不重发**；重试语义在 Raft 层（下一轮心跳/ReplicateLog 会带新的请求）。Raft 对 RPC 的幂等性要求不高（term/index 校验天然去重），所以把重试放上层是正确的分层。

**追问**："为什么串行发 RPC 不并行？"
A：当前实现选举/心跳/复制都是锁外**串行**发送（raft_node.cc 各处 for 循环）。这是显式的工程权衡：串行让"一轮最坏延迟 = Σ 单 peer 超时"可控（心跳 RPC 超时 150ms << 选举下限 300ms，源码注释 694-699 明确写了这个推导），同时避免并行响应的并发复杂度。代价是 peer 数多时一轮变长——所以把 per-RPC 超时压短。**这是我能讲清 trade-off 的点，不是偷懒。**

---

## 21 Benchmark（口径诚实是第一原则）

**真实数字**：2 核 VM、`midi-redis --port=6380 --conn_threads=2`、无 raft_dir、Python 单线程 socket + 手工 RESP、30,000 req + 1,000 warmup、1000 random keys、单连接串行。

| 命令 | QPS | P50 | P99 |
|---|---|---|---|
| PING | 8643 | 110.1μs | 134.4μs |
| SET 10B | 4160 | 241.1μs | 293.0μs |
| GET 10B | 4736 | 204.9μs | 248.8μs |
| DEL | 4618 | — | — |
| INCR | 7986 | — | — |
| EXPIRE | 3941 | — | — |
| GET pipeline ×16 | 18761 | — | — |

**先说三个"不是"**：不是生产压测、不是多连接并发、不是持久化全开（无 WAL fsync）。**不能说"maxredis 性能是 4K QPS"**，只能说"该固定口径下测得这些结果"。另注意：INCR 数字与当前源码注册表不符（见下文），被追问时以源码为准。

### 为什么 PING 比 SET 快？

PING 不进 Raft、不碰 shard（直接回 PONG），纯"解析 + 回包"，是协议栈裸开销上限 ≈ 8.6K。SET 多了：WAL append + 复制路径 + 状态机 apply + AOF 记录，约慢一倍 → 4.2K。

### 为什么 INCR 比 SET 快（7.9K vs 4.1K）？

⚠️ **先修正一个事实**：当前 HEAD 的命令注册表（main_service.cc:826-833）只注册了 PING/SET/GET/DEL/EXPIRE/SAVE/DEBUG/INFO，**没有 INCR**，全 git 历史也搜不到。你的 benchmark 里有 INCR 7986 QPS，说明该数字测自与当前源码不一致的二进制。面试时如果被问到 INCR，正确说法是："INCR 在测试过的历史版本里存在，当前源码注册表中没有；我以当前源码为准。"**不要**在面试里解释"INCR 为什么快"——你无法从当前源码给出证据，编造解释会被当场识破。

### 为什么 GET 不是最快（4.7K）？

GET 要走 `engine_.Schedule` 跨 shard 投递 + DbSlice 查找 + 回包；PING 是零数据面。另外默认 linearizable_read 开着时 GET 还要过 ReadIndex（本 benchmark 未开/或开了走 lease 快路径——按实际配置如实说）。

### 为什么 EXPIRE 更慢（3.9K）？

EXPIRE 走 Raft 写路径 + TTL 数据结构的更新，比 SET 多一次过期表维护。而且 **EXPIRE 不写 AOF（L4）**——它慢不是 AOF 造成的。

### 为什么 pipeline 能提升到 18.7K？

16 条命令一次发、一次收：16 个 RTT 合并成 1 个 RTT，摊销的是**网络往返 + Python 客户端调度**，不是服务端 CPU。18.7K / 16 ≈ 1.17K 批次/s，每个请求成本反而和串行差不多——说明瓶颈在客户端往返，不在服务端吞吐。

### 单连接 benchmark 的最大问题

它测的是 **RTT 链路的延迟下限**，不是吞吐上限。服务端实际并发能力完全没被压出来（单连接时服务端大部分时间在等网络）。

### 条件变化推演（面试官必问，按模型回答）

- **100/1000 连接**：QPS 会大幅上升直到 CPU 打满；2 核上 io_uring + fiber 的优势开始显现（每连接一个 fiber，无线程爆炸）。但**没有实测数字，不给具体数**。
- **换 C++ 客户端**：客户端不再是瓶颈，P50 应该能压到 50μs 以内级别，QPS 由服务端 CPU 决定。同样不编数字。
- **开 WAL fsync（always）**：2 核 VM 磁盘 fsync 若 ~0.5ms，SET 会掉到 ~1-2K 甚至更低，P99 会被 fsync 尾部拖高——**持久化成本直接可见**。
- **3 节点 Raft**：写路径增加 1 次 RPC RTT（本地回环或真实网络）；网络 RTT 1ms 时，SET 延迟下限就是 1ms+，QPS 掉到百级。**跨机房（10ms+ RTT）单组 Raft 写入 QPS 上限 ≈ 100/RTT 数量级**——这是 Raft 的架构级限制，只能靠 batch/多 group 缓解。
- **SSD fsync 0.5ms**：同 WAL always 情形。

**五层性能模型（回答"如果让你优化"的框架）**：
1. **CPU 层**：解析（RESP 编解码零拷贝）、协议热点（per-connection buffer 复用、批解析）
2. **网络层**：多连接并发压满、pipeline、multishot recv、RECVSEND_BUNDLE
3. **Fiber 层**：减少跨线程投递（连接亲和）、批量调度（DispatchBrief 批）
4. **Shard 层**：热点 key 打散、单写者窗口优化、批量 apply
5. **Raft 层**：group commit（多客户端 entry 一批提交，摊薄每 entry 的复制 RTT 与 fsync）、异步 apply 回包、lease 读快路径占比
6. **WAL 层**：group fsync（多个 entry 一次 fsync）、everysec 模式、写盘并行

**10 倍目标的优化顺序（面试官高频题）**：① 多连接并发（立刻受益，架构已支持）→ ② 开 pipeline/batch 提交 → ③ Raft group commit → ④ 关/降频 fsync（everysec）→ ⑤ io_uring bundle/multishot → ⑥ 热点 shard 优化。按"先打满已有架构能力，再动结构"的顺序讲。

---

## 22 C++ 深挖（全部挂钩源码）

| 面试官问题 | 源码挂钩 | 回答要点 |
|---|---|---|
| RAII 在哪体现 | `RaftNode::Shutdown` 的 `std::lock_guard`（raft_node.cc:60）、`unique_ptr<WalWriter>`、`SegmentLogStorage::~SegmentLogStorage` | 锁、文件、fiber 生命周期全部 RAII；Shutdown 幂等（`drain_started_.exchange`，82-103）防双重释放 |
| move vs copy 实战 | `log_storage_->Append(std::move(entry))`（1228）、`entries_.push_back(std::move(entry))`、`GetRange` 返回拷贝（1305 注释解释为什么锁内必须拷贝） | 锁外 RPC 读的是**副本**，不能别名可变内存——这是 move/copy 决策正确性而非性能 |
| shared_ptr vs unique_ptr 选型 | `unique_ptr<RedisParser>`（dragonfly_connection.h:64）、transport 裸指针 + 手动置空（70） | 所有权唯一就 unique_ptr；跨线程共享才 shared_ptr；`transport_=nullptr` 防悬垂 |
| 内存序为什么用 release/acquire | `shutdown_.store(true, memory_order_release)`（61）、`rpc_alive_`、`heartbeat_stop_`、`heartbeat_epoch_.fetch_add(acq_rel)`（829） | shutdown flag + 数据可见性配对：写入线程 release，读取线程 acquire；fiber 之间仍有 happens-before 需求，不能全用 relaxed |
| atomic 为什么不够/为什么够 | `keyspace_hits_.fetch_add(relaxed)`（main_service.cc:612）——纯计数，relaxed 够 | 分场景：计数器 relaxed；flag+数据用 release/acquire；复杂状态转移交给 mutex |
| false sharing | `peer_next_index_` / `peer_last_log_index_` / `peer_hb_ok_` 是独立 vector（raft_node.h） | 三个数组分离而非 struct-of-array，热点更新（每轮心跳全量写 peer_hb_ok_）不互相踩 cache line；但要诚实说**没有做显式 alignas 验证** |
| lock contention 怎么排查 | RaftNode 全局一把 fiber mutex | 策略：缩短临界区（锁内只改状态+构造请求）、RPC 出锁、批量 apply 128 条；热点仍是"每条 SET 进锁两次以上"，未来可拆读写锁/分段锁——**没做** |
| condition variable | 本项目没用 pthread condvar，对应物是 `util::fb2::FiberQueue` / `ThisFiber::SleepFor` 轮询（WaitForApplied 的 1ms sleep 轮询，942） | 诚实说 WaitForApplied 是 sleep-poll 而非事件唤醒——这是 L4 附近的粗糙实现，条件变量/notify 版本是改进项 |
| coroutine/fiber 本质 | boost::context 栈切换，fb2::Fiber | fiber 挂起=保存寄存器+换栈，不经过内核；阻塞式语义由调度器保证 |
| lifetime 管理最难点 | `RaftNode::Shutdown` 顺序：先 shutdown_ flag（防 timer 复活）→ 停 transport → 停心跳 → 停选举 → `WaitForRpcDrain` 等 in-flight handler 退出（79） | 顺序错了就是 use-after-free；`rpc_refs_` 计数保证 mutex_ 与成员销毁时无并发访问 |
| UB 风险点 | `GetTerm` 越界返回 0 的兜底、`entries_` 的 index 换算 | 说实话：没有 sanitizer 全量验证过；测试跑过 gtest |

---

## 23 Linux 深挖（全部挂钩项目）

| 面试官问题 | 源码挂钩 | 回答要点 |
|---|---|---|
| 你的网络路径有多少次 syscall | io_uring 一次 `io_uring_enter` 提交 SQE 批（recv+send 链），DEFER_TASKRUN 减少额外唤醒 | epoll 是每事件一次 read/write；io_uring 批提交摊薄 syscall——但**4K QPS 下 syscall 不是瓶颈** |
| fsync vs fdatasync | `WalWriter::Flush`（WAL 用 fsync）；AOF 只有 fflush（L4） | fdatasync 不刷 inode 元数据（大小变化还是要刷）；WAL append 场景 fdatasync 是更优选择，本项目用 fsync 更保守 |
| page cache 与 O_DIRECT | WAL 默认走 page cache + fsync；`fsync_interval_ms>0` 模式纯 page cache + 后台 fiber 刷 | page cache 读快但断电窗口 = 间隔；O_DIRECT 绕过 cache 少一次拷贝但要求对齐，本项目没做 |
| mmap 用没用 | WAL 是 read/write + ftruncate（segment_log_storage.cc:358），**没用 mmap** | 诚实回答：不用 mmap 的理由是崩溃时脏页写回顺序不可控（SIGBUS + 崩溃一致性复杂），torn write 校验也更直接 |
| TCP send/recv buffer | io_uring NODROP + provided buffer | 慢客户端反压：send 未完成则挂起 fiber，不无限堆内存 |
| context switch 成本 | fiber 栈切换 vs 内核调度 | 同线程 fiber 切换 ~百 ns 级无内核参与；跨 Proactor 投递才涉及调度 |
| CPU 调度亲和 | `conn_threads=2`，Proactor 线程模型 | 没有显式 CPU pinning——这是没做的小优化 |
| crash consistency 体系 | WAL(CRC+fsync+truncate) → apply.meta(顺序) → manifest → snapshot | 恢复以"扫描+截断+重放+leader 重建 commit"为纲 |

---

## 24 30 个高概率面试题（速答版）

1. **为什么这个系统需要 Raft？** 单机内存 KV 无法容错；多节点需要选主、复制、统一提交顺序。Raft 是选它因为可理解性+工程实现生态。
2. **Raft 和 Paxos 区别？** Raft 把一致性拆成可操作步骤（选举/复制/安全），Paxos 是理论框架工程难。项目没实现 Paxos，不深吹。
3. **选举超时怎么定？** 300-1200ms 随机，> 心跳(50ms)+RPC 超时(150ms)之和。源码 election_timer.h:51。
4. **为什么随机超时？** 防 split vote 锁步。
5. **term 和 index 的关系？** term 是时钟，index 是位置；(term, index) 唯一标识 entry。
6. **follower 收到日志但不 commit 可以吗？** 可以且应该，commit 由 leader 的 leader_commit 字段下推。
7. **怎么防脑裂？** 更高 term 降级 + CheckQuorum 主动退位 + Lease 依赖多数派 ACK。
8. **为什么先写 WAL 再 apply？** 崩溃后 replay 恢复状态机；apply 完才回客户端。
9. **torn write 怎么处理？** CRC32C + 长度校验，扫描失败截尾。ScanSegment:137-189。
10. **apply.meta 是干嘛的？** 记录 last_applied 避免重复/遗漏 replay；顺序在 WAL fsync 之后。
11. **snapshot 为什么边界是 last_applied？** 只 compact 已 apply 的，未提交 entry 必须留在 WAL 继续复制。
12. **ReadIndex 为什么要 WaitForApplied？** commit 了但可能没 apply，读要读到已 apply 状态。
13. **Lease 多久？怎么续？** 100ms；仅多数派 ACK 续租。raft_node.cc:774-778。
14. **CheckQuorum 多久？** 600ms 无多数派 ACK step down。raft_node.h:390。
15. **joint consensus 里 commit 怎么算？** 两配置分别算多数派取 min。1485-1525。
16. **Leader Transfer 的前置条件？** 目标 match_index 追平。1026-1037。
17. **nextIndex 回退算法？** min(next-1, hint+1)，snapshot 兜底。1409-1416。
18. **什么情况下走 InstallSnapshot？** nextIndex 低于 snapshot index（前缀已 compact）。
19. **fiber 和 thread 区别？** 用户态栈切换 vs 内核调度；共享 OS thread。
20. **io_uring 比 epoll 好在哪里？** 批提交 syscall 少、provided buffer 省分配、无事件风暴。
21. **你的系统真的无锁吗？** 不是。shard 数据面单写者，Raft 状态机 mutex，FiberQueue 有同步。
22. **为什么 RPC 不能在锁里？** 阻塞、反向调用、AB-BA 死锁、临界区膨胀。
23. **RPC 返回后怎么防旧响应污染？** 重新加锁校验 term/role/index。1392-1398。
24. **单连接 4K QPS 说明系统慢吗？** 不说明，单连接测的是 RTT 延迟；吞吐要并发压。
25. **为什么 INCR 比 SET 快？** 当前源码注册表没有 INCR（main_service.cc:826-833），benchmark 数字与源码不一致——面试中主动说"以源码为准"，不解释不存在的路径。
26. **崩溃后 follower 能自己 commit 吗？** 不能，commit 需 leader 重建多数派。1542-1547。
27. **AOF 和 WAL 双写为什么？** 历史遗留；AOF 无 fsync 且非恢复源。主动认账。
28. **线性化读 vs 顺序一致读？** ReadIndex+lease 线性化；本地读(默认 flag 关)是弱一致。
29. **五层性能模型？** CPU/网络/Fiber/Shard/Raft+WAL（见 21 章）。
30. **最大的技术债？** snapshot 接收端未接线、WaitForApplied 超时返回、AOF 无 fsync（见 29 章）。

---

## 25 30 个源码级追问题（能答对 25+ 说明源码真的读过）

1. `AdvanceCommitIndexLocked` 里 candidate term 判断的完整条件？（raft_node.cc:1472-1479）
2. `ReplicateLog` 最多几轮？为什么有上限？（1250，8 轮，防病态自旋）
3. `low_latency` 的 break 条件是什么？（1337-1339，successes >= majority 即停）
4. catch-up 模式跳过哪些 peer？（1346：无心跳 ACK 或已追平）
5. `OnAppendEntries` 的 prev_log_index=0 分支怎么处理？（1114-1118，跳过 term 检查）
6. 空 AppendEntries 为什么不能截断 follower 尾部？（1141-1149 注释：空请求是心跳语义）
7. heartbeat 里为什么只推 commit 不 apply？（756-761 注释：apply 会阻塞心跳 fiber → 集群选举风暴）
8. `BecomeCandidateLocked` 为什么是 SetState 一次写而不是两次？（338-340：term+vote 原子持久化）
9. Same-term step down 为什么不清 voted_for？（325-326：同 term 重复投票风险）
10. `StartElection` 三个阶段分别做什么？出锁的范围是什么？（449-548）
11. 选举失败为什么退回 Follower 而不是继续 Candidate？（535-545）
12. `TryBecomeLeaderLocked` joint 分支的两个多数派怎么算？（550-568）
13. `OnHeartbeat` 里 follower 收到 leader_commit 大于自己 log 怎么办？（622-624：忽略，等复制）
14. `ReadIndex` 慢路径 Phase3 重校验哪三件事？（895-902：高 term / 角色 / term 变化）
15. `WaitForApplied` 的轮询粒度？（942，SleepFor 1ms）
16. Lease 的时钟是什么？为什么？（957-963，steady_clock，NTP）
17. `StepDownLocked` 和 `BecomeFollowerLocked` 的关系？（950-955，同 term step down 保留票）
18. Transfer 的目标追平判断用什么数据？（peer_last_log_index_ vs my last_index，1030-1036）
19. `SendTimeoutNowToTarget` 的请求在锁内构造还是锁外？（1063-1070 锁内构造，1073 锁外发）
20. `OnTimeoutNow` 为什么把 StartElection 放在锁外？（668-670）
21. `SubmitEntry` 的 index 字段为什么置 0？（1227：log 存储分配）
22. `GetRange` 拷贝的原因？（1303-1305 注释：别名安全）
23. backoff 到 snapshot index 之下怎么办？（1415-1416：强制 snapshot 路径）
24. `AdvanceCommitIndexJointLocked` 取 min 还是 max？（1511：min，双多数派交集的正确形态）
25. `ReplayUnappliedLogs` 单节点例外是什么？（1542-1547：无 peer 时唯一 voter 自己决定）
26. apply 批大小？（1573，128 条）
27. apply.meta 刷盘前的 WAL Flush 条件？（1627-1628：仅 batch 模式才刷 WAL——因为 per-append 模式 append 时已 fsync）
28. `MaybeAutoFinalizeJointLocked` 为什么查 joint entry 的 term？（1645）
29. snapshot driver 的轮询周期？（raft_group.cc：1000×1ms）
30. `SegmentLogStorage::Get` 快慢两条路径？（256-269：O(1) 下标 + 兜底线性扫）

---

## 26 20 个压力面问题（高压场景 + 应对）

1. **"你这就是个玩具吧？4K QPS。"** → 承认口径，讲清楚单连接 RTT 口径 ≠ 吞吐上限；反问式展示：多连接/pipeline 架构已支持，没压测过不吹数字。
2. **"为什么不直接用 etcd/TiKV/RocksDB？"** → 学习目标 + 特定取舍（内存 KV + 自研一致性核心），承认生产会复用成熟组件。
3. **"你自己写的 Raft 你敢上线吗？"** → 不敢，明确说缺 jepsen 类注入测试、缺持久化大规模验证。
4. **"ReadIndex 超时还返回，这不是线性化读破了吗？"** → 主动认 L4，给修复方案。
5. **"Snapshot 接收端没接线，那多节点 catch-up 不是假的吗？"** → 是，发送侧完整、接收侧逻辑有测试但未接生产初始化。
6. **"AOF 没有 fsync，你还敢写 INFO 里 aof_enabled:1？"** → 认，INFO 字段名误导，AOF 不是持久化保障。
7. **"EXPIRE 不写 AOF，崩溃后 TTL 丢不丢？"** → 认 L4，恢复后 key 永不过期（内存泄漏风险），修复是补 RecordCommand。
8. **"Raft 里 term/vote 持久化是不是每次选举都 fsync？"** → 是，`SetState` 一次原子 fsync；成本在选举/降级路径不在写路径。
9. **"Lease 100ms 是不是太短？"** → 短租约=更小的 stale 读窗口，续租由心跳保证，故意保守。
10. **"为什么心跳要 50ms 这么密？"** → 快速发现死亡 peer + 快速续租 + 快速 catch-up 检测；代价是带宽，2 核 VM 无所谓。
11. **"你串行发 RPC，peer 一多延迟不是线性涨？"** → 是已知权衡，per-RPC 超时 150ms 压着上限；扩展方向是并行+超时裁剪。
12. **"fiber 单线程模型，一个死循环 fiber 会不会卡死全服务？"** → 会，协作式调度的天然风险；靠代码纪律+单测，没有抢占/隔离机制——认。
13. **"你的锁内状态机真的无阻塞吗？"** → 锁内还有 WAL Append（含 fsync！）。严格说 `SubmitEntry` 锁内 append 在 fsync_per_append 模式下会持锁等 fsync——这是真实存在但可辩护的取舍（单写者简化）。**别在面试中先说"完全无阻塞"，等被问到再解释，或者主动说"Append 在锁内、fsync 也在，这是简化"**。
14. **"为什么不用 braft / etcd-raft 库？"** → 学习目的 + 全栈掌控；承认工业界会复用。
15. **"XXH64 分片有没有热点问题？"** → 有，单 key 热点集中一个 shard；没做二次拆分。
16. **"test 里跑的 3 节点是怎么通信的？"** → LocalTransport 进程内多节点，TCP transport 有实现但集群压测以 local 为主。
17. **"commit_index 推进后立刻 apply，会不会 apply 阻塞写路径？"** → 会，同步 apply；group commit/异步 apply 是优化方向。
18. **"manifest fsync 失败你怎么办？"** → 现在没处理（L4），正确是 FATAL。
19. **"snapshot 的 CRC 你校验了吗？"** → snapshot.bin 的 CRC 不完整（L4），WAL 的 CRC 是完整的。区分开说。
20. **"sender 短读你处理了吗？"** → 处理不足（L4）。认。

---

## 27 10 个故障排查题

1. **kill -9 后重启，日志尾部半条 header** → ScanSegment 停扫，尾部垃圾不生效；下次 append 覆盖或 leader 复制截断。
2. **磁盘满，WAL append 失败** → `Append` 返回 0/log ERROR，但没有磁盘满的对外处理——技术债，如实说。
3. **leader 分区（小分区侧）** → 心跳 600ms 无多数派 → CheckQuorum step down → 写 READONLY、读慢路径失败。恢复后靠更高 term 追日志。
4. **leader 分区（大分区侧）** → 心跳正常续租，业务无感；旧 leader 回来后发现更高 term 自动降级。
5. **两个节点同时超时选举** → 随机超时 + 退回 Follower 打破锁步；同 term 只可能一个拿到多数票。
6. **follower 磁盘损坏（CRC 错）** → 该 follower 停扫截尾，leader 从截断点重发；如回退到 snapshot 之下走 InstallSnapshot（生产未接线！）。
7. **一次 SET 返回 READONLY** → 三种原因：本节点非 leader / 提交失败回退 / WaitForApplied 超时。排查顺序：role → term → 心跳 ACK 数。
8. **读放大到慢路径（lease 没续上）** → 查心跳 ACK 是否 majority、peer RPC 延迟是否 > 150ms 超时导致整轮超时。
9. **启动后数据比崩溃前少** → apply.meta 领先 WAL（顺序错误）或者 snapshot compact 越界——查刷盘顺序与 snapshot 边界。
10. **AOF 文件巨大但恢复不用它** → 正常：恢复源是 Raft WAL；AOF 是 KV 层旁路。这题答不好会暴露对恢复体系的不理解。

---

## 28 10 个系统设计题（每题给骨架）

### 1. 设计百万 QPS KV
分片(XXH64 一致哈希/虚拟节点) → 每 shard 单写者 + 无锁读 → 多队列多 Proactor → 网络 io_uring 批处理 → 读多写少用 lease 快路径。瓶颈：热点 key、跨分片一致性。monitor：P99、连接数、每 shard CPU。

### 2. 设计 3 副本 KV
Raft 复制 + WAL fsync + ReadIndex 读。写链路：本地 WAL → 并行复制 → quorum commit → apply → 回包。故障：自动选举 + CheckQuorum；数据：snapshot + 日志追赶。

### 3. 设计跨机房 Raft
单组 3 副本跨 3 AZ：写延迟 = 跨 AZ RTT，可用性高、吞吐被 RTT 限死 → 折中：每组单机房 + 跨机房异步复制（双主/主备），或 Parallel Raft / multi-group 分片降低单组压力。诚实说没做过跨机房实现。

### 4. 设计 WAL
固定头(index/term/size/crc) + 分段 + fsync 策略分层(always/everysec) + 扫描恢复截尾 + 幂等 replay。进阶：group commit、SPDK/DIO。

### 5. 设计 Snapshot
一致性边界(last_applied) + barrier 冻结写入 + 全量导出 + meta(CRC) + 增量(可选) + compact WAL。接收：chunk 流式 + 原子替换 + 校验。

### 6. 设计 Shard rebalance
先加 shard(空) → 渐进式 key range 迁移(双写+回读) → 切换路由 → 回收。难点：迁移期间的一致性(Raft 或版本号)、热点保护。本项目的 Joint Consensus 是同类问题的共识层解法。

### 7. 设计 Leader Transfer
目标追平 → 停止新写(短暂) → TimeoutNow → 目标立即选举 → 老 leader 收到新 term 降级。升级场景还要处理 client 重定向。

### 8. 设计故障检测
心跳 ACK 多数派计数 + 选举超时(3σ) + CheckQuorum 退位 + 慢盘/GC 探测(心跳带本地指标)。要点：检测 ≠ 一致性，降级要激进、选主要保守。

### 9. 设计监控体系
每节点：term/role/commit/last_applied/WAL size/fsync P99/RPC 超时数/snapshot 次数；集群：leader 切换次数、多数派 ACK 率、读快慢路径占比；告警：term 频繁跳变、WAL 增长、读慢路径占比突增。

### 10. 设计 benchmark
分层：微基准(解析/存储/raft 单函数) → 组件(单节点多连接) → 集群(3 节点 + 故障注入) → 一致性(线性化验证器、jepsen 类)。用 C++ 客户端 + 固定 key 分布 + 关闭省电/CPU 变频。**本项目 benchmark 只到第二层，且单连接。**

### 11-15（扩展题一句话）
- **扩容**：joint consensus 加节点 + 数据 rebalance 两件事分开做。
- **数据迁移**：双写 + 快照回填 + 路由版本号，Raft log 里带迁移水位。
- **冷热数据**：TTL 分级 + LRU 淘汰 + 磁盘冷层(rocksdb)，热层 shard 内存。
- **TTL**：每 key 过期时间戳 + 时间轮/最小堆定时扫描 + 惰性删除双保险；Raft 下过期也要走日志（否则节点间不一致）。
- **一致性读**：ReadIndex + WaitForApplied + lease 快路径，即本项目设计，直接讲。

---

## 29 技术债与诚实回答（L4 清单，每题六问）

### 1. InstallSnapshot 接收侧未接线
- **为什么存在**：开发顺序先写发送(单节点 snapshot+compact)，接收逻辑单测覆盖但生产初始化漏接。
- **影响**：落后超过 snapshot 边界的 follower 无法追数据，集群可用性受损。
- **复现**：3 节点、compact 后重启一节点、断开一段时间再恢复。
- **修复**：`RaftGroup::InitStorage` 中 `node_.SetSnapshotReceiver(new SnapshotReceiver(...))` 或复用 snapshot_dir。
- **测试**：集成测试验证落后节点经 snapshot 追平后 commit 一致。
- **监控**：InstallSnapshot 拒绝次数、follower 落后指数。

### 2. WaitForApplied 超时后仍返回
- **为什么**：主流程没暴露分区读场景，防御性 timeout 只打了日志。
- **影响**：分区下可能返回 stale 值，破坏线性化读承诺。
- **修复**：超时返回 0，`Service::Get` 回 READONLY。
- **测试**：模拟分区读验证返回错误而非旧值。

### 3. AOF 无 fsync
- **影响**：掉电丢 AOF 尾部；但 AOF 非恢复源，影响有限——要如实说清两者关系。
- **修复**：要么补 fsync 策略，要么从架构上移除 AOF 双写。

### 4. EXPIRE 漏写 AOF
- **影响**：崩溃恢复后 TTL 丢失、key 永活 → 内存泄漏。
- **修复**：`Service::Expire` 补 `RecordCommand`（与 Set/Del 对齐）。

### 5. Memcached store 断链
- **事实**：`DispatchMC` 对 store 命令 value 消费不完整（dragonfly_connection.cc:457 附近）。
- **说法**：parser 存在，store 路径不可用。绝不声称支持 Memcached。

### 6. manifest fsync 错误处理缺失
- **修复**：Save 失败 FATAL；监控：manifest 写错误计数。

### 7. snapshot CRC 不完整
- **影响**：snapshot 文件损坏静默加载。
- **修复**：meta 里存 CRC，Load 时校验。

### 8. snapshot sender 短读处理不足
- **影响**：大 snapshot 传输中途失败处理不健壮。

### 9. benchmark 无标准脚手架
- **影响**：数字不可比、不可复现。
- **修复**：固定脚本 + 参数化 + 自动采集 perf。

### 10. election timeout 文档漂移（曾写 150~300ms）
- **教训**：文档必须从代码生成或强制同步；面试时以代码为准。

---

## 30 简历真实性审计表

| 能力 | 简历说法 | 源码实际情况 | 等级 | 可以怎么说 | 不能怎么说 | 面试官可能追 |
|---|---|---|---|---|---|---|
| Raft 选举/复制 | 从零实现 | raft_node.cc 1784 行，完整路径+测试 | L1 | 实现了，测试覆盖 | 生产稳定运行 | term/vote 持久化 |
| ReadIndex | 实现 | 848-924 完整，但超时返回 L4 | L1/L4 | 实现+已知技术债 | 完全正确 | 分区读会怎样 |
| Leader Lease | 实现 | 多数派 ACK 续租 | L1 | 设计可讲 | — | 和 CheckQuorum 区别 |
| Joint Consensus | 实现 | 双多数派+持久化 | L1 | 机制+测试 | 运维面完整 | 一步变更为何不行 |
| Leader Transfer | 实现 | 追平+TimeoutNow | L1 | 协议完整 | — | 目标追不平怎么办 |
| Snapshot | 未在简历单列 | 发送 L1~L2，接收 L4 | L1/L4 | 发送侧+接收侧逻辑有测试 | 完整可靠 catch-up | 落后节点怎么追 |
| WAL+CRC+恢复 | 实现 | 分段+CRC+截尾+顺序刷盘 | L1/L2 | 真实运行过 | 生产级验证 | torn write 识别 |
| io_uring | 采用 | 主路径+部分特性 | L1 | 部分高级能力 | 全部特性 | SQPOLL 为什么没开 |
| Fiber/协程 | 采用 | fb2 + boost::context | L1 | — | Fiber=线程并行 | 单线程模型风险 |
| 单写者 shard | 采用 | DbSlice 单写者 | L1 | 减少共享锁竞争 | 全系统无锁 | FiberQueue 是什么 |
| RESP | 兼容 | ParseMultiBulk 正式路径 | L1 | 常用命令可用 redis-cli | 完整兼容 Redis | SET NX/EX 支持吗 |
| Memcached | （不写简历） | store 断链 | L4 | 不主动提；被问则承认 | 支持 Memcached | — |
| Benchmark | 真实数字 | 口径受限 | 真实 | 固定口径结果 | 生产性能 4K QPS | 并发后多少 |

**结论**：简历里最危险的两个词是"分布式 KV"（让人期待生产级）和"从零实现"（让人期待完整）。建议简历表述保持"实验型/学习型项目"定位，性能段自带口径说明。

---

## 31 面试红线（绝对不能说的话）

1. "完整兼容 Redis" → 只说常用命令 + redis-cli 可访问。
2. "整个系统无锁" → 说单写者 shard + Raft mutex。
3. "生产环境稳定运行/上过线" → 无此事实。
4. "性能 4K QPS" → 必须带口径；说"该口径下测得"。
5. "实现了快照 catch-up" → 接收端未接线，必须带限定。
6. "AOF 保证持久化" → AOF 无 fsync。
7. "支持 Memcached" → store 断链。
8. "用了 io_uring 全部高级特性" → SQPOLL/bundle/multishot 未启用。
9. "选举超时 150~300ms" → 代码是 300-1200ms。
10. "Raft 线程 / 连接线程" → 不存在独立线程，是 fiber。
11. 编造任何没有源码依据的性能数字、内存数字、恢复时长。
12. 把论文里的东西说成自己实现的（如"我实现了 leader 的 no-op entry 提交"——本实现没有 no-op entry）。

---

## 32 背诵稿（最终版）

### "最有技术含量的地方是什么？"
"Raft 的并发模型：锁内状态机 + 锁外 RPC + 返回后按 term/role 重校验。全项目四条 RPC 路径（选举、心跳、复制、ReadIndex）都遵守这个三段式。它解决的是 fiber mutex 环境下持锁做网络 IO 的三大问题：临界区膨胀、对端反向调用形成 AB-BA 死锁、响应回来后状态已被并发改变。实现上每个 RPC handler 的返回值都要过三重过滤：peer term 更高 → 降级并丢弃结果；role 不再是 leader → 丢弃；term 变了 → 丢弃。"

### "你遇到过什么 bug？"
"两个典型的。第一个：follower 收到 AppendEntries 后只截断了冲突点之前的内容，但 leader 没发到的本地尾部没有被清掉，导致重启后这些'前任领导时代的未提交 entry'又冒出来，可能污染后续一致性检查——修法是 append 后按 leader_tail 显式 TruncateFrom（raft_node.cc:1141-1149）。第二个：WaitForApplied 超时后仍然返回，是分区读的潜在 stale read 源，目前打了 WARNING 但返回逻辑还没改，这是我记在技术债里的。"

### "你最大的技术债是什么？"
"三个：snapshot 接收端没在生产初始化里接 receiver（落后节点无法通过快照追赶）；WaitForApplied 超时返回；AOF 无 fsync 且 EXPIRE 不写 AOF。我会按'影响可用性 > 影响一致性承诺 > 影响持久化兜底'的顺序修。"

### "为什么用 Raft？"
"需要选主+复制+统一提交顺序，Raft 相比 Paxos 的可操作性（leader 明确、步骤化）和生态（etcd/braft 验证过）更好；相比 Zab 学习资料和社区实现更多。也是学习目标：亲手写一遍 5.4.2 才知道 Figure 8 为什么存在。"

### "为什么用 io_uring？"
"目标是高并发小包场景的架构预埋：批提交省 syscall、provided buffer 省分配、配合 fiber 天然匹配 Proactor 模型。诚实说当前 4K QPS 单连接根本测不出它的价值，它的收益在并发场景。"

### "为什么 Fiber？"
"把 io_uring 的异步完成回调写成同步代码，状态机复杂度下降；同线程切换成本低。代价是单线程协作式调度的风险，靠代码纪律。"

### "为什么单写者？"
"key 路由固定到 shard，shard 内唯一执行上下文，数据面无锁竞争；一致性边界清晰（snapshot 走 barrier）。代价是热点集中和跨 shard 同步。"

### "为什么 WAL？"
"崩溃恢复的唯一事实源：先持久化日志再回客户端，恢复时扫描+CRC 校验+截尾+replay，配合 apply.meta 保证不重不漏。"

### "性能怎么样？"
"（永远先给口径）2 核 VM、单连接 Python、无 WAL fsync：PING 8.6K、SET 4.1K、GET 4.7K、pipeline16 18.7K。这测的是单连接 RTT 延迟，不是吞吐上限。服务端并发能力没有被这个数字代表。"

### "如果让你重做？"
"1) 先定正确性测试（jepsen 类故障注入）再写功能；2) 提交路径改 group commit + 异步 apply，去掉锁内 fsync；3) 用现成 Raft 库做对照实现；4) benchmark 用 C++ 客户端 + 标准脚手架从第一天就固定口径；5) 接收端 snapshot 与发送端同步开发而不是事后补。"

### "项目还有什么没做完？"
"管理面（动态增删节点 CLI）、监控告警、接收端 snapshot 接线、AOF 修复、group commit、多连接压测、一致性注入测试。Raft 核心协议路径本身是完整的。"

---

## 33 《面试官从简历上随便挑一个词，如何一路把我问到源码》

### 攻击链 1：Raft
"你说从零实现 Raft，那 SET 从客户端到 commit 完整路径讲一遍。"（04 章）
→ "为什么 RPC 不能在锁里发？"（mutex 串行化 + 三段式，raft_node.cc 注释 19-20）
→ "Follower 日志冲突怎么处理？"（1110-1149：gap 拒绝 + term 冲突 TruncateFrom + 尾部清理）
→ "nextIndex 怎么回退？边界？"（1406-1417：min(next-1, hint+1)，snapshot 兜底）
→ "commitIndex 怎么推进？"（1440-1483：排序取多数派位置 + current term 检查）
→ "旧 term 为什么不能直接 commit？"（Figure 8，1467-1479）
→ "旧 term entry 什么时候被 apply？"（间接提交 + 本实现"返回而非前扫"的取舍）
→ "Leader 分区了怎么办？"（CheckQuorum 600ms → StepDownLocked 950-955）
→ "Lease 分区下还安全吗？"（续租条件 = 多数派 ACK，774-778）
→ "节点 kill -9 怎么恢复？"（ScanSegment 截尾 + commit_index=last_applied + 等 leader，171）
→ "follower 重启能自己 commit 吗？"（不能，1542-1547）
→ "那 WAL 里已经有的 entry 算什么状态？"（未提交，等 leader 的 leader_commit）
→ "性能多少？为什么这么低？"（口径 + 五层模型）

### 攻击链 2：WAL
"你的 WAL 是什么格式？"（index/term/size/crc32，segment 64MB）
→ "为什么有 CRC？"（torn write / 位翻转检测）
→ "torn write 怎么识别？"（半 header / size=0 / 短读 / 非单调 index / CRC 错 → 停扫截尾，137-189）
→ "截断的语义是什么？"（尾部都是未确认 entry，等 leader 重发）
→ "appand 之后 fsync 了吗？"（默认 per-append；everysec 模式后台 fiber）
→ "apply.meta 是什么？和 WAL 谁先 fsync？"（WAL 先，1627-1629；反例：apply.meta 领先 = 丢数据）
→ "follower 重启为什么不能自己 commit？"（多数派只能由 leader 重建）
→ "replay 幂等吗？"（幂等命令 + last_applied 水位）
→ "snapshot 之后 WAL 怎么办？"（CompactLogs：删 segment + anchor）
→ "snapshot 边界为什么是 last_applied？"（未提交 entry 必须留在 WAL）
→ "manifest 是干什么的？更新失败怎么办？"（current_segment 追踪；失败未处理 = 技术债）
→ "每一条 entry 一次 fsync，性能怎么优化？"（group commit / everysec / DIO）

### 攻击链 3：io_uring
"io_uring 和 epoll 的本质区别？"（提交模型：批 SQE vs 事件通知）
→ "你用了哪些特性？"（DEFER_TASKRUN/TASKRUN_FLAG/SINGLE_ISSUER/MSG_RING 探测）
→ "SQPOLL 为什么没开？"（源码注释：fd 注册、低负载开销，182）
→ "DEFER_TASKRUN 解决了什么？"（task_work 唤醒风暴）
→ "你的 Proactor 怎么被唤醒？"（ring fd 事件 + TASKRUN_FLAG 控制）
→ "fiber 和 io_uring 怎么结合？"（提交挂起 fiber → CQE 到来 resume）
→ "multishot recv 开了吗？"（没有，显式 flag 未开）
→ "那你的接收路径一次收多少？"（按需求提交 recv SQE）
→ "4K QPS 下 io_uring 价值在哪？"（架构预埋，非当前瓶颈）
→ "怎么证明 io_uring 比 epoll 快？"（没有自己的对比测试——诚实）

### 攻击链 4：一致性读
"GET 怎么保证线性化？"（ReadIndex + WaitForApplied）
→ "lease 快路径什么时候能用？"（NowMs < leader_lease_expire_，862-865）
→ "lease 是怎么续的？"（多数派 ACK 才 ExtendLeaderLeaseLocked）
→ "为什么用 steady_clock？"（NTP 跳变，957-963）
→ "lease 过期了怎么办？"（慢路径：向多数派发 ReadIndex 确认）
→ "慢路径返回后还能用旧 commit 吗？"（重校验 term/role，895-902）
→ "ReadIndex 确认的是 commit，apply 没追上怎么办？"（WaitForApplied 等）
→ "等超时了呢？"（超时仍返回 = L4，937-941）
→ "那你的线性化读在分区下成立吗？"（不成立，主动认）
→ "怎么修？"（超时返回错误 → READONLY）
→ "为什么读不走 Raft log 而走 ReadIndex？"（读不产生日志，零持久化开销）

### 攻击链 5：成员变更
"你的成员变更怎么实现？"（Stable→Joint→Stable）
→ "为什么不能一步替换？"（新旧多数派并存双主）
→ "joint 期间选举怎么算票？"（两配置分别计数，519-530）
→ "joint 期间 commit 怎么算？"（min(old_majority_pos, new_majority_pos)，1485-1525）
→ "joint 配置持久化了吗？"（SetJointConfigState，1606；重启恢复 124-130）
→ "joint 卡住了怎么办？"（finalize 由 joint entry 提交后自动追加，1637-1653）
→ "Leader Transfer 和成员变更什么关系？"（无关；transfer 是选主控制）
→ "你加节点之后数据怎么迁移？"（没有数据迁移！成员变更只管一致性，数据面不变——诚实）
→ "那你的'扩容'是假的吗？"（机制层面支持配置变更，数据面扩容未实现）

---

## 附：面试当天 30 秒速查卡

- 核心函数索引：SubmitEntry 1217 / ReplicateLog 1233 / AdvanceCommitIndexLocked 1440 / ApplyCommittedLogsLocked 1568 / ReadIndex 848 / WaitForApplied 926 / OnAppendEntries 1084 / StartElection 449 / HeartbeatTickImpl 676 / ScanSegment 137 / Append 271
- 关键数字：election 300-1200ms / heartbeat 50ms / lease 100ms / check_quorum 600ms / RPC timeout 150ms / apply batch 128 / append batch 128 / WaitForApplied 5s / transfer timeout 3s / segment 64MB / ReplicateLog 8 轮
- 三句话定位：实验型生产化尝试 / Raft 并发模型是核心资产 / 性能数字必须带口径
- 主动认账清单：snapshot 接收端、WaitForApplied 超时、AOF 无 fsync、EXPIRE 漏 AOF、manifest 错误处理、锁内 fsync、串行 RPC
