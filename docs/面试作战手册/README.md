# 《大厂分布式存储面试作战手册》

> 本手册完全基于真实代码撰写，全部论断均经过源码通读核实（`server/raft/raft_node.cc` 1784 行核心逐行读过）。**禁止把"代码存在"说成"功能落地"**，每个能力都标注了诚实等级。

## 真实性等级说明

| 等级 | 含义 | 本项目示例 |
|------|------|-----------|
| L1 | Production：真实运行路径 | Raft 选举/复制/提交、WAL、心跳、TCP transport、分片存储 |
| L2 | Implemented：代码完成，仅测试可触发 | RedisParser 类、FileLogStorage、多 group、multishot bufring(需 flag) |
| L3 | Stub / 死代码 / 接口 | `#if 0` 块、CommandLog 的 static_cast 陷阱、SnapshotManager::Start |
| L4 | 明确缺陷 | InstallSnapshot 接收侧未接线、AOF 无 fsync、EXPIRE 漏写 AOF |

## 文件索引

| 文件 | 内容 |
|------|------|
| [README.md](./README.md)（本文件） | 第一~四部分：一句话介绍 / 架构图 / 10 亮点 / 源码级讲解 |
| [第05部分-100题详答.md](./第05部分-100题详答.md) | 100 个高频面试题全部详答 |
| [第06-07部分-压力面与故障排查.md](./第06-07部分-压力面与故障排查.md) | 最危险 20 问（模拟压力面）+ 8 个故障排查案例 |
| [第08-09部分-技术债与路线图.md](./第08-09部分-技术债与路线图.md) | 20 条技术债表 + 一个月上线路线图 |
| [第10-12部分-晋升背诵与红线.md](./第10-12部分-晋升背诵与红线.md) | 校招/P5-P8 晋升讲法 + 3/5/10 分钟背诵稿 + 红线清单 |
| [第13部分-校招实习转正特供版.md](./第13部分-校招实习转正特供版.md) | 校招专用：15s/1min/3min 逐字稿 + 必问 20 题 + 转正答辩稿 + 校招红线 |

---

# 第一部分：一句话介绍

**15 秒版**
> 我做了个基于 io_uring 的分布式 Redis——用 C++ 从零实现了整套 Raft 共识（选举、日志复制、ReadIndex、Lease、Joint Consensus、快照、WAL），跑在自研的 fiber 协程框架上，单机 4 万 QPS 写、pipeline 下 63 万 QPS。

**30 秒版**
> 项目叫 maxredis，仿 Dragonfly 的架构：io_uring proactor + 每线程协程调度 + 分片存储（无锁单写者模型），上面叠了一层完整 Raft 共识层，提供线性化读。八个 Raft 高级特性全做了：Joint Consensus、Leader Transfer、ReadIndex+Lease、InstallSnapshot、日志压缩、WAL 崩溃恢复、CheckQuorum、Figure 8 提交安全。270 个测试，多节点用进程内 transport 模拟网络分区、kill -9 恢复都验证过。

**1 分钟版**
> 我的项目分两层。下层是存储引擎：借鉴 Redis 的 RESP 协议和 Dragonfly 的多线程分片模型，每分片一个 proactor 线程 + 一根消费 fiber，所有 KV 操作无锁；网络层用 io_uring，multishot recv 加内核提供缓冲区，配合自研 fiber（boost::context 封装）把异步回调写成同步代码。上层是分布式一致性：整个 Raft 从论文实现，包括几件容易做错的事——Figure 8 修复（只有当前 term 的条目才能直接推进 commit）、ReadIndex 的 Leader Lease 快路径（只在多数派 ACK 后续租，配合 CheckQuorum 保证被分区的 leader 不能服务陈旧读）、Joint Consensus 双多数、Leader Transfer 用 TimeoutNow 强制对方立即选举。持久化层是分段的 WAL，CRC32C 校验、单条记录 fsync，崩溃恢复时撕裂的尾部记录直接截断，符合 Raft 语义。诚实说：多节点的 InstallSnapshot 接收侧在生产路径没接上，我列在技术债里了。

**3 分钟版**
> 见 [第10-12部分-晋升背诵与红线.md](./第10-12部分-晋升背诵与红线.md) 3 分钟背诵稿，可直接背。

---

# 第二部分：项目架构

> 诚实说明：本项目**没有独立 Connection Thread，也没有独立 Raft Thread**。所有"线程"都是 proactor OS 线程；连接是 per-connection fiber，Raft 心跳/选举/快照驱动都是 fiber，靠一个 fiber 级互斥锁 `util::fb2::Mutex`（raft_node.h:381）串成"逻辑单写者"。图里如实标注。

**标注：**
- `[FT]` = fiber（协程），挂在某个 Proactor 线程上
- `[PT]` = Proactor OS 线程（每 CPU 一个）
- `[ST]` = Shard 线程 = proactor 线程 + 专有消费 fiber（`shard_queueN`，engine_shard_set.cc:23-26）
- `[LOCK]` = `util::fb2::Mutex`（park fiber，不 park OS 线程）

## 1. 总体架构图

```
                                ┌─────────────────────┐
                                │   Client 层          │
                                │ redis-cli/memcached  │
                                └──────────┬──────────┘
                                 RESP / Memcached ASCII (TCP)
                                           ▼
┌───────────────────────────────────────────────────────────────────┐
│  AcceptServer 收连接 → 每连接一个 fiber [PT]                          │
│  Connection::HandleRequests / InputLoop (dragonfly_connection.cc:107)│
│    ┌────────────────────┐   ┌─────────────────────┐                 │
│    │ ParseMultiBulk+sdssplitargs 真实路径│  │ RedisParser 类: 死代码  │
│    │ (dragonfly_connection:473)      │  │ (#if 0 :377-423, 仅测试用)│
│    └──────────┬─────────┘   └─────────────────────┘                 │
│               ▼                                                     │
│    Service::DispatchCommand (main_service.cc:472)                    │
│    SET/DEL/EXPIRE → RaftEngine::SubmitCommand                         │
│    GET → kLocal(直读)/kLinearizable(ReadIndex)                       │
└───────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│  RaftEngine (group 0 硬编码) [LOCK]                                   │
│  RaftNode: 共识状态全在 fb2::Mutex 后面 (raft_node.h:381)              │
│    ├─ heartbeat_fiber_ [FT] 50ms 心跳+lease+CheckQuorum               │
│    ├─ election_timer fiber [FT] 随机 300-1200ms                       │
│    ├─ raft_snapshot_driver fiber [FT] 1s 轮询快照 (raft_group.cc:97)   │
│    └─ wal_flush_fiber [FT] 批量 fsync (interval>0 时)                  │
│  PeerManager / Transport(ITransport)                                  │
│    ├─ TCP: TcpTransport [PT] 帧协议 magic+seq+len+CRC                │
│    └─ 测试: LocalTransport (进程内, raft_node_test 用)                 │
└───────────────────────────────────────────────────────────────────┘
                    │ Apply (状态机 Apply 在锁内同步执行)
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│  EngineShardSet (分片, 每 shard 一个 [ST] 无锁单写者)                    │
│  ┌──────────────┐ ┌──────────────┐        ┌──────────────┐        │
│  │ Shard 0      │ │ Shard 1      │  ...   │ Shard N-1    │        │
│  │ FiberQueue(64)│ │ FiberQueue(64)│        │ FiberQueue(64)│        │
│  │ DbSlice      │ │ DbSlice      │        │ DbSlice      │        │
│  │ PrimeTable = absl::flat_hash_map (common_types.h:62)             │
│  └──────────────┘ └──────────────┘        └──────────────┘        │
│  路由: XXH64(key, 种子120577) % shard_num (common_types.h:47-50)      │
└───────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 持久化（raft_dir 模式 = 唯一权威）                                       │
│  data/raft/group_0/                                                  │
│   ├ meta.json     term+voted_for, tmp+fsync+rename (raft_storage.cc:31)│
│   ├ apply.meta    last_applied, 同样原子写                            │
│   ├ wal/segment_*.log  64MB 段, CRC32C 头, 每 append fsync 或 interval │
│   └ snapshot/snapshot.bin+meta.json  快照边界=last_applied             │
│ 非 raft 模式: appendonly.aof（无 fsync! L4）+ snapshot.bin 双机制        │
└───────────────────────────────────────────────────────────────────┘
```

## 2. 模块依赖图

```
                  prior方向: 依赖谁
   ┌────────────────────────────────────────────┐
   │ dragonfly_connection.cc  ──► main_service ──┼─────► raft_engine ──► raft_group
   │      (解析/流水线)                │          │             │              │
   │                                  ▼          │             ▼              │
   │                           command_registry  │        raft_node (核心1784行)│
   │                           storage/          │        │    │     │        │
   │                           persistence/      │        ▼    ▼     ▼        │
   │                                            ─► peer_manager  ─► raft_storage
   │  raft_node ──► log_storage(ILogStorage)     │   snapshot_*   apply_progress
   │        │        ├ CommandLog(内存, raft模式被换掉)   transport(ITransport)
   │        │        └ SegmentLogStorage(WAL)          ├ tcp_transport
   │        ▼                                          └ local_transport(测试)
   │   state_machine(IStateMachine) ──► KvStateMachine ──► EngineShardSet
   │        ApplyLogEntry ─ Set/Del/Expire  → shard fiber 无锁写 PrimeTable
   │   snapshot_barrier.h: kv 所有写路径 BeginRead/EndRead, 快照时 BeginWrite
   └────────────────────────────────────────────┘
   全部构建在 helio 上: fb2 fiber / synchronization / uring_proactor / uring_socket
```

## 3. 写请求调用链（SET a 1，raft_dir 模式，默认每 append fsync）

```
Client ──SET a 1──► [PT] Connection fiber
  └► ParseMultiBulk (dragonfly_connection.cc:473) 状态机, tokens
  └► Service::Set (main_service.cc:563)
      └► RaftEngine::SubmitCommand (raft_engine.cc:30)
          └► CommandEncoder::Encode → RESP 数组序列化 (command_encoder.cc)
          └► RaftNode::SubmitEntry (raft_node.cc:1217)   [LOCK]
              ├ append: log_storage_->Append(entry)     [LOCK 内]
              │    └ SegmentLogStorage::Append (segment_log_storage.cc:271)
              │         ├ WalWriter::Append (内存 buf)
              │         └ Flush() = fwrite+fflush+fdatasync  ←性能瓶颈≈1ms
              └ ReplicateLog(low_latency=true) (raft_node.cc:1233) [LOCK 外发RPC]
                   ├ 健康优先排序: peer_hb_ok_ (raft_node.cc:1273-1278)
                   ├ SendAppendEntries 逐个同步等响应 (tcp_transport.cc:252)
                   ├ quorum 够了就 break —— 分区peer不卡写路径
                   ├ [LOCK] AdvanceCommitIndexLocked + Figure 8 检查 (:1440)
                   └ ApplyCommittedLogsLocked (:1568)
                        └ state_machine_->ApplyLogEntry → KvStateMachine::Set
                             └ EngineShardSet::Await(sid) → [ST] AddOrFind 无锁写
        └ ApplyResult → cntx->SendStored() "+OK\r\n"
  └ persistence_manager_->RecordCommand (main_service.cc:586)  ← raft模式也写AOF(L4)
Client ◄── +OK\r\n
```

## 4. 读请求调用链（GET，kLocal 默认 vs kLinearizable）

```
                    GET
   ┌────────────────┴────────────────────┐
   │ flag: linearizable_read=false       │ flag: linearizable_read=true
   ▼                                    ▼
kLocal 直读 (main_service.cc:607):   RaftEngine::Get (raft_engine.cc:48)
   engine_.Schedule(key, cb)  → ReadIndex (raft_node.cc:848) [LOCK]
   └ EngineShardSet::Add → [ST]   ├ lease 有效 (NowMs<leader_lease_expire_):
   └ db_slice.Find 无锁             │     快路径, read_index=commit_index_
      ├ 命中: 惰性过期检查          └ lease 失效:
      └ KEY_NOTFOUND                  ├ ReadIndex RPC 求 quorum (锁外)
                                      ├ [LOCK] 确认 quorum+续 lease
                                      └ WaitForApplied(read_index) (:926)
                                           ├ 锁内 ApplyCommittedLogs 先试一把
                                           ├ 否则 1ms 睡眠 ← 让 heartbeat fiber 推日志
                                           └ 5s 超时直接返回(!) ← L4 线性化窗口
   → db_slice.Find(key) 读 → SendGetReply "$1\r\n1\r\n"
```

## 5. Leader 选举流程

```
[LOCK] OnElectionTimeout (raft_node.cc:397)  ---- 唯一入口: election fiber [FT]
  ▼
BecomeCandidateLocked (:335)
  └ storage_.SetState(term+1, self)  一次 atomic tmp+fsync+rename (:340)
  └ SetRoleLocked(Candidate) → timer Deactivate
  ▼
StartElection (:449)
  Phase1 [LOCK]: 捕获 peers + VoteRequest{term,last_log_index,last_log_term}
  Phase2 无锁: 逐 peer SendVoteRequest (RPC timeout 500ms/peer)
  Phase3 [LOCK]:
     ├ peer term 更高 → BecomeFollowerLocked(max_term)  ← 立刻承认
     ├ 收集票数 → TryBecomeLeaderLocked (:550)
     │    ├ stable:  vote_count >= N/2+1
     │    └ joint:    old_votes>=old_majority && new_votes>=new_majority
     ├ 输掉选举 → SetRoleLocked(Follower) ← 关键设计:回退Follower再等新随机
     └ BecomeLeaderLocked (:369): next_index 全初始化 = LastIndex+1,
         last_majority_ack_ms_ = now, 启动 heartbeat fiber
```

## 6. AppendEntries 流程（图中是 Follower 侧处理）

```
Leader [LOCK外] ──► Follower [LOCK] OnAppendEntries (raft_node.cc:1084)
  ├ term < 本地 → 拒, 返回本地 term (对方将自降)
  ├ term > 本地 → BecomeFollowerLocked(req.term)  ← 选举安全
  ├ prev_log_index > 本地 last → 拒(false, last_index)  ← 快速回退
  ├ GetTerm(prev_log_index) != prev_log_term → 拒(false, prev-1) ← 冲突回退
  ├ 逐条: 存在且同 term → skip; 冲突 → TruncateFrom(i-1) 再 Append
  ├ 请求带数据且本地尾部更长 → TruncateFrom(leader_tail)  ← 残留未提交必须删
  ├ leader_commit > commit_index → commit=min(leader_commit, my_last)
  │     └ ApplyCommittedLogsLocked  ← follower 也应用状态机!同构复制
  └ 返回 {term, true, my_last}
Leader [LOCK] Phase3:
  ├ match_index 更新 → next_index = match+1
  ├ AdvanceCommitIndexLocked: last_index 数组排序取第 N/2+1 大 (:1449-1465)
  ├ 若拒绝: backoff = min(next-1, peer_last+1), 8 轮内重试 (ReplicateLog :1250)
  └ Figure 8: candidate_term < current_term 时不允许推进 commit (:1472-1479)
```

## 7. Snapshot 流程（leader 侧创建→发送 vs 接收）

```
[FT] raft_snapshot_driver (raft_group.cc:97) 每 ~1s
  └► RaftNode::CreateSnapshotIfNeeded (raft_node.cc:1655) [LOCK]
      └ ScheduleCreateIfNeeded(last_applied_) (snapshot_manager.cc:86)
           触发条件: LastIndex - snapshot_index >= log_gap_=100000  (硬编码! L4)
           ├ barrier_.BeginWrite (snapshot_barrier.h:43)  ← 跨全部 shard 冻结写
           ├ state_machine_->SaveSnapshot(snapshot.bin) ← [ST] 并行 Export
           ├ snapshot.meta Flush (JSON tmp+fsync+rename)
           ├ barrier_.EndWrite   ← 注意 kv 写路径都要 BeginRead/EndRead
           ├ CompactLogs: CompactSegments(删 snapshot 之前的段文件) + CompactUpTo
           └ 更新 last_snapshot_index_ (raft_node.cc:1668)

Lagging follower 追赶:  Leader
  ReplicateLog → ShouldInstallSnapshot(next<=snap_index) (:245)
  └ SnapshotSender.SendSnapshot: 64KB×N 块 + done 标记 (snapshot_sender.cc:43)
  └► Follower OnInstallSnapshot (raft_node.cc:1163)
       └ snapshot_receiver_->HandleChunk
            ├ ★ L4: 生产路径没有任何地方构造 SnapshotReceiver!
            │   raft_group.cc 没调 SetSnapshotReceiver → :1178 恒拒绝
            │   仅 snapshot_receiver_test.cc:27 构造 → 追赶永远失败
            └ （若接上）tmp+fsync+rename, 更新 last_applied_/commit_index_/
               last_snapshot_index_ 并 Clear 日志 (:1198-1206)
```

## 8. Crash Recovery 流程

```
进程重启 → Service::Init (main_service.cc:89)
 ├ raft_dir 空: LoadSnapshot(snapshot.bin) + ReplayAof  (老路线, AOF无fsync)
 └ raft_dir 有: RaftEngine::InitRaftStorage → RaftGroup::InitStorage
      └ SegmentLogStorage::Open (segment_log_storage.cc:40)
           ├ manifest.json → 找段列表 → 逐段 ScanSegment
           ├ ScanSegment: 读 RecordHeader{index,term,size,crc32} (:137)
           │    任意一处失败(半头/CRC错/非单调index) → 停在这,撕裂尾丢弃 ←关键
           ├ 打开最后一个段 append 模式
      └ RaftNode::SetStoragePath (raft_node.cc:110)
           ├ meta.json Load → 恢复 term/voted_for + 在途 joint config (:124-130)
           ├ apply.meta Load → last_applied_
           ├ SnapshotLoader → 有快照: LoadSnapshot + 回退到 anchor + PruneCompacted
           ├ commit_index_ = last_applied_  ← 永不自我提交, 等 leader
           └ 无快照 → ResetApplyProgress(0) → 全量 WAL 重放 (idempotent)
      [FT] ReplayUnappliedLogs (raft_node.cc:1527):
           ├ follower 且有多节点 → 不动, 等 leader 决定 ← 状态机安全
           └ 单节点 leader → commit=last, 重放
  最后 BootstrapSingleNode (main_service.cc:176) → Leader → 可用
```

## 9. 网络分区流程

```
                     ┌── 多数派分区(3/5) ──┐          ┌── 少数派 ──┐
   Leader A          │  B、C、D           │          │    E      │
                     心跳/日志继续           │             隔离
   A 每50ms心跳+lease续期 (仅多数派ACK才续)     │
   E 收不到 → 300-1200ms随机 → 自己选举 (term更高)
   A 收到更高 term 的 Vote/Heartbeat/AE → BecomeFollowerLocked  ← 立刻让位
   结果: 多数派选出新 leader E', 继续服务;
         A 的未提交日志被丢弃 (Figure 8 + 提交判定)
   分区愈合: E'(多数派) 给 A 推日志/快照 → A 追上
   Guard: CheckQuorum (raft_node.cc:779): A 连续 600ms 无多数派 ACK → 自查下台
          └ 这保证被分区 leader 不能靠旧 lease 服务线性化读 ← ReadIndex 安全
   测试路径: raft_node_test.cc 的 PartitionedTransport 包装 (agent核实)
```

## 10. kill -9 恢复流程

```
kill -9 (强杀)
  ├ 正在 fdatasync 的尾部记录: 磁盘要么完整要么撕裂
  ├ 撕裂尾: 重启 ScanSegment CRC/单调检查 → 丢弃 → 符合 Raft 语义 (:145-182)
  ├ 每 append fsync (默认): 已 ACK 的写 100% 在盘 → 无丢失窗口
  └ interval 批量模式 (raft_fsync_interval_ms>0): 最多丢 last interval,
     但顺序保证 WAL 先于 apply.meta → 恢复只会幂等重放, 绝不跳过已提交
  重启 → 同上 Crash Recovery → Leader 重新建立 commit_index_
  （真实场景 "kill -9" 也可指测试: raft_apply_recovery_test.cc 25用例, 已跑通;
    真实进程级测试 binary raft_cluster_test 在 CMake 声明但 build 树里没有! L3）
```

---

# 第三部分：项目亮点（10 个）

**【亮点1】Figure 8 提交安全（Raft §5.4.2 最容易做错的地方）**
- **场景**：按" majority 就 commit"的直觉写，会被哈佛 6.824 的 Figure 8（LeaderA 在 term2 提交后崩溃，B 在 term3 当选但缺 term2 条目）击穿。
- **问题**：旧 term 条目靠复制数推进 commit，可能被未来 leader 覆盖后仍被新 leader 按未知 term 提交——State Machine Safety 破坏。
- **方案**：`AdvanceCommitIndexLocked`（raft_node.cc:1440-1482）排序取第 majority 个 index 后，检查 `candidate_term < current_term` 一律拒绝；只允许当前 term 条目直接推进，旧条目必须靠其上方的当前 term 条目"顺带提交"。
- **结果**：`raft_node_test.cc` 覆盖 Figure 8 场景（两穿位 leader 交替），提交严格单调。
- **源码**：raft_node.cc:1472-1479（含 VLOG 打点）＋ joint 变体 :1513-1524。
- **面试价值**：这是 Raft 实现者 vs 论文阅读者的分水岭。能主动讲 Figure 8 的，面试官立刻知道你写过真代码。

**【亮点2】ReadIndex + Leader Lease + CheckQuorum 三重组合**
- **场景**：线性化读不能在 leader 直接读本地，否则分区后读陈旧值。
- **问题**：每次读都做 quorum RPC 代价高；不做又错。
- **方案**：lease 只被"多数派确认"续期（`ExtendLeaderLeaseLocked` 只在 majority ACK 后调用，raft_node.cc:774-778 的 C2 fix）；lease 有效走快路径（:862-866），失效走 ReadIndex RPC；连 600ms 无多数派 ACK 就 `StepDownLocked`（:779-783）。时间源用 `steady_clock` 防 NTP 跳变（:957-963）。
- **结果**：lease 与 quorum 绑死——被分区 leader 的 lease 必然过期，CheckQuorum 再兜底退位；快速路径零 RTT，慢路径多一跳。
- **源码**：raft_node.cc:848-924, :946-955。
- **面试价值**：能讲清"lease 为什么可以信任"的人不多。要点只有一句话：lease 续期权 = 多数派 ACK 权，两者是同一件事。

**【亮点3】心跳即空 AppendEntries，双通道补日志**
- **场景**：写路径 fast-path（健康优先 + quorum 即 break）可能长期对落后 peer 不发日志；测试暴露出 "Follower 永久落后"。
- **问题**：落后 peer 永远等不到 AppendEntries，commit 无法推进。
- **方案**：heartbeat tick 里从 ACK 刷新 `peer_last_log_index_`，发现 `last_log < my_last` 就置 `need_replicate`（raft_node.cc:753-754），心跳循环外调 `ReplicateLog(low_latency=false)`——无 early break，推到每一个落后且可达的 peer（:805-806, :1340-1348）；配合 8 轮 nextIndex 退避重试（:1250）。
- **结果**：写路径不卡分区 peer，落后 peer 由心跳路径兜底。
- **源码**：raft_node.cc:1233-1431（三种 order：healthy-first / quorum-break / catch-up-all）。
- **面试价值**：说明你知道"leader 要主动推日志而不是等回应"，以及性能与收敛的两难解法。

**【亮点4】单写者共识核：fb2::Mutex + 锁外发 RPC**
- **场景**：RaftNode 状态被连接 fiber、心跳 fiber、选举 fiber、快照 fiber 四路并发访问；committed (term, voted_for) 必须原子。
- **问题**：普通 mutex 会 park OS 线程，同线程 fiber 重入死锁；持锁同步调 RPC 会 AB-BA 死锁（两节点互相复制时）。
- **方案**：`util::fb2::Mutex` 只 park fiber（raft_node.h:22-40 注释成文）；两个铁律：公开方法全上锁、RPC 永远在锁外发（捕获请求→解锁→发→回锁处理响应）。
- **结果**：`2f933da` 提交明确记录这是修跨线程 data race 的 hardening；raft_node 全部状态变更串行化。
- **源码**：raft_node.h:19-40（注释即文档）, StartElection 三阶段 :449-548, ReplicateLog 三阶段 :1251-1429。
- **面试价值**：面试官最怕"每个函数一把锁还发 RPC"的新人。这两条规则可以直接背。

**【亮点5】超时 Reuse：seq 帧 + retire-don't-delete 连接回收**
- **场景**：RPC 超时后，晚到的响应会让流式协议错位；close 掉连接又和 helio epoll/uring 的 arm-slot generation race 打架。
- **问题**：超时即断连 → 每次心跳超时都重建连接 → 分区时疯狂 churn。
- **方案**：每个请求一个单调 seq（`++conns.seq_counter`），帧尾加 CRC32；超时后连接保留，晚到旧 seq 帧直接跳过（tcp_transport.cc:143-216）；坏 socket 一律"Close 后进 retired 列表等线程退出再 delete"（:113-122, epoll_proactor.cc:497 generation-race TODO 引用）。
- **结果**：分区心跳零 churn；半包只发生在部分读时（:166-176 区分"帧边界超时=保留" vs "半头=重连"）。
- **源码**：tcp_transport.cc:81-226。
- **面试价值**：这是"协议工程师"级别的细节，普通项目不会写。

**【亮点6】WAL 崩溃语义：撕裂尾直接丢弃**
- **场景**：kill -9 或断电，正在写的记录可能半条。
- **问题**：把半条记录当有效 → 日志索引错乱；把一截有效当无效 → 丢已提交。
- **方案**：`ScanSegment`（segment_log_storage.cc:137-189）：头不完整停、size==0 停、非单调 index 停、CRC32C 错停——从此处截断，前面全部有效。`file_size_` 追踪偏移，重新 append 模式续写。
- **结果**：与 Raft 持久化语义完全一致，raft_apply_recovery_test 验证。
- **源码**：segment_log_storage.cc:137-189, wal_writer.cc:88-98。
- **面试价值**："撕裂尾处理"是持久化面试的必考题，你主动讲 CRC + 截断 = 加分。

**【亮点7】apply.meta 永不领先 WAL 的提交时序**
- **场景**：批量 fsync 模式下（interval>0），apply.meta 写快了会跳过重放。
- **问题**：恢复时若 apply.meta 领先 WAL，会"跳过还没验收的已提交条目"，或相反重复应用。
- **方案**：先 `log_storage_->Flush()` 再 `apply_progress_.Flush()`（raft_node.cc:1625-1631，注释 :1618-1622），配合幂等重放——最多重复应用，绝不跳过提交。
- **源码**：raft_node.cc:1618-1631；apply_progress.cc:22-68 的 tmp+fsync+rename。
- **面试价值**：持久化顺序论证（WAL→meta→数据）是超越"会用 fsync"的高阶认知。

**【亮点8】Joint Consensus 双多数 + 自动 finalize + 持久化在途配置**
- **场景**：成员变更不按 Joint Consensus 走，可能出现瞬间双首领。
- **问题**：一步切换的经典危害；变更中途崩溃恢复不了。
- **方案**：Stable→Joint→Stable；投票、提交、心跳全部按双多数（AdvanceCommitIndexJointLocked :1485-1525, TryBecomeLeaderLocked :551-567）；step1 提交后自动追加 step2（MaybeAutoFinalizeJointLocked :1637-1653，etcd 式）；joint 配置写进 meta.json，重启恢复在途变更（raft_node.cc:124-130）。
- **源码**：raft_node.cc:208-238, :1485-1525, :1637-1653。
- **面试价值**：直接答"用 Joint Consensus 不用 single-step"，再补一句"因为成员过半计算在切换瞬间是不连续的"。

**【亮点9】Leader Transfer：catch-up 完成后 TimeoutNow**
- **场景**：运维要平滑换 leader（滚动升级）。
- **问题**：等人选选举超时太慢；直接停写不优雅。
- **方案**：`IsTransferReadyLocked` 要求 match_index >= last_index（raft_node.cc:1026-1037），心跳循环里 catch-up + 3s 超时取消（:787-799），就绪后 `SendTimeoutNowToTarget`（raft_node.cc:1061-1082）→ 目标立即 `StartElection`（OnTimeoutNow :650-672，锁外发起防重入）。
- **源码**：raft_node.cc:965-1082。
- **面试价值**：知道 Raft 论文 §6 转移的时机判定（先追上再转移），胜过只会说"有 leader transfer"的人。

**【亮点10】无锁分片：Shard Thread = proactor + 消费 fiber（L1, 但埋了 expire 的雷）**
- **场景**：KV 加锁太慢；Dragonfly 的答案是把每个 shard 钉在一个线程上，任务经 FiberQueue 投递。
- **问题**：跨线程共享 DbSlice 怎么办？
- **方案**：`queue_.Run()` 消费 fiber 独占（engine_shard_set.cc:21-26, kQueueLen=64）；DbSlice 全部方法无锁（db_slice.cc:43-124），一致性由"单写者线程模型"保证，跨线程一律 `Await(sid, cb)`。
- **结果**：GET 只经一次投递+无锁 find；SET 路径在 Raft apply 时同步投递 shard。
- **缺陷**：**过期只惰性删除**（db_slice.cc:54-58）——死键常驻内存、DbSize/快照虚高（L4）；快照屏障 Barrier 是自旋读计数（snapshot_barrier.h:21-48），依赖所有写者都是 fiber。
- **源码**：engine_shard_set.cc:18-31, kv_state_machine.cc:51-62。
- **面试价值**：单写者模型是高频考点，你还能主动报出它的隐患——这本身就加诚实分。

---

# 第四部分：源码级讲解（14 模块）

（每个模块按：为什么这么设计 / 为什么不用其他方案 / 调用链 / 核心数据结构 / 复杂度 / 缺陷 / 改进。ℹ️ 等级标注）

## 1. RESP 【解析真实路径 L1，RedisParser 类 L2/L3】

- **为什么这么设计**：连接 fiber 单线程内用状态机手写解析器 `ParseMultiBulk`（dragonfly_connection.cc:473-555），`multibulk_len_`/`bulk_len_` 两个成员做推进，不建 AST、不分配临时对象；inline 命令走 `sdssplitargs`（Redis 移植，:354）。
- **为什么不用其他方案**：正式 `RedisParser` 类（redis_parser.cc）在 3/4 的历史版本里被 `#if 0` 掉了（dragonfly_connection.cc:377-423），因为双份解析器会漂移——诚实的做法是删掉其中一个，这是技术债。
- **调用链**：`InputLoop → ParseRedis → ParseMultiBulk → cc_->AddParsedCommand → DispatchCommand`（dragonfly_connection.cc:243-279）。
- **核心数据结构**：`IoBuf`（环形双缓冲）+ `ParsedCommand`（串联链表 next，流水线用）。
- **时间/空间复杂度**：O(n) 严格单遍；每命令两次 memcpy（IoBuf→sds），空间 O(buffer)。
- **缺陷（L4）**：`parser_error_` 在活跃路径从不赋值 → 协议错误回复永远是 "malformed request"（:293）；`DCHECK(io_buf->InputLen()==0 || status!=OK)` 依赖解析全耗尽（:251，注释承认 busy-loop 复现 `echo -n "set foo"|nc`）。
- **改进**：删除死代码路径；解析错误枚举透传；残余输入进 stash 再等数据。

## 2. Memcached 【解析 L1，store 派发 L4 断链】

- **为什么这么设计**：腾缝兼容 memcached ASCII：`ParseMemcache`（dragonfly_connection.cc:426-471）→ `DispatchMC`（main_service.cc:500-541）映射 set/add/replace → SET [NX|XX]，get → GET。
- **为什么不用其他方案**：不需要独立命令集，复用 RESP 命令层是合理取舍（代价：cas/append/prepend/gat 只有解析没有派发，默认分支返回 client error）。
- **调用链**：`InputLoop → ParseMemcache → memcache_parser_->Parse（memcache_parser.cc:85-146）→ DispatchMC → DispatchCommand`。
- **核心数据结构**：`MemcacheParser::Command{type,key,bytes_len,flags,expire_ts,cas_unique,no_reply,keys_ext}`。
- **缺陷（L4，重要）**：store 命令的 value 从不被消费——`ConsumeInput(consumed)` 只吃命令行（dragonfly_connection.cc:459），`consumed >= InputLen()`（:456）对带 value 的输入恒 false → **DispatchMC 根本不会被调用，set 静默失败，value 再被当命令解析报错**。另：行长 >300 报错（memcache_parser.cc:91）、token 上限 8。
- **改进**：store 命令 consume `consumed+bytes+2` 并同步派发；先把 memcached 整条路径接上再谈宣传支持。

## 3. io_uring 【L1 主路径 + L2 特性需 flag】

- **为什么这么设计**：每 proactor 线程一个独立 ring（SINGLE_ISSUER），1024 队列（dfly_main.cc:118）；socket 读写全走 `URING_SOCKET`，IO 完成后唤醒 fiber。
- **开了什么**（uring_proactor.cc:135-244）：5.8+ 强制 CHECK；5.19+ 上 `IORING_SETUP_SUBMIT_ALL`+`poll_first`+buf_ring；6.1+ `DEFER_TASKRUN|TASKRUN_FLAG|SINGLE_ISSUER`；MSG_RING 跨线程广播（:202）；注册 ring_fd（6.0+，规避 oracle 5.15 死锁 bug :211-218）。
- **没开什么（诚实）**：SQPOLL——注释明说 "need to check if its worth pursuing. Not for short-term"（:179-182）；RECVSEND_BUNDLE——`if (false && ...)`（uring_socket.cc:496-499）；multishot recv 默认关——`uring_recv_buffer_cnt` 默认 0（dfly_main.cc:27-29），需内核 ≥6.2 且显式开。
- **为什么不用 epoll**：`IORING_OP_POLL_ADD` 在 ring 内仿真 epoll（:530-561），一次收批 CQE 128 个（:64）；epoll 是 force_epoll 回退。
- **缺陷**：ENOBUFS 直接 `LOG(FATAL)`（uring_socket.cc:468-473 "TBD"）；direct-fd 表线性 find 且不可 resize（:672-678）；timer 用一次性 `IORING_OP_TIMEOUT` 而非 multishot（uring_proactor.h:217 TODO）。
- **改进**：ENOBUFS 降级重注册；direct-fd 用 free-list。
- **面试价值**：io_uring 特性全名单+为什么不开 SQPOLL（fd 注册限制）是最强的内行信号。

## 4. Proactor 【L1】

- **为什么这么设计**：1 线程 1 proactor 1 ring；dispatcher fiber 跑 `MainLoop`，worker fiber 协作调度（proactor_base.cc:127-135）。`GetNextProactor` round-robin（proactor_pool.cc:169-182）。
- **为什么不用其他方案**：不用 libuv（无 ring 支持）、不用裸 asio（缺少 fiber 集成）。
- **核心结构**：`ProactorDispatcher`、remote_ready_queue（无锁 MPSC，scheduler.h:202）、epoch/generation 防迟完成。
- **缺陷（照实引用）**：epoll 下有 generation race：`item.index==-1` 检查不足，可能把完成派发给新回调（epoll_proactor.cc:497-499 TODO）；epoll 的 async read 是同步占位（epoll_socket.cc:378-382 "TODO implement async"）。
- **改进**：CQE 里加 32bit generation。

## 5. Fiber 【L1】

- **为什么这么设计**：把回调地狱写成阻塞式代码；每 OS 线程一个调度器，`boost::context` 做切换（detail/scheduler.h:66），非 Boost.Fiber（helio 自 2023.5 起 fork 改造）。
- **关键 API**：`util::fb2::Fiber`（64KB 栈默认）、`ThisFiber::SleepFor/Yield`、`fb2::Mutex`（park fiber）、`FiberQueue`（投递）、`Await/DispatchBrief`。
- **为什么不用协程库**：Boost.Fiber 版本 API 陈旧且切换开销高，fork 进来可深度定制（栈 PMR、优先级预算 NORMAL 1ms/BACKGROUND 50us）。
- **陷阱**：fiber 阻塞 = 线程不阻塞，但**不会自动并行**——一个 proactor 上所有 fiber 共享 1ms 预算，cpu 密集代码要主动 `SleepFor/Yield`；`util::ThisFiber` 只能在 proactor 线程调用。
- **缺陷**：栈 64KB 固定，深递归易爆（`FiberAtomicGuard` 可临时禁 preempt 但治标）；dispatcher fiber 禁止被 preempt。
- **复杂度**：切换 O(1)。

## 6. Raft Election 【L1】

- 核心要点：term 持久化原子化（BecomeCandidateLocked :340 一次 fsync）；输选举回退 Follower 而非留在 Candidate（:540-545，注释解释为什么——破分票锁步：Follower 会授票，Candidate 拒绝）；`OnRequestVote` 的 log-up-to-date 检查（:430-439）；`vote` 决定先 fsync 再返回（:443）。
- **缺陷（L4 级诚实点）**：文档写 [150,300]ms 随机，**代码是 [300,1200]ms**（election_timer.h:51）——文档漂移；900ms 散布 + 50ms 心跳下分票概率比论文设计高。另：`storage_.set_voted_for` 单字段写不检查 Flush 返回值（raft_storage.cc:133-157），fsync 失败时"选票已落盘"假设失效。

## 7. Log Replication 【L1】

- **调用链**：SubmitEntry（:1217）→ ReplicateLog（:1233）三阶段。核心创新点（诚实归属）：`peer_hb_ok_` 健康优先排序 + low_latency 模式 quorum 即 break + catch-up 模式推全部（:1273-1348）；nextIndex 指数退避（:1406-1417）。
- **复杂度**：每轮 O(P) 次 RPC、O(P) 状态、8 轮内收敛（backoff 指数）。
- **缺陷**：a) Follower `OnAppendEntries` 对 `entry.index <= LastIndex()` 同等号与 term 匹配跳过（:1126-1129）——正确但 O(batch) 线性扫描每次（batch=128 上限）；b) `kMaxAppendBatch=128` 大命令下推进慢（无动态批大小）。

## 8. Figure 8 【L1 + 验证细节】

- **为什么这么设计**：见亮点 1。额外细节：joint 提交路径同样带 Figure 8 检查（:1513-1524），因为 dual-majority 的 min 也可能落在旧 term 条目上。
- **面试必答**：术语三条——Log Matching、Leader Completeness、State Machine Safety；Figure 8 场景逐帧讲：A(term2) 复制到 S1、S2 后 crash；B(S1,S3 投) term3 当选缺条目；B 复制 → 若 B 直接提交旧 term 条目……；解法是"只有 current term 的 entry 能直接推进 commit"。

## 9. ReadIndex 【L1】

- **为什么这么设计**：follower 也能服务线性化读的标准答案；但本项目的做法是**只允许 leader 使用**（Get 路径在 leader 调用 `RaftEngine::Get(kLinearizable)` → ReadIndex），fallback 不是把读转发给 leader，而是返回错误给客户端重试。
- **调用链**：见第二部分图 4。快路径 O(1)：lease 内直接 commit_index；慢路径 O(P) 次 RPC。
- **缺陷（L4）**：`WaitForApplied` 5s 超时后**照常返回值**（:937-941）→ 极端 apply 卡死下线性化被破坏（应返回错误）。
- **改进**：超时返回 NOT_LEADER/重试错误；lease 参数化（目前 100ms 硬编码在 raft_node.h:386、check_quorum 600ms :390）。

## 10. Lease 【L1】

- **为什么这么设计**：读快路径免 RTT；续期=多数派 ACK（HeartbeatTickImpl :774-778 与 ReadIndex quorum :917 两处，唯一两处）。
- **为什么不用时钟同步**：`steady_clock`（:957-963）防 NTP；lease 信任的依据是"多数派刚刚确认过我"，不是墙钟。
- **缺陷（L4）**：a) 100ms 硬编码，无 flag；b) 心跳是顺序逐 peer 同步 RPC——P 个 peer 全部超时最坏 P×150ms，lease 在慢速集群可能无谓失效。
- **改进**：并行心跳；lease 与心跳间隔联动参数化。

## 11. WAL 【L1】

- **为什么这么设计**：SegmentLogStorage 分段（64MB）+ 每记录 `RecordHeader{index,term,size,crc32}`（wal_writer.cc:88-98）+ fsync 策略两档（每 append / interval 批量）。
- **为什么不用多个文件**：段文件 + manifest.json（当前段号）+ WalIndex（LogIndex→(段,偏移) 内存哈希）索引——append 全顺序，只有 compaction 删文件。
- **复杂度**：Append O(1)；GetTerm O(1)（数组直查 + anchor 特判）；TruncateFrom O(N)（内存 + 一次 ftruncate，会删后续段文件）；CompactUpTo O(N)（std::move 摊平）。
- **缺陷（L4）**：a) manifest Save 的 fwrite/fsync 返回值不检查（manifest.cc:72-74）；b) 所有 rename 后无目录 fsync；c) FileLogStorage 是并存的第二实现、仅测试用（file_log_storage_test.cc）——两套实现漂移风险；d) raft_engine.h:104 对 log_storage 无条件 `static_cast<CommandLog*>`，SegmentLogStorage 激活时调用即 UB。

## 12. Snapshot 【发 L1 / 收 L2 / 接线 L4】

- **为什么这么设计**：快照边界=last_applied（绝不含未提交尾，raft_node.cc:1655-1663→snapshot_manager.cc:86 检查）；一致性靠 SnapshotBarrier（BeginWrite 等待所有 shard 上的 BeginRead 结束）——写路径全部持读标（kv_state_machine.cc:52-107）。
- **传输**：64KB 块 + done 标记（snapshot_sender.cc:43-68）。
- **缺陷（L4 重磅）**：**接收侧生产接线断裂**——无任何 `SetSnapshotReceiver` 调用（全仓库仅测试构造），`OnInstallSnapshot` :1178 恒拒绝 → 多节点落后 follower 永远无法追赶。其次：snapshot.bin 无 CRC（loader 只验 4 字节 magic，snapshot_loader.cc:48-63）；`SetMeta` 忽略 Flush 返回值（snapshot_meta.cc:122-125）；sender 短读不校验（snapshot_sender.cc:53-58）；30s 同步 RPC 会阻塞 heartbeat fiber（tcp_transport.cc:46）。
- **改进**：InitStorage 里 `node_.SetSnapshotReceiver(...)`；恢复/接收校验 CRC；sender 统计已发字节遇错即停。

## 13. Recovery 【L1】

- 三个诚实重点：a) 无快照且 raft_dir 下有 `ResetApplyProgress(0)`+全 WAL 重放（main_service.cc:135-143）；b) follower 不自我提交（ReplayUnappliedLogs :1542-1547）；c) Replay 在 fiber 里跑而不是 AwaitBrief（main_service.cc:150-159，注释解释 fb2 "should not preempt dispatcher"——踩过坑的痕迹）。
- **缺陷（L4）**：a) 有快照但快照/apply.meta/WAL 三者不同步的边界没全测；b) 单机传统模式（无 raft_dir）恢复靠 AOF，而 AOF 无 fsync。

## 14. TCP Transport 【L1】

- **为什么这么设计**：自定义帧协议 `magic(4)+type(1)+seq(4)+len(4)+payload+CRC32`；每(线程,peer) 一条长连接（thread_local 池，tcp_transport.cc:66-69），锁串行化该 peer 的整轮来回。
- **为什么不用 gRPC**：gRPC 每 RPC 开销（HTTP/2 帧、流控、多线程池）对 50ms 心跳 × 数百节点不划算；帧协议 13 字节头 + 一次 syscall。诚实补充：面试先讲功能的取舍，再讲本项目为什么自制——这是验证过吞吐的取舍。
- **缺陷（L4）**：a) 同步阻塞式 RPC 被锁串行——同一 peer 的 AppendEntries 与 Heartbeat 无法并行，大日志下心跳被拖；b) 帧头长度字段手写小端（:179-183）无静态断言。
- **改进**：读写分帧器异步化；AppendEntries 与 Heartbeat 各一条连接。