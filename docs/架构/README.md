# 系统架构 (v2)

## 一、启动流程

```
main()
  ├─ MainInitGuard — 初始化日志、flags
  ├─ ProactorPool  — 创建 N 个 proactor 线程（每 CPU 一个）
  │   └─ IOUring 或 Epoll 事件循环
  ├─ AcceptServer  — 管理监听套接字
  │   ├─ HttpListener (端口 8080) — 监控/指标页面
  │   └─ Listener (端口 6380)     — Redis 协议
  └─ RunEngine()
      ├─ Service 初始化
      │   ├─ CommandRegistry 注册命令
      │   ├─ EngineShardSet 初始化分片
      │   └─ RaftEngine 初始化（内部创建 KvStateMachine + RaftGroup）
      ├─ acceptor.Run() — 开始接受连接
      └─ acceptor.Wait() — 等待退出
```

## 二、网络层（helio/）

### 2.1 Proactor（事件循环）

每个 proactor 运行在一个独立 pthread 中，包含：

- **EpollProactor** — 基于 epoll 的事件循环
- **UringProactor** — 基于 io_uring，支持异步系统调用

核心操作：

```
Run() → 轮询 I/O 事件 → 调度 fiber → 执行回调
DispatchBrief(f)   — 向 proactor 投递任务（不等待）
AwaitBrief(f)      — 投递并阻塞等待完成
```

### 2.2 Fiber（协程）

基于 boost.context 的用户态协程，单线程内协作式调度：

```
Fiber(fn)             — 创建并启动 fiber
ThisFiber::Yield()    — 主动让出 CPU
ThisFiber::SleepFor() — 休眠 fiber（不阻塞线程）
FiberQueue            — MPSC 任务队列，shard 专用
```

Fiber 的优势：轻量级（数万 fiber 可共存）、无锁同步（EventCount）、避免线程上下文切换。

### 2.3 FiberSocketBase（异步套接字）

```
Connect() / Accept() / Recv() / Write() / Shutdown()
RegisterOnRecv(cb) — 注册数据到达回调
ProvidedBuffer     — io_uring 零拷贝接收缓冲区
```

## 三、连接管理

### 3.1 Listener（连接接受）

```
Listener::NewConnection(proactor) → Connection 对象
           │
           ├─ 协议选择：REDIS / MEMCACHE
           ├─ SSL/TLS 初始化
           └─ 分配给 proactor 线程
```

连接分配策略：

- 默认：round-robin 轮流分配给各 proactor
- `--conn_use_incoming_cpu`：按 NIC 的 CPU 亲和性分配
- `--conn_threads`：限定使用的线程数

### 3.2 Connection（连接生命周期）

```
Accept → HandleRequests()
  ├─ 创建 ConnectionContext
  ├─ TLS 握手（如果需要）
  └─ InputLoop():
       1. DoRead() → 读取数据到 IoBuf
       2. 解析协议（Redis RESP 或 Memcached ASCII）
       3. 每个解析完成的命令调用 service->DispatchCommand()
       4. ReplyReadyCommands() → 发送响应
  └─ 清理：关闭 socket，从 listener 解注册
```

### 3.3 ConnectionContext（连接上下文）

```
ConnectionContext
  ├─ parsed_head → parsed commands 链表头
  ├─ parsed_tail → parsed commands 链表尾
  ├─ to_execute  → 下一个待分发的命令
  ├─ cid         → 当前命令的 CommandId
  ├─ shard_set   → EngineShardSet 引用
  └─ reply_builder_ → 响应构建器
```

### 3.4 ParsedCommand（解析后的命令）

```cpp
struct ParsedCommand {
  sds* tokens;                  // 参数数组
  unsigned argc;                // 参数个数
  ParsedCommand* next;          // 链表下一节点
  Response resp;                // 响应（monostate/Error/SimpleString/BulkString/Null）
};
```

支持流水线：多条命令在链表上排队，`ReplyReadyCommands()` 依次发送响应。

## 四、命令分发

### 4.1 Service

中心服务对象，拥有命令注册表和分片引擎：

```cpp
class Service {
  CommandRegistry registry_;    // 命令注册表
  EngineShardSet shard_set_;    // 分片引擎
  RaftEngine raft_engine_;      // Raft 共识引擎
  ProactorPool& pp_;           // 线程池
};
```

### 4.2 命令注册

```cpp
void Service::RegisterCommands() {
  registry_ << CI{"PING", CO::STALE | CO::FAST, -1, 0, 0, 0}.HFUNC(Ping)
            << CI{"SET",  CO::WRITE | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(Set)
            << CI{"GET",  CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(Get)
            << CI{"DEBUG", CO::RANDOM | CO::READONLY, -2, 0, 0, 0}.HFUNC(Debug)
            << CI{"INFO", CO::READONLY | CO::LOADING | CO::STALE, -1, 0, 0, 0}.HFUNC(Info);
}
// COMMAND 在 CommandRegistry 构造函数中注册
```

CommandId 属性：

| 字段 | 说明 |
|------|------|
| name | 命令名 |
| mask | 选项掩码（WRITE / READONLY / FAST / STALE 等） |
| arity | 参数个数（正数=固定，负数=最少） |
| first_key / last_key / step | key 位置信息 |

### 4.3 命令列表

| 命令 | 功能 | 复杂度 |
|------|------|--------|
| PING | 健康检查 | O(1) |
| SET | 设置 key-value | O(1) |
| GET | 获取 key 的值 | O(1) |
| DEL | 删除 key | O(1) |
| EXPIRE | 设置 key 的过期时间 | O(1) |
| DEBUG | 调试命令 | - |
| INFO | 服务器信息统计 | O(1) |
| COMMAND | 命令查询（INFO/DOCS/COUNT） | O(N) |

### 4.4 分发流程

```
DispatchCommand(args, cntx)
  ├─ 命令名转大写
  ├─ registry_.Find(cmd) → CommandId
  ├─ 校验参数个数 (arity)
  ├─ cntx->cid = cid
  └─ cid->Invoke(args, cntx)
       └─ 命令对应的 handler 函数
           └─ 写命令 → raft_engine_.SubmitCommand(cid, args)
           └─ 读命令 → raft_engine_.SubmitCommand(cid, args)
                         └─ 内部识别为读，直接 kv_.Apply()
```

## 五、Raft 共识层

### 5.1 模块总览

```
RaftEngine                         — 对外入口，包装 RaftGroup + KvStateMachine + CommandLog
  ├─ RaftGroup                     — GroupId + RaftNode
  │   └─ RaftNode                  — 角色状态机（Follower/Candidate/Leader）
  │       ├─ ElectionTimer          — 选举超时定时器（150-300ms 随机）
  │       ├─ HeartbeatFiber         — 周期性心跳 fiber（默认 50ms）
  │       ├─ PeerManager            — 对等节点管理（增删查）
  │       ├─ Transport*             — RPC 通信抽象接口
  │       ├─ ILogStorage*           — 日志存储接口（CommandLog）
  │       └─ IStateMachine*         — 状态机接口（KvStateMachine）
  ├─ KvStateMachine (IStateMachine) — 将命令应用到 DbSlice
  ├─ CommandLog (ILogStorage)       — 有序命令日志（1-indexed vector）
  ├─ CommandEncoder                — 命令序列化（SET/DEL/EXPIRE → ReplicatedCommand）
  └─ ReplicatedCommand             — 可复制的命令结构（type + args + Serialize）
```

### 5.2 RaftEngine（共识引擎入口）

`server/raft/raft_engine.h` — 最外层的共识接口：

```cpp
class RaftEngine {
  ApplyResult SubmitCommand(const CommandId* cid, CmdArgList args);
  bool Expire(DbIndex db_ind, std::string_view key, uint64_t expire_at_ms);
  OpResult<std::string> Get(DbIndex db_ind, std::string_view key);
  size_t DbSize(DbIndex db_ind) const;
  void Schedule(DbIndex db_ind, std::string_view key,
                std::function<void(EngineShard*)> cb);

  RaftGroup& group();
  CommandLog& log();

 private:
  KvStateMachine kv_;    // 状态机（直接读写存储）
  RaftGroup group_;      // 当前 Raft 组（节点+角色+peer）
  CommandLog log_;       // 暂存日志
};
```

`SubmitCommand` 流程：
1. **命令编码**：调用 `CommandEncoder::Encode()` 判断是否为写命令。非写命令（读）**直接调用 `kv_.Apply()` 绕过 Raft 共识**。
2. **Leader 检查**：写命令必须由 Leader 节点处理，否则返回 ERROR。
3. **单节点快速路径**（`PeerCount() == 0`）：调用 `FastCommitPath()`，依次执行 `log_.Append()` → `AdvanceCommitIndex()` → `ApplyCommittedLogs()`，完成本地提交。
4. **多节点路径**（存在 peer）：追加日志到 `CommandLog`，调用 `RaftNode::ReplicateLog()` 向所有 peer 发送 `AppendEntries`，收集多数派响应后推进 commit_index 并应用已提交日志。

### 5.3 RaftNode（角色状态机）

`server/raft/raft_node.h`

所有角色切换统一经过 `SetRole(RaftRole)`，管理 timer 和 fiber 生命周期：

```
SetRole(Follower):
  - 重置 voted_for_ / vote_count_
  - 启动/重置 ElectionTimer
  - 停止 HeartbeatFiber

SetRole(Candidate):
  - term_++
  - 自投票（voted_for_ = node_id_）
  - 停用 ElectionTimer（选举期间不超时）
  - 停止 HeartbeatFiber

SetRole(Leader):
  - 设置 leader_term_ = term_
  - 停用 ElectionTimer
  - 启动 HeartbeatFiber（默认 50ms 间隔）
```

```
角色转换：

Follower
  ├─ OnElectionTimeout() → Candidate
  ├─ OnHeartbeat()       → 重置选举超时，保持 Follower
  └─ OnRequestVote()     → 按规则投票，保持 Follower

Candidate
  ├─ StartElection()     → 自增 term，向所有 peer 发送 VoteRequest
  │   └─ 获得多数票     → TryBecomeLeader() → BecomeLeader()
  │   └─ 收到更高 term   → BecomeFollower()
  └─ OnAppendEntries()   → BecomeFollower()

Leader
  ├─ ReplicateLog()      → 向所有 peer 发送 AppendEntries
  │   └─ AdvanceCommitIndex() + ApplyCommittedLogs()
  ├─ HeartbeatLoop()     → 周期性 fiber 发送心跳（默认 50ms）
  └─ 收到更高 term       → BecomeFollower()
```

关键方法：

| 方法 | 说明 |
|------|------|
| `SetRole(role)` | 统一角色转换入口，管理 timer/fiber 生命周期 |
| `BecomeFollower(term)` | 转为 Follower，重置 voted_for，重置选举定时器 |
| `BecomeCandidate()` | term++，自投票，发起选举 |
| `BecomeLeader()` | 转为 Leader，停用选举定时器，启动心跳 fiber |
| `OnElectionTimeout()` | Follower → Candidate（ElectionTimer 回调） |
| `StartElection()` | 向所有 peer 发送 VoteRequest 收集响应 |
| `OnRequestVote()` | 按 Raft 规则投票（term/log 新旧比较） |
| `OnHeartbeat()` | 重置选举超时，保持 Follower |
| `OnAppendEntries()` | 接收日志条目，冲突解决（TruncateFrom），更新 commit_index |
| `ReplicateLog()` | Leader 向所有 peer 复制日志，推进 commit_index |
| `AdvanceCommitIndex()` | 统计多数派已复制的日志索引 |
| `ApplyCommittedLogs()` | 将 committed 日志通过 IStateMachine 应用到存储 |
| `StartHeartbeat(ms)` | 启动周期性心跳 fiber |
| `StopHeartbeat()` | 停止心跳 fiber |
| `HeartbeatLoop()` | fiber 主循环：`while → SendHeartbeatToPeers() → SleepFor(interval)` |

### 5.4 RaftStorage（持久状态）— 脚手架

`server/raft/raft_storage.h`

```
RaftStorage
  ├─ current_term() / set_current_term()
  └─ voted_for() / set_voted_for()
```

**当前状态：** `RaftStorage` 已定义但 **尚未接入**。`RaftNode` 的 `term_` 和 `voted_for_` 以成员变量形式直接管理，`RaftStorage` 预留为后续磁盘持久化接口。日志条目由 `CommandLog` 在内存中管理（`std::vector<LogEntry>`，1-indexed，`[0]` 为哨兵项）。

### 5.5 CommandLog（命令日志 — ILogStorage 实现）

`server/raft/command_log.h` + `server/raft/log_storage.h`

实现 `ILogStorage` 接口，纯内存存储（`std::vector<LogEntry>`，1-indexed，index 自动分配）：

| 方法 | 说明 |
|------|------|
| `LogSize()` | 日志条目数 |
| `LastIndex()` / `LastTerm()` | 最后一个条目的 index/term |
| `Get(index)` | 获取指定索引条目 |
| `Append(entry)` | 追加条目，自动分配 index |
| `GetRange(start, limit)` | 批量读取 |
| `TruncateFrom(new_last)` | 冲突时截断尾部 |
| `Clear()` | 清空 |

`RaftNode` 通过 `ILogStorage*` 接口访问日志，方便后续替换为持久化实现。

### 5.6 RPC 数据结构

所有 RPC 目前为内存级函数调用，尚未接入网络传输。

**Vote RPC** (`server/raft/vote_rpc.h`)：

```cpp
struct VoteRequest {
  Term term = 0;
  NodeId candidate_id;
  LogIndex last_log_index = 0;
  Term last_log_term = 0;
};

struct VoteResponse {
  Term term = 0;
  bool vote_granted = false;
};
```

**AppendEntries RPC** (`server/raft/append_entries_rpc.h`)：

```cpp
struct AppendEntriesRequest {
  Term term = 0;
  NodeId leader_id;
  LogIndex prev_log_index = 0;       // 前一日志索引（一致性检查）
  Term prev_log_term = 0;            // 前一日志 term
  std::vector<LogEntry> entries;     // 待复制日志条目
  LogIndex leader_commit = 0;        // Leader 已提交索引
};

struct AppendEntriesResponse {
  Term term = 0;
  bool success = false;              // 日志复制是否成功
  LogIndex last_log_index = 0;       // Follower 最新日志索引
};
```

**Heartbeat RPC** (`server/raft/heartbeat_rpc.h`)：

```cpp
struct HeartbeatRequest {
  Term term = 0;
  NodeId leader_id;
};

struct HeartbeatResponse {
  Term term = 0;
  bool success = false;
};
```

### 5.7 ElectionTimer（选举超时）

`server/raft/election_timer.h`

```
ElectionTimer(RaftNode* node)
  ├─ Start()   — 启动 fiber，随机等待 150-300ms 后触发 OnElectionTimeout
  ├─ Reset()   — 重置随机等待时间
  └─ Stop()    — 停止 fiber
```

基于独立 fiber 实现，每次超时后自动重置随机间隔。收到合法心跳或 AppendEntries 时由外部调用 `Reset()` 延后选举。

### 5.8 RaftGroup（分组）

`server/raft/raft_group.h`

```cpp
class RaftGroup {
  GroupId group_id_;
  RaftNode node_;
};
```

当前为单组设计（group_id=0），对外暴露 `node()` 访问 RaftNode。

### 5.9 Transport（RPC 通信抽象）

`server/raft/transport.h`

```cpp
class Transport {
  virtual VoteResponse SendVoteRequest(NodeId, VoteRequest) = 0;
  virtual HeartbeatResponse SendHeartbeat(NodeId, HeartbeatRequest) = 0;
  virtual AppendEntriesResponse SendAppendEntries(NodeId, AppendEntriesRequest) = 0;
};
```

纯虚接口，与 `RaftNode` 通过指针关联。当前仅有进程内实现 `LocalTransport`（`server/raft/local_transport.h`），通过 `NodeId → RaftNode*` 映射表直接调用目标节点的 `OnRequestVote()` / `OnHeartbeat()` / `OnAppendEntries()`。网络传输（TCP/gRPC）预留接口。

### 5.10 CommandEncoder（命令编码）

`server/raft/command_encoder.h` + `server/raft/replicated_command.h`

```cpp
enum class CommandType : uint8_t { SET = 0, DEL = 1, EXPIRE = 2 };

struct ReplicatedCommand {
  CommandType type;
  vector<string> args;
  string Serialize() const;                // "SET key val"
  static ReplicatedCommand Deserialize(string_view data);
};

class CommandEncoder {
  static optional<ReplicatedCommand> Encode(CommandId*, CmdArgList args);
  static bool IsWriteCommand(CommandId*);
};
```

- 只支持 `SET`、`DEL`、`EXPIRE` 三个写命令的编码
- 读命令 `Encode()` 返回 `nullopt`，直接本地执行（绕过 Raft）
- `Serialize()` 将命令序列化为空格分隔字符串（如 `"SET a 1"`）
- `Deserialize()` 从日志条目中还原 `ReplicatedCommand`

### 5.11 PeerManager（对等节点管理）

`server/raft/peer_manager.h`

```cpp
class PeerManager {
  void AddPeer(NodeId);          // 幂等添加
  bool RemovePeer(NodeId);       // 不存在返回 false
  size_t PeerCount() const;      // peer 数量
  size_t ClusterSize() const;    // peer + 自身
  const vector<NodeId>& GetPeerIds() const;
};
```

基于 `std::vector<NodeId>` 的有序管理，用于：
- 多数派计算：`majority = ClusterSize()/2 + 1`
- 广播 RPC：遍历 `GetPeerIds()` 向每个 peer 发送
- 当前为静态成员配置，后续可扩展为动态 Join/Leave

## 六、状态机

### 6.1 IStateMachine（状态机接口）

`server/state_machine/state_machine.h`

```cpp
enum class ApplyOp : uint8_t { OK = 0, NOT_FOUND = 1, ERROR = 2 };

struct ApplyResult {
  ApplyOp op = ApplyOp::OK;
  uint64_t affected_rows = 0;
};

class IStateMachine {
  virtual ApplyResult Apply(const CommandId* cid, CmdArgList args) = 0;
  virtual ApplyResult ApplyLogEntry(const LogEntry& entry) = 0;
  virtual void Set(DbIndex db_ind, std::string_view key, std::string_view val) = 0;
  virtual bool Del(DbIndex db_ind, std::string_view key) = 0;
  virtual bool Expire(DbIndex db_ind, std::string_view key, uint64_t expire_at_ms) = 0;
  virtual OpResult<std::string> Get(DbIndex db_ind, std::string_view key) = 0;
  virtual size_t DbSize(DbIndex db_ind) const = 0;
};
```

两个入口：
- `Apply()` — 直接执行（读命令路径，bypass Raft 共识；写命令路径，由 `FastCommitPath` 调用）
- `ApplyLogEntry()` — 从 Raft 日志条目中解析命令并执行（`RaftNode::ApplyCommittedLogs` 调用，适用于单节点快速路径和多节点共识路径）

### 6.2 KvStateMachine（KV 状态机实现）

`server/state_machine/kv_state_machine.h`

将命令映射到 DbSlice 操作：

```
Apply(cid, args)
  ├─ "SET" → Set(0, key, val)
  ├─ "DEL" → Del(0, key)
  └─ 其他 → ApplyOp::ERROR

ApplyLogEntry(entry)
  ├─ 解析 entry.command："SET key val" / "DEL key" / "EXPIRE key ms"
  ├─ 调用 Set() / Del() / Expire()
  └─ 返回 ApplyResult
```

跨线程写入通过 `shard_set_->Await(sid, lambda)` 投递到目标分片线程执行。

## 七、存储引擎

### 7.1 分片架构

数据按 key 哈希值分片，每个分片绑定到一个 proactor 线程：

```cpp
ShardId Shard(key, shard_num) {
  hash = XXH64(key, 120577);  // 哈希函数
  return hash % shard_num;
}
```

每个分片包含：

```
EngineShard (thread_local)
  ├─ db_slice_ — 数据库切片
  └─ queue_    — FiberQueue（MPSC 任务队列）
```

跨线程通信通过 `FiberQueue` + `Await`：

```cpp
shard_set_.Await(sid, [&] {
  EngineShard::tlocal()->db_slice.AddOrFind(0, key);
});
```

### 7.2 DbSlice（数据库切片）

每个分片持有完整的数据库阵列：

```cpp
class DbSlice {
  vector<DbRedis> db_arr_;  // db0, db1, ...
};

struct DbRedis {
  unique_ptr<MainTable> main_table;  // absl::flat_hash_map<string, string>
};
```

关键操作：

| 方法 | 说明 |
|------|------|
| `Find(db, key)` | 查找 key，返回 `OpResult<Iterator>` |
| `AddOrFind(db, key)` | 查找或创建空值 |
| `Del(db, key)` | 删除 key |
| `SetExpire(db, key, ms)` | 设置过期时间 |
| `DbSize(db)` | 返回 key 数量 |
| `ActivateDb(db)` | 激活指定数据库 |

当前存储模型：`flat_hash_map<string, string>`，仅支持字符串值，后续可扩展为支持 Redis 数据类型。

### 7.3 StringSet（字符串集合）

`string_set/` 模块是一个内存优化的哈希集合实现，为未来支持 Redis SET 类型做准备。

对比标准实现：

| 实现 | 负载100% | 负载75% |
|------|----------|---------|
| dictEntry (24+8B) | 32N | 35N |
| StringSet (8B/bucket) | ~11.2N | ~13N |

特点：

- 8 字节标记指针（SuperPtr），高位存标记位
- 碰撞时采用位移策略（相邻空桶优先）
- 链式解决（LinkKey）
- 扩容时将桶数组翻倍

## 八、协议解析

### 8.1 Redis RESP

支持 inline 和 multi-bulk 两种格式：

```
Inline:    PING\r\n
MultiBulk: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
```

解析状态机：

```
INIT → ARRAY_LEN (for *)
     → PARSE_ARG (for $)
     → INLINE    (for plain text)
PARSE_ARG → BULK_STR → FINISH_ARG
INLINE    → CMD_COMPLETE
```

零拷贝优化：bulk string 直接引用输入缓冲区，跨包的大字符串暂存到 heap。

### 8.2 Memcached ASCII

支持的命令：

- 存储：`SET` / `ADD` / `REPLACE`
- 读取：`GET` / `GETS`
- 其他：`DELETE`

解析后通过 `DispatchMC()` 转换为 Redis 命令：

```
ADD      → SET NX
REPLACE  → SET XX
GET/GETS → GET
```

## 九、响应构建

```
ReplyBuilder
  ├─ Protocol 分派：Redis RESP / Memcached ASCII
  ├─ SendOk()         → +OK\r\n
  ├─ SendError()      → -ERR message\r\n
  ├─ SendBulkString() → $len\r\nstr\r\n
  ├─ SendSimpleString → +str\r\n
  └─ SendNull()       → $-1\r\n
```

支持批处理模式：流水线请求时，多个响应合并写入，减少系统调用。

## 十、持久化

### 10.1 模块结构

```
PersistenceManager
  ├─ AOF           — Append-Only File，记录所有写命令
  └─ Snapshot      — 全量快照保存/加载（SnapshotEncoder / SnapshotDecoder）
```

### 10.2 AOF（Append-Only File）

- 默认启用
- 所有 SET/DEL 命令追加记录到 `appendonly.aof`
- 按行记录，格式接近 RESP 协议

### 10.3 Snapshot（快照）

- 手动触发：`redis-cli -p 6380 SAVE` → 生成 `snapshot.bin`
- 自动触发：`--snapshot_time_sec=60 --snapshot_cmd_count=1000`
- 启动时恢复：先加载 `snapshot.bin`，再回放 `appendonly.aof`

## 十一、数据流全路径

### 11.1 读命令路径（Raft bypass）

读命令（如 GET）不经过 Raft 共识，直接由本地 KvStateMachine 处理：

```
客户端
  │
  ▼ TCP → Parse → DispatchCommand()
                      │
                      ▼
            RaftEngine::SubmitCommand(cid, args)
                      │
                      ├─ CommandEncoder::Encode() → nullopt ← 非写命令
                      └─ kv_.Apply(cid, args)        ← bypass Raft
                           └─ Shard(key) → shard_set_.Await(sid, [&] {
                               EngineShard::tlocal()->db_slice.Find(key)
                           })
```

### 11.2 写命令路径（单节点 — FastCommitPath）

单节点模式（`PeerCount() == 0`），写命令走快速提交路径：

```
客户端 → TCP → Parse → DispatchCommand()
                              │
                              ▼
                    RaftEngine::SubmitCommand(cid, args)
                              │
                              ├─ CommandEncoder::Encode() → ReplicatedCommand
                              ├─ Leader 检查
                              ├─ FastCommitPath()
                              │   ├─ log_.Append(entry)
                              │   ├─ AdvanceCommitIndex()
                              │   ├─ ApplyCommittedLogs()
                              │   │   └─ KvStateMachine::ApplyLogEntry(entry)
                              │   │       └─ Shard() → Await(sid, lambda)
                              │   │           └─ DbSlice 写入
                              │   └─ 返回 ApplyResult
                              └─ 返回响应给客户端
```

### 11.3 写命令路径（多节点 — Raft 共识集成）

多节点模式（存在 peer），写命令走完整 Raft 复制流水线：

```
客户端 → TCP → Parse → DispatchCommand()
                              │
                              ▼
                    RaftEngine::SubmitCommand(cid, args)
                              │
                              ├─ CommandEncoder::Encode() → ReplicatedCommand
                              ├─ Leader 检查
                              ├─ log_.Append(entry)
                              ├─ RaftNode::ReplicateLog()
                              │   ├─ 向所有 peer 发送 AppendEntries（通过 Transport）
                              │   ├─ 收集响应
                              │   └─ AdvanceCommitIndex()  ← 多数派确认
                              ├─ ApplyCommittedLogs()
                              │   └─ KvStateMachine::ApplyLogEntry(entry)
                              │       └─ Shard() → Await(sid, lambda)
                              │           └─ DbSlice 写入
                              └─ 返回响应给客户端
```

**当前限制：**
- RPC 通信仅通过 `LocalTransport` 在同一进程内模拟，尚未接入 TCP/gRPC 网络传输
- `RaftStorage` 尚未接入，term/voted_for 由 RaftNode 成员变量直接管理
- `CommandLog` 为纯内存实现，重启后丢失

## 十二、并发模型

| 层级 | 模型 |
|------|------|
| I/O 事件 | io_uring / epoll 异步事件驱动 |
| 任务调度 | Fiber 协程（boost.context） |
| 数据分片 | Shard 绑定到固定线程，无锁访问 |
| 跨线程通信 | FiberQueue（MPSC）+ Proactor::AwaitBrief |
| 同步原语 | EventCount、Done、CondVarAny（协程友好） |

关键原则：

- **每分片单线程**：`DbSlice` 无锁，串行化通过 FiberQueue
- **消息传递**：跨线程不共享数据，通过任务队列通信
- **协程阻塞不阻塞线程**：`Await` 只阻塞 fiber，线程继续处理其他 fiber

## 十三、配置项

| flag | 默认值 | 说明 |
|------|--------|------|
| `--port` | 6380 | Redis 端口 |
| `--http_port` | 8080 | HTTP 端口（负数=关闭） |
| `--memcache_port` | 0 | Memcached 端口（0=关闭） |
| `--force_epoll` | false | 强制使用 epoll（而非 io_uring） |
| `--conn_threads` | 0 | 连接处理线程数（0=全部） |
| `--conn_use_incoming_cpu` | false | 按 NIC CPU 亲和性分配连接 |
| `--tls` | false | 启用 TLS |
| `--tls_key_file` | — | TLS 私钥路径 |
| `--tls_cert_file` | — | TLS 证书路径 |
| `--snapshot_time_sec` | — | 自动快照间隔（秒） |
| `--snapshot_cmd_count` | — | 自动快照触发命令数 |

## 十四、目录结构

```
server/
├── protocol/              RESP 协议解析
│   ├── conn_context.h
│   ├── redis_parser.h
│   ├── memcache_parser.h
│   ├── resp_expr.h
│   ├── reply_builder.h
│   └── dfly_protocol.h
│
├── service/               Service 层
│   ├── main_service.h     命令处理器（SET, GET, DEL, ...）
│   ├── command_registry.h 命令注册和分发
│   ├── command_serializer.h
│   ├── snapshot_fiber.h   自动快照
│   ├── state_serializer.h 导出/导入（SnapshotData）
│   └── dfly_main.cc       入口点
│
├── storage/               存储层
│   ├── db_slice.h         per-shard key-value store
│   ├── engine_shard_set.h 分片引擎
│   ├── common_types.h     PrimeValue, MainTable
│   └── op_status.h
│
├── state_machine/         状态机抽象
│   ├── state_machine.h    IStateMachine 接口
│   ├── kv_state_machine.h KvStateMachine
│   └── kv_state_machine.cc
│
├── raft/                  Raft 共识层
│   ├── raft_types.h       RaftRole, Term, LogIndex, NodeId, LogEntry
│   ├── raft_storage.h     持久状态脚手架（term/voted_for，尚未接入）
│   ├── raft_node.h/.cc    角色状态机（Follower/Candidate/Leader）
│   ├── raft_group.h       单 Group 包装
│   ├── raft_engine.h/.cc  RaftEngine（SubmitCommand 入口）
│   ├── command_log.h/.cc  有序命令日志（内存 ILogStorage 实现）
│   ├── log_storage.h      ILogStorage 抽象接口
│   ├── election_timer.h/.cc 选举超时 fiber（150-300ms 随机）
│   ├── timer.h            ITimer 接口
│   ├── transport.h        Transport 纯虚接口（RPC 抽象）
│   ├── local_transport.h/.cc 进程内传输实现（NodeId→RaftNode* 映射）
│   ├── peer_manager.h/.cc 对等节点管理
│   ├── command_encoder.h/.cc 命令序列化（SET/DEL/EXPIRE → ReplicatedCommand）
│   ├── replicated_command.h 可复制的命令结构
│   ├── vote_rpc.h         Vote RPC 数据结构
│   ├── append_entries_rpc.h AppendEntries RPC 数据结构
│   └── heartbeat_rpc.h    Heartbeat RPC 数据结构
│
└── persistence/           持久化层
    ├── aof_writer.h       Append-Only File writer
    ├── persistence_manager.h
    ├── snapshot_manager.h 快照编码/解码
    └── snapshot_manager_test.cc
```

## 十五、版本变更记录

| 版本 | 变更内容 |
|------|----------|
| v1 | 基础架构：网络层、协议解析、命令分发、存储引擎（DbSlice/EngineShardSet）、响应构建（ReplyBuilder）、持久化（AOF + Snapshot） |
| v2 | 完整 Raft 共识层：RaftNode（角色状态机含心跳 fiber）、RaftGroup、CommandLog/ILogStorage、ElectionTimer、Transport/LocalTransport、CommandEncoder/ReplicatedCommand、PeerManager；状态机抽象 IStateMachine/KvStateMachine；SubmitCommand 三路分流（读 bypass / 单节点 FastCommitPath / 多节点 ReplicateLog）；目录结构、数据流全路径、版本变更记录 |
