# RESP 协议兼容 — 面试讲解文档

---

## 一、为什么实现 RESP 就能兼容所有 Redis 客户端

**一句话**：Redis 客户端和服务端之间唯一的通信协议就是 RESP + TCP socket。客户端不关心服务端是否 Redis 源码。

客户端调用流程（以 redis-cli 底层 hiredis 为例）：

```
用户输入: SET a 1
    ↓
客户端命令函数: redisCommand(c, "SET %s %s", "a", "1")
    ↓
RESP编码: "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
    ↓
Socket发送: write(fd, buf, 30)
    ↓
Socket接收: read(fd, buf, len)
    ↓
RESP解码: redisReply → 打印到终端
```

各语言客户端本质一样：
- **hiredis (C)**：`redisCommand(c, "SET %s %s", key, val)`
- **Jedis (Java)**：`jedis.set("a", "1")` → `Socket.getOutputStream().write(...)`
- **go-redis (Go)**：`rdb.Set(ctx, key, val)` → `conn.Write(data)`
- **redis-py (Python)**：`r.set("a", "1")` → `sock.sendall(buf)`

**客户端没有任何 Redis 专有函数调用**，只是把 RESP 字节写入 TCP socket。服务端只要正确解析 RESP 并返回 RESP 响应，客户端就无法区分真假 Redis。

源码证据：`server/service/dragonfly_listener.cc:96-98`
```cpp
util::Connection* Listener::NewConnection(ProactorBase* proactor) {
  return new Connection{protocol_, engine_, ctx_};  // 每个TCP连接创建Connection
}
```
Listener 在 6380 端口监听，每个新 TCP 连接创建一个 Connection 对象，内部持有 RedisParser。

---

## 二、完整请求流程（结合源码）

```
redis-cli (或 Jedis/Lettuce/go-redis/redis-py)
    │
    │ TCP 连接到 6380 端口
    ▼
┌─────────────────────────────────────────────────────────┐
│ DragonflyListener                                       │
│ server/service/dragonfly_listener.cc:96                 │
│ 职责：accept TCP 连接，分配 proactor 线程，创建 Connection │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ Connection::InputLoop                                   │
│ server/service/dragonfly_connection.cc:175              │
│ 职责：Fiber 协程，DoRead() 从 socket 读数据到 IoBuf       │
│       驱动 Parser 解析，遍历 ParsedCommand 链表分发       │
│       循环处理 Pipeline 请求                             │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ Connection::ParseRedis / ParseMultiBulk                 │
│ server/service/dragonfly_connection.cc:294,454          │
│ 职责：RESP 协议状态机解析                                │
│   *3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n            │
│   → ParsedCommand{tokens=["SET","a","1"], argc=3}       │
│   支持半包（返回 NEED_MORE），支持 Pipeline（链表排队）    │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ CmdArgList = absl::Span<MutableStrSpan>                 │
│ server/storage/common_types.h:24                        │
│ 职责：零拷贝参数列表，直接引用解析结果                     │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ Service::DispatchCommand                                │
│ server/service/main_service.cc:193                      │
│ 职责：命令名转大写 → registry_.Find(cmd) O(1)查找       │
│       校验参数个数 (arity) → cid->Invoke(args, cntx)     │
└─────────────────────────────────────────────────────────┘
    │
    ├── 读命令 (GET) ──────────────────────────┐
    │                                           ▼
    │  ┌───────────────────────────────────────────────────┐
    │  │ RaftEngine::Schedule → 直接分片查找               │
    │  │ server/service/main_service.cc:326               │
    │  │ → es->db_slice.Find(0, key) 本地hash表查询       │
    │  │ → 不经过 Raft，无锁，零延迟                       │
    │  └───────────────────────────────────────────────────┘
    │
    ├── 写命令 (SET/DEL/EXPIRE) ───────────────┐
    │                                           ▼
    │  ┌───────────────────────────────────────────────────┐
    │  │ RaftEngine::SubmitCommand                         │
    │  │ server/raft/raft_engine.cc:21                     │
    │  │ CommandEncoder::Encode → ReplicatedCommand        │
    │  │ 单节点: FastCommitPath (Append+Commit+Apply)     │
    │  │ 多节点: Append → ReplicateLog → 等多数派          │
    │  │ → KvStateMachine::ApplyLogEntry                   │
    │  │ → Shard(key) → Await(sid) → DbSlice 写入          │
    │  └───────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ ReplyBuilder / RespSerializer                           │
│ server/protocol/reply_builder.cc                        │
│ 职责：RESP 格式编码响应                                  │
│   +OK\r\n     $-1\r\n     :1\r\n     $5\r\nhello\r\n   │
│   -ERR msg\r\n                                          │
│   通过 writev 零拷贝写入 socket                          │
└─────────────────────────────────────────────────────────┘
    │
    ▼
客户端收到 RESP 响应，解码显示
```

### 各层职责速查

| 层级 | 文件 | 职责 |
|------|------|------|
| DragonflyListener | `service/dragonfly_listener.cc` | TCP accept + 分配 proactor |
| Connection | `service/dragonfly_connection.cc` | Fiber I/O 循环 + 驱动 Parser |
| ParseMultiBulk | `connection.cc:454` | RESP 状态机解析 |
| CmdArgList | `storage/common_types.h:24` | 零拷贝 Span 参数列表 |
| CommandRegistry | `service/command_registry.cc` | flat_hash_map 命令查找 |
| Service | `service/main_service.cc` | 业务分发 + 读写路径分流 |
| RaftEngine | `raft/raft_engine.cc` | 写命令 Raft 共识（单节点 FastCommit） |
| DbSlice | `storage/db_slice.h` | 读直接查询 flat_hash_map |
| ReplyBuilder | `protocol/reply_builder.cc` | RESP 响应编码 |

---

## 三、RESP 协议详解

### 3.1 RESP2 数据类型

| 类型 | 前缀 | 格式 | 示例 |
|------|------|------|------|
| Simple String | `+` | `+<content>\r\n` | `+OK\r\n` |
| Error | `-` | `-<content>\r\n` | `-ERR unknown\r\n` |
| Integer | `:` | `:<number>\r\n` | `:1\r\n` |
| Bulk String | `$` | `$<length>\r\n<data>\r\n` | `$3\r\nSET\r\n` |
| Array | `*` | `*<count>\r\n<elements>` | `*3\r\n...` |
| Null (Bulk) | `$` | `$-1\r\n` | Null |
| Null Array | `*` | `*-1\r\n` | Null Array |

### 3.2 实例：SET a 1

**客户端发送的原始字节**：
```
*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n
```

**逐字节解析**：
```
*        → Array 类型
3        → 3 个元素
\r\n     → 终止符
$        → Bulk String
3        → 长度 3
\r\n
SET      → 数据 "SET"
\r\n
$        → Bulk String
1        → 长度 1
\r\n
a        → 数据 "a"
\r\n
$        → Bulk String
1        → 长度 1
\r\n
1        → 数据 "1"
\r\n      ← 最后一个 CRLF 后整条命令解析完成
```

**解析结果**：
```
CmdArgList = ["SET", "a", "1"]
ParsedCommand { tokens=[sds("SET"), sds("a"), sds("1")], argc=3 }
```

### 3.3 常见命令的 RESP 对照

| 命令 | RESP 编码 |
|------|-----------|
| `PING` | `*1\r\n$4\r\nPING\r\n` |
| `SET a 1` | `*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n` |
| `GET a` | `*2\r\n$3\r\nGET\r\n$1\r\na\r\n` |
| `DEL a` | `*2\r\n$3\r\nDEL\r\n$1\r\na\r\n` |
| `EXPIRE a 60` | `*3\r\n$6\r\nEXPIRE\r\n$1\r\na\r\n$2\r\n60\r\n` |

---

## 四、RedisParser 实现详解

### 4.1 项目中实际有两个解析器

1. **RedisParser 类**（`server/protocol/redis_parser.cc`）— 零拷贝状态机，代码已被 `#if 0` 注释，保留供参考
2. **Connection::ParseMultiBulk**（`dragonfly_connection.cc:454`）— 当前生产环境实际使用的简化版本

### 4.2 状态机设计（RedisParser 类）

7 个状态：

```
                    ┌─────────────────────────────────────┐
                    │                                     │
                    ▼                                     │
┌──────┐     '*'   ┌────────────┐    '$'    ┌────────────┐
│ INIT │ ────────► │ ARRAY_LEN  │ ────────► │ PARSE_ARG  │
│  _S  │          │     _S      │           │     _S      │
└──┬───┘          └──────┬─────┘          └──┬──┬──┬──┘
   │                     │                    │  │  │
   │ 其他字符            │ len<=0             │  │  │
   ▼                     ▼                    │  │  │
┌────────┐         ┌──────────┐              │  │  │
│ INLINE │         │  CMD_    │              │  │  │
│   _S   │         │ COMPLETE │         ┌────┘  │  └────┐
└───┬────┘         └──────────┘         ▼       ▼       ▼
    │                              ┌────────┐ ┌───┐ ┌──────────┐
    │ 读到\n                       │ BULK_  │ │NIL│ │  CMD_    │
    ▼                              │ STR_S  │ └───┘ │ COMPLETE │
┌──────────┐                       └───┬────┘       └──────────┘
│ CMD_     │                           │ 读完\r\n
│ COMPLETE │                           ▼
└──────────┘                       ┌──────────┐   ┌──────┴─────┐
                                   │ FINISH_  │──►│ 还有剩余arg │
                                   │  ARG_S   │   │ 继续循环    │
                                   └────┬─────┘   └────────────┘
                                        │ 最后一个arg
                                        ▼
                                   ┌─────────────┐
                                   │ CMD_COMPLETE │
                                   └─────────────┘
```

### 4.3 ParseMultiBulk 简化版（实际使用）

```cpp
// dragonfly_connection.cc:454-536
auto Connection::ParseMultiBulk(base::IoBuf* io_buf) -> ParserStatus {
  // 状态变量:
  // multibulk_len_ — 剩余要读的 arg 个数
  // bulk_len_      — 当前 bulk string 剩余字节数 (-1=需要先读长度前缀)

  if (multibulk_len_ == 0) {
    // 1. 读 *N\r\n → multibulk_len_ = N
    // 2. 预分配 sds tokens[N]
  }
  
  while (multibulk_len_) {
    if (bulk_len_ == -1) {
      // 读 $len\r\n → bulk_len_ = len
    }
    // 读 len 字节数据 + \r\n → sdsnewlen(data, len)
    // bulk_len_ = -1, multibulk_len_--
  }
  // 所有 arg 读完 → parse_complete = 1
}
```

### 4.4 TCP 粘包/拆包/半包问题

**问题根源**：TCP 是流式协议，没有消息边界。

- **粘包**：两次 `write` 的数据被一次 `read` 收到（`"*3\r\n..."` + `"*2\r\n..."` → 一次读到两块）
- **拆包**：一次 `write` 的数据被分两次 `read` 收到（`"*3\r\n$3\r\n"` → 第一次读半条）
- **半包**：一次 `read` 读到半条命令（`"*3\r\n$3\r\nSE"` 还在等 "T\r\n..."）

**为什么不能直接 readline**：
```
$5\r\n         ← 这是行，readline 可以读
hello\r\n      ← 5字节 + \r\n 共7字节，不是一行能解决的
```
Bulk String 不是按行分割的，`$N\r\n` 后面是 N 字节数据，必须精确读取 N 字节。

**解决策略**（项目代码中的实现）：

```cpp
// 1. 读 *N\r\n: 找不到 \r\n → 半包
size_t pos = input.find('\r', 0);
if (pos == string_view::npos) {
  return NEED_MORE;  // 返回上层继续读
}

// 2. 读 $len\r\n: 找不到或数据不够 → 半包
if (pos + 1 == input.size()) {
  return NEED_MORE;
}

// 3. 读 bulk data: 缓冲区数据不够 → 半包
if (input.size() < size_t(bulk_len_) + 2) {
  return NEED_MORE;
}
```

关键机制：
- **状态保持**：`multibulk_len_`、`bulk_len_` 保存在 Connection 对象中，不丢失
- **IoBuf 不丢弃**：未处理的数据留在 IoBuf 中，下次继续解析
- **InputLoop 循环**：`do-while` 循环，NEED_MORE 时继续 `DoRead()`
- **安全限制**：`PROTO_INLINE_MAX_SIZE=64KB`、`kMaxBulkLen=64MB`、`kMaxArrayLen=1024`

---

## 五、CommandRegistry 设计

### 5.1 为什么用 flat_hash_map 而不是 switch

```cpp
// command_registry.h:110
absl::flat_hash_map<std::string_view, CommandId> cmd_map_;
```

| | flat_hash_map | switch |
|------|------|------|
| 查找复杂度 | O(1) hash 查找 | O(1) 跳转表（仅编译期常量） |
| 扩展性 | 一行代码加命令 | 修改 switch + case |
| 命令数量 | 任意（Redis 有 200+） | 编译后固定 |
| 动态注册 | 运行时可行 | 编译时确定 |
| 元数据 | CommandId 携带 arity/key位置/flag | 只有处理函数 |

### 5.2 注册机制

```cpp
// main_service.cc:517-528
void Service::RegisterCommands() {
  using CI = CommandId;
  
  registry_ << CI{"PING", CO::STALE | CO::FAST, -1, 0, 0, 0}.HFUNC(Ping)
            << CI{"SET",  CO::WRITE | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(Set)
            << CI{"GET",  CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(Get)
            << CI{"DEL",  CO::WRITE, -2, 1, 1, 1}.HFUNC(Del)
            << CI{"EXPIRE", CO::WRITE, 3, 1, 1, 1}.HFUNC(Expire)
            << CI{"DEBUG", CO::RANDOM | CO::READONLY, -2, 0, 0, 0}.HFUNC(Debug)
            << CI{"INFO",  CO::READONLY | CO::LOADING | CO::STALE, -1, 0, 0, 0}.HFUNC(Info);
}
```

**三步拆解：**

1. **`CI{...}` 构造 CommandId**：
```cpp
// command_registry.h:54
CommandId("SET",           // 命令名
          CO::WRITE | CO::DENYOOM,  // 标记位: 写命令 + 内存满禁止
          -3,              // arity: 负数=最少参数, 正数=固定参数
          1, 1, 1);        // key 在第1个位置, 步长1
```

2. **`.HFUNC(Set)` 绑定成员函数**：
```cpp
// main_service.cc:511-516
inline CommandId::Handler HandlerFunc(Service* se, ServiceFunc f) {
  return [=](CmdArgList args, ConnectionContext* cntx) { 
    return (se->*f)(args, cntx); 
  };
}
#define HFUNC(x) SetHandler(HandlerFunc(this, &Service::x))
```

3. **`operator<<` 插入 map**：
```cpp
// command_registry.h:115-120
CommandRegistry& operator<<(CommandId cmd) {
  cmd_map_.emplace(cmd.name(), std::move(cmd));  // "SET" → CommandId
  return *this;  // 链式调用
}
```

### 5.3 CommandId 携带的元数据

```cpp
// command_registry.h:36-107
class CommandId {
  const char* name_;     // "SET"
  uint32_t opt_mask_;    // CO::WRITE | CO::DENYOOM
  int8_t arity_;         // -3: 最少3个参数
  int8_t first_key_;     // 1: 第1个arg是key
  int8_t last_key_;      // 1
  int8_t step_key_;      // 1: 单key命令
  Handler handler_;      // std::function 处理函数
};
```

这些元数据除了供分发使用，还用于 `COMMAND INFO` 命令返回给客户端。

### 5.4 分发流程

```cpp
// main_service.cc:193-218
void Service::DispatchCommand(CmdArgList deprecated, ConnectionContext* cntx) {
  // 1. 命令名转大写
  sdstoupper(parsed_cmd.tokens[0]);  // "set" → "SET"
  
  // 2. hash 表查找 O(1)
  const CommandId* cid = registry_.Find(cmd_str);
  if (cid == nullptr) {
    return cntx->SendError("unknown command...");
  }
  
  // 3. 参数个数校验
  //    arity>0: 固定参数, argc必须等于arity
  //    arity<0: 最少参数, argc必须>=|arity|
  if ((cid->arity() > 0 && argc != size_t(cid->arity())) ||
      (cid->arity() < 0 && argc < size_t(-cid->arity()))) {
    return cntx->SendError(WrongNumArgsError(cmd_str));
  }
  
  // 4. 调用 handler
  cid->Invoke(deprecated, cntx);
}
```

### 5.5 添加新命令只需一行

```cpp
registry_ << CI{"INCR", CO::WRITE, 2, 1, 1, 1}.HFUNC(Incr);
```
然后在 `Service` 类中实现 `Incr(CmdArgList, ConnectionContext*)` 即可。不用修改 switch、不用改分发逻辑。

---

## 六、读写路径分离

### 6.1 GET — 直接访问 DbSlice

```cpp
// main_service.cc:311-327
void Service::Get(CmdArgList args, ConnectionContext* cntx) {
  string_view key = string_view(pcmd.tokens[1], sdslen(pcmd.tokens[1]));
  
  auto cb = [cntx, cmd = cntx->to_execute](EngineShard* es) {
    OpResult<MainIterator> res = es->db_slice.Find(0, key);  // 直接本地查找
    if (res) {
      cntx->SendGetReply(key, 0, res.value()->second.value, cmd);
    } else {
      cntx->SendGetNotFound(cmd);
    }
  };
  engine_.Schedule(0, key, std::move(cb));  // 投递到目标分片线程
}
```

- **不经过 Raft**：读不改变状态，不需要日志复制
- **不经过 RaftEngine::SubmitCommand**：直接 Schedule 到分片线程
- **无锁**：分片绑定到固定线程，DbSlice 内 `flat_hash_map` 单线程访问

### 6.2 SET — 提交 Raft

```cpp
// main_service.cc:283-308
void Service::Set(CmdArgList args, ConnectionContext* cntx) {
  CmdArgVec cmd_vec;
  for (unsigned i = 0; i < pcmd.argc; ++i) {
    cmd_vec.emplace_back(pcmd.tokens[i], sdslen(pcmd.tokens[i]));
  }
  ApplyResult result = engine_.SubmitCommand(cntx->cid, 
      CmdArgList{cmd_vec.data(), cmd_vec.size()});
  
  if (result.op == ApplyOp::ERROR) {
    return cntx->SendError("READONLY You can't write against a non-leader");
  }
  cntx->SendStored();  // "+OK\r\n"
}
```

进入 RaftEngine：
```cpp
// raft_engine.cc:21-43
ApplyResult RaftEngine::SubmitCommand(const CommandId* cid, CmdArgList args) {
  auto cmd = CommandEncoder::Encode(cid, args);
  
  // 非写命令（PING等）→ 直接Apply，绕过Raft
  if (!cmd) {
    return kv_.Apply(cid, args);
  }
  
  // 只有 Leader 能写
  if (group_.node().role() != RaftRole::Leader) {
    return {ApplyOp::ERROR, 0};
  }
  
  // 单节点：FastCommitPath（Append+Commit+Apply 三步一次完成）
  if (group_.node().peer_manager().PeerCount() == 0) {
    return FastCommitPath(*cmd);
  }
  
  // 多节点：Append → ReplicateLog → 等多数派响应
  LogEntry entry(group_.node().term(), 0, cmd->Serialize());
  group_.log_storage()->Append(entry);
  return group_.node().ReplicateLog();
}
```

### 6.3 FastCommitPath（单节点快速路径）

```cpp
// raft_engine.cc:45-53
ApplyResult RaftEngine::FastCommitPath(const ReplicatedCommand& cmd) {
  LogEntry entry(group_.node().term(), 0, cmd.Serialize());
  group_.log_storage()->Append(entry);     // 1. 追加日志
  group_.node().AdvanceCommitIndex();      // 2. 推进 commit_index
  return group_.node().ApplyCommittedLogs(); // 3. 应用到状态机
}

// ApplyCommittedLogs → KvStateMachine::ApplyLogEntry
//   → 解析 "SET a 1" → Set(0, "a", "1")
//   → Shard("a") → Await(sid) → DbSlice::AddOrFind
```

### 6.4 读写分流对比

| | 读路径 (GET) | 写路径 (SET/DEL) |
|------|------|------|
| 流程 | Parser → Schedule → DbSlice::Find | Parser → RaftEngine → Log → Apply → DbSlice |
| Raft 参与 | 不参与 | 必须参与 |
| 串行 | 1 次（到目标分片） | 1 次（Apply 时到目标分片） |
| 锁 | 无锁（分片绑定线程） | 无锁（分片绑定线程） |
| 单节点延迟 | 一次 hash 查找 + 一次跨线程调度 | Append + Commit + Apply（无网络） |

### 6.5 为什么性能高

1. **读不需要共识**：这是 Raft 论文的标准优化——read-only operations 不需要写日志
2. **分片无锁**：每个 CPU 核心独立处理其分片数据，zero contention
3. **Fiber 协程**：Await 只阻塞 fiber 不阻塞线程，线程继续处理其他连接
4. **零拷贝**：Bulk String 直接引用 IoBuf 数据，Span 零拷贝传递

---

## 七、ReplyBuilder — RESP 响应编码

### 7.1 编码对照

| 回复内容 | 方法 | 编码结果 | 源码行 |
|----------|------|----------|--------|
| 成功 | `SendSimpleString("OK")` | `+OK\r\n` | reply_builder.cc:95 |
| 整数 1 | `SendLong(1)` | `:1\r\n` | :82 |
| 字符串 | `SendBulkString("hello")` | `$5\r\nhello\r\n` | :101 |
| 错误 | `SendError("unknown")` | `-ERR unknown\r\n` | :154 |
| Null | `SendNull()` | `$-1\r\n` | :74 |
| GET 值 | `SendGetReply(key, flags, val)` | `$N\r\nval\r\n` | :183 |
| GET 未找到 | `SendGetNotFound()` | `$-1\r\n` | :193 |

### 7.2 编码实现原理

```cpp
// Simple String: 前缀 + 内容 + CRLF
void RespSerializer::SendSimpleString(string_view str) {
  iovec v[3] = {IoVec("+"), IoVec(str), IoVec("\r\n")};
  Send(v, 3);
}

// Bulk String: $长度\r\n + 数据 + \r\n
void RespSerializer::SendBulkString(string_view str) {
  tmp[0] = '$';
  FastIntToBuffer(str.size(), tmp+1);  // 写入长度
  *next++ = '\r'; *next++ = '\n';
  iovec v[3] = {IoVec(lenpref), IoVec(str), IoVec("\r\n")};
  Send(v, 3);
}

// Integer: :值\r\n
void RespSerializer::SendLong(int64_t val) {
  tmp[0] = ':';
  FastIntToBuffer(val, tmp+1);
  *next++ = '\r'; *next++ = '\n';
  iovec v[] = {IoVec(pref)};
  Send(v, 1);
}

// Null: 硬编码
void RespSerializer::SendNull() {
  iovec v[] = {IoVec("$-1\r\n")};
  Send(v, 1);
}
```

### 7.3 为什么用 writev

```cpp
void BaseSerializer::Send(const iovec* v, uint32_t len) {
  // batch模式：合并响应
  if (should_batch_) {
    for (unsigned i = 0; i < len; ++i) {
      batch_.append((char*)v[i].iov_base, v[i].iov_len);
    }
    return;
  }
  // 直接发送：一次系统调用发送多个不连续内存块
  ec = sink_->Write(v, len);
}
```

- **scatter-gather I/O**：一次系统调用发送多块不连续内存，减少内核态切换
- **Batch 模式**：Pipeline 场景合并多个响应为一次 write

---

## 八、高频面试追问及标准回答

### 8.1 追问清单

1. 为什么兼容 RESP 就能使用 redis-cli？
2. 为什么不用 Redis 源码？
3. 为什么自己实现 RESP 协议？
4. RESP 相比 HTTP 有什么优势？
5. RESP 为什么适合数据库？
6. TCP 粘包拆包怎么处理？半包怎么解决？
7. 为什么不能用 readline？
8. 状态机怎么设计的？有多少状态？
9. 为什么用 flat_hash_map 而不是 switch 做命令注册？
10. GET 和 SET 处理路径有什么不同？为什么？
11. Pipeline 怎么实现？
12. Parser 如何支持 Pipeline？
13. 如果收到超大 Bulk String（1GB）会怎样？
14. 恶意不完整数据怎么防护？
15. 为什么用 iovec/writev 而不是 printf？
16. 为什么代码里有两个解析器（RedisParser + ParseMultiBulk）？
17. RedisParser 的零拷贝怎么做到？
18. 命令注册的 HFUNC 宏做了什么？

### 8.2 标准回答

**Q1: 为什么兼容 RESP 就能使用 redis-cli？**

> RESP 是 Redis 客户端和服务端之间唯一的通信协议。redis-cli 本质是 Socket + RESP 编解码：把用户命令按 RESP 编码 → write(fd, buf, len) → read(fd, buf, len) → 按 RESP 解码显示。只要服务端能正确解析 RESP 并返回 RESP 格式的响应，客户端就无法区分。Jedis/lettuce/go-redis/redis-py 都是同理。

**Q2: 为什么不用 Redis 源码？**

> Redis 单线程模型受限于单核。我们的项目采用多线程分片架构——key hash 到不同 CPU 核心各自处理，无锁并行。另外集成了 Raft 实现分布式一致性，这是 Redis 本身不具备的。

**Q3: 为什么自己实现 RESP 协议？**

> RESP 协议非常简单（<400 行 C++），自己实现可以获得零拷贝优化（直接引用 IoBuf 数据），并且解析器可以设计为流式状态机，配合 Fiber 协程 + io_uring 架构。引入 hiredis 反而引入其 I/O 模型假设。

**Q4: RESP 相比 HTTP 优势？**

> ① 更精简（30 字节 vs 100+ 字节）② 解析更简单（长度前缀 vs Header 解析）③ 原生 Pipeline ④ 无 Web 语义负担（Cookie/Session/CORS）。RESP 就是为数据库高频小请求设计的。

**Q5: RESP 为什么适合数据库？**

> ① 长度前缀可安全传二进制数据 ② 元信息开销极小 ③ Pipeline 减少 RTT ④ 类型覆盖数据库所有返回（OK/Error/Integer/Bulk/Null/Array）。

**Q6: TCP 粘包拆包怎么处理？**

> TCP 是流式协议无消息边界。我们的解析器保持状态变量（multibulk_len_/bulk_len_）在 Connection 对象中，数据不够返回 NEED_MORE，IoBuf 保留未处理数据，下次 DoRead 后继续解析。

**Q7: 为什么不能用 readline？**

> RESP 的 Bulk String 不是按行分割的：`$5\r\nhello\r\n` 其中 `hello` 是 5 字节精确数据，不能用 readline 读。必须用长度前缀精确控制读取字节数。

**Q8: 状态机怎么设计？**

> 7 个状态：INIT → ARRAY_LEN（*开头）/ INLINE（文本）/ PARSE_ARG（$开头）→ BULK_STR → FINISH_ARG → CMD_COMPLETE。解析栈（parse_stack_）支持嵌套 Array。ParseMultiBulk 是简化版，只用两个状态变量（multibulk_len_/bulk_len_）。

**Q9: 为什么 hash_map 而不是 switch？**

> O(1) 查找、动态注册、不修改核心分发逻辑、携带元数据（arity/key位置/flag）。Redis 200+ 命令用 switch 不可维护。

**Q10: GET 和 SET 路径不同？为什么？**

> GET 不改变状态，直接本地 DbSlice 查找，不经过 Raft。SET 是写操作，必须通过 Raft 日志复制保证一致性。读写分离是分布式系统的基本优化。

**Q11: Pipeline 实现？**

> ParsedCommand 通过 next 指针形成链表。InputLoop 循环中持续解析 + 依次分发。Batch 模式将多个响应合并为一次 writev。

**Q12: Parser 如何支持 Pipeline？**

> 流式解析：每次处理 IoBuf 中尽可能多数据，解析完一条命令重置状态为 INIT，立即解析下一条。半包时状态保持，下次继续。

**Q13: 超大 Bulk String？**

> kMaxBulkLen=64MB 硬限制，超过返回解析错误。ParseMultiBulk 中 bulk_len_ > INT_MAX 返回 ERROR 断开连接。

**Q14: 恶意数据防护？**

> PROTO_INLINE_MAX_SIZE=64KB、kMaxBulkLen=64MB、kMaxArrayLen=1024。超过返回 ERROR 断开连接。IoBuf 有 kMaxReadBufferSize=64KB 上限。

**Q15: 为什么 writev 不用 printf？**

> printf 处理格式字符串有开销，且会多次调用 write(2) 系统调用。writev 一次系统调用发送多块不连续内存（scatter-gather I/O），减少内核态切换。

**Q16: 为什么有两个解析器？**

> RedisParser 类是最初设计的完整状态机，支持嵌套 Array。后来发现实际 99% 流量是简单 bulk 格式，就在 Connection 里写了更直接的 ParseMultiBulk。老代码保留但被注释，测试仍在跑。

**Q17: 零拷贝怎么做的？**

> ConsumeBulk 中：`bulk_str = str.subspan(0, bulk_len_)`，直接引用 IoBuf 数据。只有跨包大字符串才 stash 到 heap。Bulk String 的 Span 延续到 CmdArgList → handler 的全路径都不拷贝。

**Q18: HFUNC 宏做了什么？**

> `#define HFUNC(x) SetHandler(HandlerFunc(this, &Service::x))` 将 `Service::Set` 成员函数包装成 `std::function<void(CmdArgList, ConnectionContext*)>`，捕获 this 指针实现 lambda 闭包。

---

## 九、面试回答模板（3 分钟）

> 我项目中兼容 Redis 客户端的关键在于完整实现了 RESP 协议层。整个请求链路从 TCP accept 开始，DragonflyListener 在 6380 端口创建 Connection，启动 Fiber 协程进入 InputLoop 读取数据。
>
> Parser 是流式状态机，按 `*N\r\n$len\r\n...` 格式逐字节解析 RESP，生成 ParsedCommand 结构。关键设计是半包处理——状态变量保存在 Connection 对象中，返回 NEED_MORE 时 IoBuf 保留数据，下次继续解析。解析完的命令通过链表排队支持 Pipeline。
>
> 命令分发用 flat_hash_map O(1) 查找 CommandId，通过 operator<< 流式注册，加新命令只需一行。每个 CommandId 除 handler 外还携带 arity、key 位置等元数据。
>
> 读写路径做了分离：GET 直接 Schedule 到分片线程查 DbSlice 的 hash 表，不经过 Raft。SET 走 RaftEngine::SubmitCommand，CommandEncoder 编码后单节点走 FastCommitPath（Append+Commit+Apply 三步），有 peer 时等多数派。KvStateMachine 将命令路由到对应的 DbSlice 分片。
>
> 响应通过 ReplyBuilder 按 RESP 格式编码，用 writev 零拷贝写回客户端，Pipeline 场景还能 batch 合并。
>
> 这套架构的核心优势是多线程分片无锁并发、读写分离降低 Raft 开销、零拷贝 RESP 减少延迟。

---

## 十、容易被深挖的地方

| 深挖点 | 为什么容易被追问 |
|--------|-----------------|
| 零拷贝的生命周期 | Span 引用 IoBuf 数据，IoBuf 被覆盖后数据损坏，stash 机制的时机 |
| Fiber 调度细节 | evc_.await 的唤醒机制，DispatchFiber 为什么被注释 |
| FastCommitPath 的触发条件 | PeerCount==0 是否是单机模式，单机和集群的切换 |
| 跨线程分片写入 | Await(sid, lambda) 的实现，为什么不需要锁 |
| AOF 回放的解析器 | ParseRespCommands 为什么又写了一套解析器（main_service.cc:105） |
| stash 机制的深拷贝策略 | StashState() 何时拷贝，开销有多大 |
| RedisParser vs ParseMultiBulk | 为什么有两个，关系是什么，什么时候用哪个 |
| sds 内存管理 | sds 的来源、生命周期、和 hiredis 的关系 |
| 命令元数据的用途 | arity、first_key、last_key 除了参数校验还有什么用 |
| Pipeline 响应顺序保证 | 如何保证 A→B→C 命令的响应顺序 A'→B'→C' 不乱 |

---

## 十一、作为项目作者的自我介绍（最像真实开发者）

> 这部分经历了几次迭代。最初用一个完整状态机 RedisParser，支持嵌套数组。后来发现线上 99% 都是简单 bulk 格式，就在 Connection 里加了更直接的 ParseMultiBulk，老代码保留但注释了。
>
> 命令注册选了 flat_hash_map + operator<< 流式语法，加新命令一行搞定。每个 CommandId 带 arity、key 位置等元数据，为后续 key 路由和集群模式做准备。
>
> 读写分离是最有意思的设计决策。读完全绕过 Raft，直接 Schedule 到分片线程查 hash 表，零拷贝返回。写必须走 Raft 但单节点 FastCommitPath 就是 Append + Commit + Apply 三步，无网络开销。架构上保留了分布式扩展能力，单机时性能又不打折扣。
>
> Pipeline 通过 ParsedCommand 链表解耦解析和分发，响应还能 batch 合并 writev。最 tricky 的是半包处理——multibulk_len_ 和 bulk_len_ 存 Connection 对象里，下次 DoRead 后继续，状态机不丢。

---

## 附录：关键源码速查

| 组件 | 文件 | 行号 |
|------|------|------|
| TCP 监听 | `service/dragonfly_listener.cc` | 96 |
| I/O 循环 | `service/dragonfly_connection.cc` | 175 |
| RESP 解析 | `service/dragonfly_connection.cc` | 454 |
| 半包返回 | `service/dragonfly_connection.cc` | 494/518 |
| 命令注册 | `service/main_service.cc` | 517 |
| 命令分发 | `service/main_service.cc` | 193 |
| GET 处理 | `service/main_service.cc` | 311 |
| SET 处理 | `service/main_service.cc` | 283 |
| Raft 提交 | `raft/raft_engine.cc` | 21 |
| FastCommit | `raft/raft_engine.cc` | 45 |
| 写 DbSlice | `state_machine/kv_state_machine.cc` | 49 |
| 读 DbSlice | `storage/db_slice.h` | 29 |
| ReplyBuilder | `protocol/reply_builder.cc` | 68 |
| RESP +OK | `protocol/reply_builder.cc` | 95 |
| RESP $Bulk | `protocol/reply_builder.cc` | 101 |
| CmdArgList 定义 | `storage/common_types.h` | 24 |
| CommandId 定义 | `service/command_registry.h` | 36 |
| flat_hash_map | `service/command_registry.h` | 110 |
| ParsedCommand | `protocol/conn_context.h` | 16 |
| RedisParser 类 | `protocol/redis_parser.h` | 16 |
| 状态机状态 | `protocol/redis_parser.h` | 64 |
| 零拷贝 bulk | `protocol/redis_parser.cc` | 307 |
