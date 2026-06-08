# 系统架构

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
      │   └─ EngineShardSet 初始化分片
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

### 4.3 分发流程

```
DispatchCommand(args, cntx)
  ├─ 命令名转大写
  ├─ registry_.Find(cmd) → CommandId
  ├─ 校验参数个数 (arity)
  ├─ cntx->cid = cid
  └─ cid->Invoke(args, cntx)
       └─ 命令对应的 handler 函数
```

## 五、存储引擎

### 5.1 分片架构

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

```
shard_set_.Await(sid, [&] {
  EngineShard::tlocal()->db_slice.AddOrFind(0, key);
});
```

### 5.2 DbSlice（数据库切片）

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
| `DbSize(db)` | 返回 key 数量 |
| `ActivateDb(db)` | 激活指定数据库 |

当前存储模型：`flat_hash_map<string, string>`，仅支持字符串值，后续可扩展为支持 Redis 数据类型。

### 5.3 StringSet（字符串集合）

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

## 六、协议解析

### 6.1 Redis RESP

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

### 6.2 Memcached ASCII

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

## 七、响应构建

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

## 八、数据流全路径

```
客户端
  │
  ▼ TCP 连接
  ▼ AcceptServer → Listener
  │                └─ NewConnection() → 分配到 proactor
  ▼
Connection::HandleRequests() (fiber)
  │
  ├─ DoRead() → IoBuf
  ├─ ParseRedis() / ParseMemcache() → ParsedCommand 链表
  ├─ DispatchCommand()
  │   ├─ registry_.Find("SET") → CommandId
  │   ├─ cid->Invoke() → Service::Set()
  │   │   └─ Shard(key) → shard_set_.Await(sid, [&] {
  │   │       EngineShard::tlocal()->db_slice.AddOrFind(key)
  │   │   })
  │   └─ cntx->SendStored()   ← 设置响应
  │
  └─ ReplyReadyCommands()
       └─ BaseSerializer::Send() → 写回 socket
```

## 九、并发模型

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

## 十、配置项

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
