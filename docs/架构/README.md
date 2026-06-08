# 架构概览

## 核心组件

- **helio/** — 高性能网络库（io_uring/epoll 事件循环、fiber 协程）
- **server/** — 服务端实现
  - `dfly_main.cc` — 主入口
  - `main_service.cc` — 命令注册与分发
  - `command_registry.cc` — COMMAND 命令实现
  - `dragonfly_connection.cc` — 连接管理
  - `redis_parser.cc` — RESP 协议解析
  - `reply_builder.cc` — 响应构建
  - `engine_shard_set.cc` — 分片存储引擎
  - `db_slice.cc` — 数据库切片
- **string_set/** — 字符串集合实现
