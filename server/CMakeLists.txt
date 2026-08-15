add_subdirectory(persistence)

add_executable(midi-redis service/dfly_main.cc)
cxx_link(midi-redis base dragonfly_lib persistence_lib TRDP::mimalloc2)

add_library(dragonfly_lib 
            protocol/conn_context.cc protocol/redis_parser.cc protocol/memcache_parser.cc
            protocol/resp_expr.cc protocol/reply_builder.cc
            raft/apply_progress.cc raft/command_log.cc raft/command_encoder.cc raft/election_timer.cc raft/file_log_storage.cc raft/local_transport.cc raft/manifest.cc raft/peer_manager.cc raft/raft_codec.cc raft/raft_engine.cc raft/raft_group.cc raft/raft_group_manager.cc raft/raft_node.cc raft/raft_storage.cc raft/segment_log_storage.cc             raft/snapshot_loader.cc raft/snapshot_manager.cc raft/snapshot_meta.cc raft/snapshot_receiver.cc raft/snapshot_sender.cc raft/snapshot_writer.cc raft/tcp_transport.cc raft/wal_writer.cc
            service/command_registry.cc service/command_serializer.cc service/config_flags.cc
            service/debugcmd.cc service/dragonfly_connection.cc service/dragonfly_listener.cc
            service/main_service.cc service/snapshot_fiber.cc service/state_serializer.cc
            state_machine/kv_state_machine.cc
            storage/common_types.cc storage/db_slice.cc storage/engine_shard_set.cc)

cxx_link(dragonfly_lib 
         fibers2 strings_lib http_server_lib tls_lib redis_dict)

add_library(dfly_test_lib test/test_utils.cc)
cxx_link(dfly_test_lib dragonfly_lib gtest_main_ext)

# ============================================
# 测试文件分层目录结构
# ============================================

# --- 协议解析 ---
cxx_test(test/protocol/redis_parser_test dfly_test_lib LABELS DFLY)
cxx_test(test/protocol/memcache_parser_test dfly_test_lib LABELS DFLY)

# --- 状态机/持久化 ---
cxx_test(test/state_persistence/command_serializer_test dragonfly_lib LABELS DFLY)
cxx_test(test/state_persistence/kv_state_machine_test dragonfly_lib LABELS DFLY)
cxx_test(test/state_persistence/state_serializer_test dragonfly_lib LABELS DFLY)
cxx_test(test/state_persistence/snapshot_manager_test persistence_lib dragonfly_lib LABELS DFLY)
cxx_test(test/state_persistence/aof_writer_test persistence_lib dragonfly_lib LABELS DFLY)

# --- Raft 核心 ---
cxx_test(test/raft_core/raft_node_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_core/raft_role_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_core/raft_engine_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_core/election_timer_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_core/raft_group_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_core/raft_storage_test dragonfly_lib LABELS DFLY)

# --- Raft RPC ---
cxx_test(test/raft_rpc/vote_rpc_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_rpc/append_entries_rpc_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_rpc/heartbeat_rpc_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_rpc/install_snapshot_rpc_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_rpc/raft_types_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_rpc/raft_codec_test dragonfly_lib LABELS DFLY)

# --- Raft 日志 ---
cxx_test(test/raft_log/command_log_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_log/log_storage_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_log/file_log_storage_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_log/segment_log_storage_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_log/wal_writer_test dragonfly_lib LABELS DFLY)

# --- Raft Snapshot ---
cxx_test(test/raft_snapshot/snapshot_meta_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_snapshot/snapshot_writer_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_snapshot/snapshot_loader_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_snapshot/snapshot_sender_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_snapshot/snapshot_receiver_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_snapshot/raft_snapshot_manager_test dragonfly_lib LABELS DFLY)

# --- Raft 集成 ---
cxx_test(test/raft_integration/raft_integration_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_integration/raft_apply_recovery_test dragonfly_lib LABELS DFLY)
cxx_test(test/raft_integration/raft_multi_group_test dragonfly_lib LABELS DFLY)

# Full-stack multi-node cluster test: spawns real midi-redis processes.
cxx_test(test/raft_integration/raft_cluster_test dragonfly_lib LABELS DFLY)
add_dependencies(raft_cluster_test midi-redis)
target_compile_definitions(raft_cluster_test PRIVATE
    MIDI_REDIS_BINARY="$<TARGET_FILE:midi-redis>")
