add_subdirectory(persistence)

add_executable(midi-redis service/dfly_main.cc)
cxx_link(midi-redis base dragonfly_lib persistence_lib TRDP::mimalloc2)

add_library(dragonfly_lib 
            protocol/conn_context.cc protocol/redis_parser.cc protocol/memcache_parser.cc
            protocol/resp_expr.cc protocol/reply_builder.cc
            raft/apply_progress.cc raft/command_log.cc raft/command_encoder.cc raft/election_timer.cc raft/file_log_storage.cc raft/local_transport.cc raft/manifest.cc raft/peer_manager.cc raft/raft_engine.cc raft/raft_node.cc raft/raft_storage.cc raft/segment_log_storage.cc raft/snapshot_meta.cc raft/wal_writer.cc
            service/command_registry.cc service/command_serializer.cc service/config_flags.cc
            service/debugcmd.cc service/dragonfly_connection.cc service/dragonfly_listener.cc
            service/main_service.cc service/snapshot_fiber.cc service/state_serializer.cc
            state_machine/kv_state_machine.cc
            storage/common_types.cc storage/db_slice.cc storage/engine_shard_set.cc)

cxx_link(dragonfly_lib 
         fibers2 strings_lib http_server_lib tls_lib redis_dict)

add_library(dfly_test_lib service/test_utils.cc)
cxx_link(dfly_test_lib dragonfly_lib gtest_main_ext)

cxx_test(protocol/redis_parser_test dfly_test_lib LABELS DFLY)
cxx_test(protocol/memcache_parser_test dfly_test_lib LABELS DFLY)
cxx_test(service/command_serializer_test dragonfly_lib LABELS DFLY)
cxx_test(command_log_test dragonfly_lib LABELS DFLY)
cxx_test(raft/log_storage_test dragonfly_lib LABELS DFLY)
cxx_test(raft/segment_log_storage_test dragonfly_lib LABELS DFLY)
cxx_test(raft/snapshot_meta_test dragonfly_lib LABELS DFLY)
cxx_test(raft/append_entries_rpc_test dragonfly_lib LABELS DFLY)
cxx_test(raft/election_timer_test dragonfly_lib LABELS DFLY)
cxx_test(raft/heartbeat_rpc_test dragonfly_lib LABELS DFLY)
cxx_test(raft/raft_apply_recovery_test dragonfly_lib LABELS DFLY)
cxx_test(raft/raft_engine_test dragonfly_lib LABELS DFLY)
cxx_test(raft/raft_integration_test dragonfly_lib LABELS DFLY)
cxx_test(raft/raft_role_test dragonfly_lib LABELS DFLY)
cxx_test(raft/vote_rpc_test dragonfly_lib LABELS DFLY)
cxx_test(raft_group_test dragonfly_lib LABELS DFLY)
cxx_test(raft_node_test dragonfly_lib LABELS DFLY)
cxx_test(raft_storage_test dragonfly_lib LABELS DFLY)
cxx_test(raft/wal_writer_test dragonfly_lib LABELS DFLY)
cxx_test(raft/file_log_storage_test dragonfly_lib LABELS DFLY)
cxx_test(raft_types_test dragonfly_lib LABELS DFLY)
cxx_test(state_serializer_test dragonfly_lib LABELS DFLY)
