add_library(persistence_lib aof_writer.cc persistence_manager.cc snapshot_manager.cc)

cxx_link(persistence_lib dragonfly_lib)

cxx_test(aof_writer_test persistence_lib dragonfly_lib LABELS DFLY)
cxx_test(snapshot_manager_test persistence_lib dragonfly_lib LABELS DFLY)
