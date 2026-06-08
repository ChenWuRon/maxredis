add_library(persistence_lib aof_writer.cc persistence_manager.cc)

cxx_link(persistence_lib dragonfly_lib)

cxx_test(aof_writer_test persistence_lib dragonfly_lib LABELS DFLY)
