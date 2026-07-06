add_library(persistence_lib aof_writer.cc persistence_manager.cc snapshot_manager.cc)

cxx_link(persistence_lib dragonfly_lib)

# Tests moved to server/test/state_persistence/
