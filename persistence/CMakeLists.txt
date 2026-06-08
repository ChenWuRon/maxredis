add_library(persistence_lib aof_writer.cc)

cxx_link(persistence_lib strings_lib)

cxx_test(aof_writer_test persistence_lib dragonfly_lib LABELS DFLY)
