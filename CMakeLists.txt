# CMakeLists.txt - cmake build for rdb
# Copyright (c) 2022, Christopher Jeffrey (MIT License).
# https://github.com/chjj/rdb

cmake_minimum_required(VERSION 3.4)
project(rdb LANGUAGES C)

include(CTest)
include(TestBigEndian)

set(rdb_sources)
set(rdb_defines)
set(rdb_cflags)
set(rdb_ldflags)
set(rdb_libs)

set(CMAKE_C_STANDARD 90)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_C_STANDARD_REQUIRED OFF)
set(CMAKE_C_VISIBILITY_PRESET hidden)
list(APPEND rdb_defines _GNU_SOURCE)

list(APPEND rdb_sources # util
                        src/util/arena.c
                        src/util/array.c
                        src/util/atomic.c
                        src/util/bloom.c
                        src/util/buffer.c
                        src/util/cache.c
                        src/util/comparator.c
                        src/util/crc32c.c
                        src/util/env.c
                        src/util/hash.c
                        src/util/internal.c
                        src/util/logger.c
                        src/util/options.c
                        src/util/port.c
                        src/util/random.c
                        src/util/rbt.c
                        src/util/slice.c
                        src/util/snappy.c
                        src/util/status.c
                        src/util/strutil.c
                        src/util/thread_pool.c
                        src/util/vector.c
                        # table
                        src/table/block.c
                        src/table/block_builder.c
                        src/table/filter_block.c
                        src/table/format.c
                        src/table/iterator.c
                        src/table/merger.c
                        src/table/table.c
                        src/table/table_builder.c
                        src/table/two_level_iterator.c
                        # db
                        src/builder.c
                        src/c.c
                        src/db_impl.c
                        src/db_iter.c
                        src/dbformat.c
                        src/dumpfile.c
                        src/filename.c
                        src/log_reader.c
                        src/log_writer.c
                        src/memtable.c
                        src/repair.c
                        src/skiplist.c
                        src/table_cache.c
                        src/version_edit.c
                        src/version_set.c
                        src/write_batch.c)

list(APPEND rdb_defines RDB_BUILD)
list(APPEND rdb_defines RDB_PTHREAD)

test_big_endian(RDB_BIGENDIAN)

if(RDB_BIGENDIAN)
  list(APPEND rdb_defines RDB_BIGENDIAN)
endif()

if(MSVC)
  list(APPEND rdb_cflags /wd4244 /wd4267)
else()
  list(APPEND rdb_cflags -pedantic
                         -Wall
                         -Wextra
                         -Wcast-align
                         -Wno-implicit-fallthrough
                         -Wno-long-long
                         -Wno-overlength-strings
                         -Wshadow)
endif()

add_library(rdb STATIC ${rdb_sources})
target_compile_definitions(rdb PRIVATE ${rdb_defines})
target_compile_options(rdb PUBLIC ${rdb_cflags})
target_include_directories(rdb PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_link_options(rdb INTERFACE ${rdb_ldflags})
target_link_libraries(rdb PRIVATE ${rdb_libs} pthread)

set(tests simple)

foreach(name ${tests})
  add_executable(t-${name} test/t-${name}.c)
  target_link_libraries(t-${name} PRIVATE rdb)
  add_test(NAME ${name} COMMAND t-${name})
endforeach()
