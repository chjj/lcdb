# CMakeLists.txt - cmake build for lcdb
# Copyright (c) 2022, Christopher Jeffrey (MIT License).
# https://github.com/chjj/lcdb

cmake_minimum_required(VERSION 3.4)
project(lcdb LANGUAGES C)

include(CTest)

option(LDB_TESTS "Build tests" ON)

set(ldb_sources)
set(ldb_defines)
set(ldb_cflags)
set(ldb_ldflags)
set(ldb_libs)

set(CMAKE_C_STANDARD 90)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_C_STANDARD_REQUIRED OFF)
set(CMAKE_C_VISIBILITY_PRESET hidden)
list(APPEND ldb_defines _GNU_SOURCE)

list(APPEND ldb_sources # util
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

if(LDB_TESTS)
  list(APPEND ldb_sources # util
                          src/util/arena_test.c
                          src/util/bloom_test.c
                          src/util/cache_test.c
                          src/util/coding_test.c
                          src/util/crc32c_test.c
                          src/util/env_test.c
                          src/util/hash_test.c
                          src/util/rbt_test.c
                          src/util/snappy_test.c
                          src/util/strutil_test.c
                          src/util/status_test.c
                          src/util/testutil.c
                          # table
                          src/table/filter_block_test.c
                          src/table/table_test.c
                          # db
                          src/autocompact_test.c
                          src/c_test.c
                          src/corruption_test.c
                          src/dbformat_test.c
                          src/db_test.c
                          src/filename_test.c
                          src/log_test.c
                          src/recovery_test.c
                          src/skiplist_test.c
                          src/version_edit_test.c
                          src/version_set_test.c
                          src/write_batch_test.c)
endif()

list(APPEND ldb_defines LDB_BUILD)
list(APPEND ldb_defines LDB_PTHREAD)

if(MSVC)
  list(APPEND ldb_cflags /wd4244 /wd4267)
else()
  list(APPEND ldb_cflags -pedantic
                         -Wall
                         -Wextra
                         -Wcast-align
                         -Wno-implicit-fallthrough
                         -Wno-long-long
                         -Wno-overlength-strings
                         -Wshadow)
endif()

add_library(lcdb STATIC ${ldb_sources})
target_compile_definitions(lcdb PRIVATE ${ldb_defines})
target_compile_options(lcdb PUBLIC ${ldb_cflags})
target_include_directories(lcdb PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_link_options(lcdb INTERFACE ${ldb_ldflags})
target_link_libraries(lcdb PRIVATE ${ldb_libs} pthread)

add_executable(rdbutil src/dbutil.c)
target_link_libraries(rdbutil PRIVATE lcdb)

if(LDB_TESTS)
  set(tests arena
            autocompact
            bloom
            c
            cache
            coding
            corruption
            crc32c
            db
            dbformat
            env
            filename
            filter_block
            hash
            issue178
            issue200
            issue320
            log
            rbt
            recovery
            simple
            skiplist
            snappy
            status
            strutil
            table
            version_edit
            version_set
            write_batch)

  foreach(name ${tests})
    add_executable(t-${name} test/t-${name}.c)
    target_link_libraries(t-${name} PRIVATE lcdb)
    add_test(NAME ${name} COMMAND t-${name})
  endforeach()
endif()
