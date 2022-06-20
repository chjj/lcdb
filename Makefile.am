# Makefile.am - automake file for lcdb
# Copyright (c) 2022, Christopher Jeffrey (MIT License).
# https://github.com/chjj/lcdb

#
# Initialization
#

ACLOCAL_AMFLAGS = -I m4
AM_CPPFLAGS = -I$(top_srcdir)/include

licensedir = $(datadir)/licenses/$(PACKAGE_NAME)
pkgconfigdir = $(libdir)/pkgconfig

dist_doc_DATA = README.md
dist_license_DATA = LICENSE
pkgconfig_DATA = lcdb.pc

EXTRA_DIST = cmake/           \
             scripts/         \
             autogen.sh       \
             CHANGELOG.md     \
             CMakeLists.txt   \
             lcdb-cmake.pc.in

#
# Dependencies
#

SUBDIRS = . test

#
# Headers
#

include_HEADERS = include/lcdb.h include/lcdb_c.h

#
# Sources
#

lcdb_sources = src/util/arena.c               \
               src/util/arena.h               \
               src/util/array.c               \
               src/util/array.h               \
               src/util/atomic.c              \
               src/util/atomic.h              \
               src/util/bloom.c               \
               src/util/bloom.h               \
               src/util/buffer.c              \
               src/util/buffer.h              \
               src/util/cache.c               \
               src/util/cache.h               \
               src/util/coding.h              \
               src/util/comparator.c          \
               src/util/comparator.h          \
               src/util/crc32c.c              \
               src/util/crc32c.h              \
               src/util/env.c                 \
               src/util/env.h                 \
               src/util/env_mem_impl.h        \
               src/util/env_unix_impl.h       \
               src/util/env_win_impl.h        \
               src/util/extern.h              \
               src/util/hash.c                \
               src/util/hash.h                \
               src/util/internal.c            \
               src/util/internal.h            \
               src/util/logger.c              \
               src/util/options.c             \
               src/util/options.h             \
               src/util/port.c                \
               src/util/port.h                \
               src/util/port_none_impl.h      \
               src/util/port_unix_impl.h      \
               src/util/port_win_impl.h       \
               src/util/random.c              \
               src/util/random.h              \
               src/util/rbt.c                 \
               src/util/rbt.h                 \
               src/util/slice.c               \
               src/util/slice.h               \
               src/util/snappy.c              \
               src/util/snappy.h              \
               src/util/status.c              \
               src/util/status.h              \
               src/util/strutil.c             \
               src/util/strutil.h             \
               src/util/thread_pool.c         \
               src/util/thread_pool.h         \
               src/util/types.h               \
               src/util/vector.c              \
               src/util/vector.h              \
               src/table/block.c              \
               src/table/block.h              \
               src/table/block_builder.c      \
               src/table/block_builder.h      \
               src/table/filter_block.c       \
               src/table/filter_block.h       \
               src/table/format.c             \
               src/table/format.h             \
               src/table/iterator.c           \
               src/table/iterator.h           \
               src/table/iterator_wrapper.h   \
               src/table/merger.c             \
               src/table/merger.h             \
               src/table/table.c              \
               src/table/table.h              \
               src/table/table_builder.c      \
               src/table/table_builder.h      \
               src/table/two_level_iterator.c \
               src/table/two_level_iterator.h \
               src/builder.c                  \
               src/builder.h                  \
               src/c.c                        \
               src/db_impl.c                  \
               src/db_impl.h                  \
               src/db_iter.c                  \
               src/db_iter.h                  \
               src/dbformat.c                 \
               src/dbformat.h                 \
               src/dumpfile.c                 \
               src/dumpfile.h                 \
               src/filename.c                 \
               src/filename.h                 \
               src/log_format.h               \
               src/log_reader.c               \
               src/log_reader.h               \
               src/log_writer.c               \
               src/log_writer.h               \
               src/memtable.c                 \
               src/memtable.h                 \
               src/repair.c                   \
               src/skiplist.c                 \
               src/skiplist.h                 \
               src/snapshot.h                 \
               src/table_cache.c              \
               src/table_cache.h              \
               src/version_edit.c             \
               src/version_edit.h             \
               src/version_set.c              \
               src/version_set.h              \
               src/write_batch.c              \
               src/write_batch.h

#
# Targets
#

liblcdb_la_SOURCES = $(lcdb_sources)
liblcdb_la_CFLAGS =

if ENABLE_SHARED
liblcdb_la_CFLAGS += -DLDB_EXPORT
liblcdb_la_LDFLAGS = -no-undefined
if MINGW
liblcdb_la_LDFLAGS += -avoid-version -Wl,--output-def,.libs/lcdb.def
else
liblcdb_la_LDFLAGS += -version-info @ABI_VERSION@
endif
endif

lib_LTLIBRARIES = liblcdb.la

if ENABLE_LWDB
liblwdb_la_SOURCES = contrib/lwdb/lwdb.c
liblwdb_la_LDFLAGS = -static

noinst_LTLIBRARIES = liblwdb.la
endif

lcdbutil_SOURCES = src/dbutil.c
lcdbutil_LDADD = liblcdb.la

bin_PROGRAMS = lcdbutil
noinst_PROGRAMS =

#
# Benchmarks
#

bench_sources = bench/histogram.c   \
                bench/histogram.h   \
                src/util/testutil.c \
                src/util/testutil.h

if ENABLE_BENCH
db_bench_SOURCES = bench/db_bench.c $(bench_sources)
db_bench_CFLAGS = -I$(top_srcdir)/src
db_bench_LDFLAGS = -static
db_bench_LDADD = liblcdb.la
noinst_PROGRAMS += db_bench
endif

if HAVE_KCDB
db_bench_tree_db_SOURCES = bench/db_bench_tree_db.c $(bench_sources)
db_bench_tree_db_CFLAGS = -I$(top_srcdir)/src
db_bench_tree_db_LDFLAGS = -static
db_bench_tree_db_LDADD = liblcdb.la -lkyotocabinet
noinst_PROGRAMS += db_bench_tree_db
endif

if HAVE_LEVELDB
db_bench_leveldb_SOURCES = bench/db_bench_leveldb.c $(bench_sources)
db_bench_leveldb_CFLAGS = -I$(top_srcdir)/src
db_bench_leveldb_LDFLAGS = -static
db_bench_leveldb_LDADD = liblcdb.la -lleveldb
noinst_PROGRAMS += db_bench_leveldb
endif

if HAVE_LMDB
db_bench_lmdb_SOURCES = bench/db_bench_lmdb.c $(bench_sources)
db_bench_lmdb_CFLAGS = -I$(top_srcdir)/src
db_bench_lmdb_LDFLAGS = -static
db_bench_lmdb_LDADD = liblcdb.la -llmdb
noinst_PROGRAMS += db_bench_lmdb
endif

if HAVE_SQLITE3
db_bench_sqlite3_SOURCES = bench/db_bench_sqlite3.c $(bench_sources)
db_bench_sqlite3_CFLAGS = -I$(top_srcdir)/src
db_bench_sqlite3_LDFLAGS = -static
db_bench_sqlite3_LDADD = liblcdb.la -lsqlite3
noinst_PROGRAMS += db_bench_sqlite3
endif

#
# Fuzzer
#

if ENABLE_FUZZER
db_fuzz_SOURCES = $(lcdb_sources) contrib/fuzzer.c
db_fuzz_CFLAGS = -I$(top_srcdir)/src -DLDB_MEMENV \
                 -fsanitize=fuzzer,address,undefined
db_fuzz_LDFLAGS = -fsanitize=fuzzer,address,undefined

noinst_PROGRAMS += db_fuzz
endif

#
# WASI
#

if WASI
INITIAL_MEMORY = 16777216
MAX_MEMORY = 2147483648
STACK_SIZE = 5242880

lcdb_wasm_SOURCES = $(lcdb_sources)
lcdb_wasm_CFLAGS = -DLDB_EXPORT
lcdb_wasm_LDFLAGS = -mexec-model=reactor                   \
                    -Wl,--allow-undefined                  \
                    -Wl,--initial-memory=$(INITIAL_MEMORY) \
                    -Wl,--max-memory=$(MAX_MEMORY)         \
                    -Wl,-z,stack-size=$(STACK_SIZE)        \
                    -Wl,--stack-first                      \
                    -Wl,--export-dynamic                   \
                    -Wl,--export=malloc                    \
                    -Wl,--export=free

bin_PROGRAMS += lcdb.wasm
endif

#
# Emscripten
#

if EMSCRIPTEN
INITIAL_MEMORY = 16777216
MAX_MEMORY = 2147483648
STACK_SIZE = 5242880

lcdb_js_SOURCES = $(lcdb_sources)
lcdb_js_CFLAGS = -DLDB_EXPORT
lcdb_js_LDFLAGS = -s SINGLE_FILE=1                        \
                  -s ASSERTIONS=0                         \
                  -s NODERAWFS=1                          \
                  -s NODEJS_CATCH_EXIT=0                  \
                  -s NODEJS_CATCH_REJECTION=0             \
                  -s ALLOW_MEMORY_GROWTH=1                \
                  -s INITIAL_MEMORY=$(INITIAL_MEMORY)     \
                  -s MAXIMUM_MEMORY=$(MAX_MEMORY)         \
                  -s TOTAL_STACK=$(STACK_SIZE)            \
                  -s ENVIRONMENT=node                     \
                  -s EXPORTED_FUNCTIONS=@etc/exports.json

bin_PROGRAMS += lcdb.js
endif
