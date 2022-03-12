# Makefile.am - automake file for lcdb
# Copyright (c) 2022, Christopher Jeffrey (MIT License).
# https://github.com/chjj/lcdb

#
# Initialization
#

ACLOCAL_AMFLAGS = -I m4
AM_CPPFLAGS = -I$(top_srcdir)/include

includedir = $(prefix)/include
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

if ENABLE_TESTS
SUBDIRS = . test
else
SUBDIRS = .
endif

#
# Headers
#

include_HEADERS = include/lcdb.h include/lcdb_c.h

noinst_HEADERS = src/util/arena.h               \
                 src/util/array.h               \
                 src/util/atomic.h              \
                 src/util/bloom.h               \
                 src/util/buffer.h              \
                 src/util/cache.h               \
                 src/util/coding.h              \
                 src/util/comparator.h          \
                 src/util/crc32c.h              \
                 src/util/env.h                 \
                 src/util/env_mem_impl.h        \
                 src/util/env_unix_impl.h       \
                 src/util/env_win_impl.h        \
                 src/util/extern.h              \
                 src/util/hash.h                \
                 src/util/histogram.h           \
                 src/util/internal.h            \
                 src/util/memcmp.h              \
                 src/util/options.h             \
                 src/util/port.h                \
                 src/util/port_none_impl.h      \
                 src/util/port_unix_impl.h      \
                 src/util/port_win_impl.h       \
                 src/util/random.h              \
                 src/util/rbt.h                 \
                 src/util/slice.h               \
                 src/util/snappy_data.h         \
                 src/util/snappy.h              \
                 src/util/status.h              \
                 src/util/strutil.h             \
                 src/util/testutil.h            \
                 src/util/thread_pool.h         \
                 src/util/types.h               \
                 src/util/vector.h              \
                 src/table/block.h              \
                 src/table/block_builder.h      \
                 src/table/filter_block.h       \
                 src/table/format.h             \
                 src/table/iterator.h           \
                 src/table/iterator_wrapper.h   \
                 src/table/merger.h             \
                 src/table/table.h              \
                 src/table/table_builder.h      \
                 src/table/two_level_iterator.h \
                 src/builder.h                  \
                 src/db_impl.h                  \
                 src/db_iter.h                  \
                 src/dbformat.h                 \
                 src/dumpfile.h                 \
                 src/filename.h                 \
                 src/log_format.h               \
                 src/log_reader.h               \
                 src/log_writer.h               \
                 src/memtable.h                 \
                 src/skiplist.h                 \
                 src/snapshot.h                 \
                 src/table_cache.h              \
                 src/version_edit.h             \
                 src/version_set.h              \
                 src/write_batch.h              \
                 test/tests.h

#
# Sources
#

lcdb_sources = src/util/arena.c               \
               src/util/array.c               \
               src/util/atomic.c              \
               src/util/bloom.c               \
               src/util/buffer.c              \
               src/util/cache.c               \
               src/util/comparator.c          \
               src/util/crc32c.c              \
               src/util/env.c                 \
               src/util/hash.c                \
               src/util/internal.c            \
               src/util/logger.c              \
               src/util/options.c             \
               src/util/port.c                \
               src/util/random.c              \
               src/util/rbt.c                 \
               src/util/slice.c               \
               src/util/snappy.c              \
               src/util/status.c              \
               src/util/strutil.c             \
               src/util/thread_pool.c         \
               src/util/vector.c              \
               src/table/block.c              \
               src/table/block_builder.c      \
               src/table/filter_block.c       \
               src/table/format.c             \
               src/table/iterator.c           \
               src/table/merger.c             \
               src/table/table.c              \
               src/table/table_builder.c      \
               src/table/two_level_iterator.c \
               src/builder.c                  \
               src/c.c                        \
               src/db_impl.c                  \
               src/db_iter.c                  \
               src/dbformat.c                 \
               src/dumpfile.c                 \
               src/filename.c                 \
               src/log_reader.c               \
               src/log_writer.c               \
               src/memtable.c                 \
               src/repair.c                   \
               src/skiplist.c                 \
               src/table_cache.c              \
               src/version_edit.c             \
               src/version_set.c              \
               src/write_batch.c

test_sources = src/util/arena_test.c         \
               src/util/bloom_test.c         \
               src/util/cache_test.c         \
               src/util/coding_test.c        \
               src/util/crc32c_test.c        \
               src/util/env_test.c           \
               src/util/hash_test.c          \
               src/util/rbt_test.c           \
               src/util/snappy_test.c        \
               src/util/strutil_test.c       \
               src/util/status_test.c        \
               src/util/testutil.c           \
               src/table/filter_block_test.c \
               src/table/table_test.c        \
               src/autocompact_test.c        \
               src/c_test.c                  \
               src/corruption_test.c         \
               src/dbformat_test.c           \
               src/db_test.c                 \
               src/filename_test.c           \
               src/log_test.c                \
               src/recovery_test.c           \
               src/skiplist_test.c           \
               src/version_edit_test.c       \
               src/version_set_test.c        \
               src/write_batch_test.c

if ENABLE_TESTS
lcdb_sources += $(test_sources)
endif

#
# Targets
#

if MINGW
lcdb_ldflags = -avoid-version -Wl,--output-def,.libs/lcdb.def
else
lcdb_ldflags = -version-info @ABI_VERSION@
endif

liblcdb_la_SOURCES = $(lcdb_sources)
if ENABLE_SHARED
liblcdb_la_CFLAGS = $(WARN_FLAGS) -DLDB_EXPORT
liblcdb_la_LDFLAGS = -no-undefined $(lcdb_ldflags)
else
liblcdb_la_CFLAGS = $(WARN_FLAGS)
endif

lib_LTLIBRARIES = liblcdb.la

lcdbutil_SOURCES = src/dbutil.c
lcdbutil_CFLAGS = $(WARN_FLAGS)
lcdbutil_LDFLAGS = -static
lcdbutil_LDADD = liblcdb.la

bin_PROGRAMS = lcdbutil
