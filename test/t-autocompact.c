/*!
 * t-autocompact.c - autocompact test for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * See LICENSE for more information.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "table/iterator.h"

#include "util/cache.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/testutil.h"

#include "db_impl.h"
#include "db_iter.h"

/*
 * AutoCompactTest
 */

typedef struct actest_s {
  char dbname[1024];
  ldb_lru_t *tiny_cache;
  ldb_dbopt_t options;
  ldb_t *db;
  ldb_slice_t tmp;
} actest_t;

static void
actest_init(actest_t *t) {
  ASSERT(ldb_test_filename(t->dbname, sizeof(t->dbname), "autocompact_test"));

  t->tiny_cache = ldb_lru_create(100);

  t->options = *ldb_dbopt_default;
  t->options.block_cache = t->tiny_cache;
  t->options.create_if_missing = 1;
  t->options.compression = LDB_NO_COMPRESSION;

  t->db = NULL;

  ldb_slice_init(&t->tmp);

  ldb_destroy(t->dbname, &t->options);

  ASSERT(ldb_open(t->dbname, &t->options, &t->db) == LDB_OK);
}

static void
actest_clear(actest_t *t) {
  ldb_close(t->db);
  ldb_destroy(t->dbname, &t->options);
  ldb_lru_destroy(t->tiny_cache);
}

static ldb_slice_t
actest_key(int i, char *buf) {
  ldb_slice_t key;

  sprintf(buf, "key%06d", i);

  key = ldb_string(buf);

  return key;
}

static ldb_slice_t
actest_val(size_t len) {
  uint8_t *val = ldb_malloc(len + 1);

  memset(val, 'x', len);

  val[len] = '\0';

  return ldb_slice(val, len);
}

static int64_t
actest_size(actest_t *t, int start, int limit) {
  char buf1[100];
  char buf2[100];
  ldb_range_t r;
  uint64_t size;

  r.start = actest_key(start, buf1);
  r.limit = actest_key(limit, buf2);

  ldb_approximate_sizes(t->db, &r, 1, &size);

  return size;
}

/*
 * AutoCompactTest::DoReads
 */

#define AC_VALUE_SIZE (200 * 1024)
#define AC_TOTAL_SIZE (100 * 1024 * 1024)
#define AC_COUNT (AC_TOTAL_SIZE / AC_VALUE_SIZE)

/* Read through the first n keys repeatedly and check that they get
   compacted (verified by checking the size of the key space). */
static void
test_auto_compact_read(int n) {
  int64_t initial_size, other_size, final_size;
  ldb_slice_t val = actest_val(AC_VALUE_SIZE);
  ldb_slice_t limit;
  int nread = 0;
  char buf[100];
  actest_t t;
  int i;

  actest_init(&t);

  /* Fill database. */
  for (i = 0; i < AC_COUNT; i++) {
    ldb_slice_t key = actest_key(i, buf);

    ASSERT(ldb_put(t.db, &key, &val, NULL) == LDB_OK);
  }

  ASSERT(ldb_test_compact_memtable(t.db) == LDB_OK);

  /* Delete everything. */
  for (i = 0; i < AC_COUNT; i++) {
    ldb_slice_t key = actest_key(i, buf);

    ASSERT(ldb_del(t.db, &key, NULL) == LDB_OK);
  }

  ASSERT(ldb_test_compact_memtable(t.db) == LDB_OK);

  /* Get initial measurement of the space we will be reading. */
  initial_size = actest_size(&t, 0, n);
  other_size = actest_size(&t, n, AC_COUNT);

  /* Read until size drops significantly. */
  limit = actest_key(n, buf);

  for (;;) {
    ldb_iter_t *iter;
    int64_t size;

    ASSERT(nread < 100);

    iter = ldb_iterator(t.db, ldb_readopt_default);

    ldb_iter_first(iter);

    while (ldb_iter_valid(iter)) {
      if (ldb_iter_compare(iter, &limit) >= 0)
        break;

      /* Drop data. */
      ldb_iter_next(iter);
    }

    ldb_iter_destroy(iter);

    /* Wait a little bit to allow any triggered compactions to complete. */
    ldb_sleep_usec(1000000);

    size = actest_size(&t, 0, n);

    fprintf(stderr, "iter %3d => %7.3f MB [other %7.3f MB]\n", nread + 1,
                    size / 1048576.0, actest_size(&t, n, AC_COUNT) / 1048576.0);

    if (size <= initial_size / 10)
      break;

    nread++;
  }

  /* Verify that the size of the key space not touched
     by the reads is pretty much unchanged. */
  final_size = actest_size(&t, n, AC_COUNT);

  ASSERT(final_size <= other_size + 1048576);
  ASSERT(final_size >= other_size / 5 - 1048576);

  actest_clear(&t);
  ldb_free(val.data);
}

/*
 * Execute
 */

int
main(void) {
  test_auto_compact_read(AC_COUNT);
  test_auto_compact_read(AC_COUNT / 2);
  return 0;
}
