/*!
 * autocompact_test.c - autocompact test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "table/iterator.h"

#include "util/cache.h"
#include "util/env.h"
#include "util/extern.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/testutil.h"

#include "db_impl.h"

/*
 * AutoCompactTest
 */

typedef struct actest_s {
  char dbname[1024];
  rdb_lru_t *tiny_cache;
  rdb_dbopt_t options;
  rdb_t *db;
  rdb_slice_t tmp;
} actest_t;

static void
actest_init(actest_t *t) {
  ASSERT(rdb_test_filename(t->dbname, sizeof(t->dbname), "autocompact_test"));

  t->tiny_cache = rdb_lru_create(100);

  t->options = *rdb_dbopt_default;
  t->options.block_cache = t->tiny_cache;
  t->options.create_if_missing = 1;
  t->options.compression = RDB_NO_COMPRESSION;

  t->db = NULL;

  rdb_slice_init(&t->tmp);

  rdb_destroy_db(t->dbname, &t->options);

  ASSERT(rdb_open(t->dbname, &t->options, &t->db) == RDB_OK);
}

static void
actest_clear(actest_t *t) {
  rdb_close(t->db);
  rdb_destroy_db(t->dbname, &t->options);
  rdb_lru_destroy(t->tiny_cache);
}

static rdb_slice_t
actest_key(int i, char *buf) {
  rdb_slice_t key;

  sprintf(buf, "key%06d", i);

  key = rdb_string(buf);

  return key;
}

static rdb_slice_t
actest_val(size_t len) {
  uint8_t *val = rdb_malloc(len + 1);

  memset(val, 'x', len);

  val[len] = '\0';

  return rdb_slice(val, len);
}

static uint64_t
actest_size(actest_t *t, int start, int limit) {
  char buf1[100];
  char buf2[100];
  rdb_range_t r;
  uint64_t size;

  r.start = actest_key(start, buf1);
  r.limit = actest_key(limit, buf2);

  rdb_get_approximate_sizes(t->db, &r, 1, &size);

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
  rdb_slice_t val = actest_val(AC_VALUE_SIZE);
  rdb_slice_t limit;
  int nread = 0;
  char buf[100];
  actest_t t;
  int i;

  actest_init(&t);

  /* Fill database. */
  for (i = 0; i < AC_COUNT; i++) {
    rdb_slice_t key = actest_key(i, buf);

    ASSERT(rdb_put(t.db, &key, &val, 0) == RDB_OK);
  }

  ASSERT(rdb_test_compact_memtable(t.db) == RDB_OK);

  /* Delete everything. */
  for (i = 0; i < AC_COUNT; i++) {
    rdb_slice_t key = actest_key(i, buf);

    ASSERT(rdb_del(t.db, &key, 0) == RDB_OK);
  }

  ASSERT(rdb_test_compact_memtable(t.db) == RDB_OK);

  /* Get initial measurement of the space we will be reading. */
  initial_size = actest_size(&t, 0, n);
  other_size = actest_size(&t, n, AC_COUNT);

  /* Read until size drops significantly. */
  limit = actest_key(n, buf);

  for (;;) {
    rdb_iter_t *iter;
    int64_t size;

    ASSERT(nread < 100);

    iter = rdb_iterator(t.db, 0);

    rdb_iter_seek_first(iter);

    while (rdb_iter_valid(iter)) {
      rdb_slice_t key = rdb_iter_key(iter);

      if (rdb_slice_compare(&key, &limit) >= 0)
        break;

      /* Drop data. */
      rdb_iter_next(iter);
    }

    rdb_iter_destroy(iter);

    /* Wait a little bit to allow any triggered compactions to complete. */
    rdb_sleep_usec(1000000);

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
  rdb_free(val.data);
}

/*
 * Execute
 */

RDB_EXTERN int
rdb_test_autocompact(void);

int
rdb_test_autocompact(void) {
  rdb_env_init();
  test_auto_compact_read(AC_COUNT);
  test_auto_compact_read(AC_COUNT / 2);
  rdb_env_clear();
  return 0;
}
