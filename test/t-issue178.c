/*!
 * t-issue178.c - issue178 test for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <lcdb.h>
#include "tests.h"

#define NUM_KEYS 1100000

static ldb_slice_t
test_key1(int i, char *buf) {
  int len = sprintf(buf, "my_key_%d", i);
  return ldb_slice(buf, len);
}

static ldb_slice_t
test_key2(int i, char *buf) {
  int len = sprintf(buf, "my_key_%d_xxx", i);
  return ldb_slice(buf, len);
}

/* Test for issue 178: a manual compaction causes deleted data to reappear. */
int
main(void) {
  ldb_dbopt_t options = *ldb_dbopt_default;
  char dbpath[1024], buf1[100], buf2[100];
  ldb_slice_t least, greatest;
  ldb_batch_t batch;
  ldb_iter_t *iter;
  size_t num_keys;
  ldb_t *db;
  int i;

  /* Get rid of any state from an old run. */
  ASSERT(ldb_test_filename(dbpath, sizeof(dbpath), "leveldb_cbug_test"));

  ldb_destroy_db(dbpath, 0);

  /* Open database. Disable compression since it affects the creation
     of layers and the code below is trying to test against a very
     specific scenario. */
  options.create_if_missing = 1;
  options.compression = LDB_NO_COMPRESSION;

  ASSERT(ldb_open(dbpath, &options, &db) == LDB_OK);

  /* Create first key range. */
  ldb_batch_init(&batch);

  for (i = 0; i < NUM_KEYS; i++) {
    ldb_slice_t key = test_key1(i, buf1);
    ldb_slice_t val = ldb_string("value for range 1 key");

    ldb_batch_put(&batch, &key, &val);
  }

  ASSERT(ldb_write(db, &batch, 0) == LDB_OK);

  /* Create second key range. */
  ldb_batch_reset(&batch);

  for (i = 0; i < NUM_KEYS; i++) {
    ldb_slice_t key = test_key2(i, buf1);
    ldb_slice_t val = ldb_string("value for range 2 key");

    ldb_batch_put(&batch, &key, &val);
  }

  ASSERT(ldb_write(db, &batch, 0) == LDB_OK);

  /* Delete second key range. */
  ldb_batch_reset(&batch);

  for (i = 0; i < NUM_KEYS; i++) {
    ldb_slice_t key = test_key2(i, buf1);

    ldb_batch_del(&batch, &key);
  }

  ASSERT(ldb_write(db, &batch, 0) == LDB_OK);

  ldb_batch_clear(&batch);

  /* Compact database. */
  least = test_key1(0, buf1);
  greatest = test_key1(NUM_KEYS - 1, buf2);

  /* Commenting out the line below causes the example to work correctly. */
  ldb_compact_range(db, &least, &greatest);

  /* Count the keys. */
  iter = ldb_iterator(db, 0);
  num_keys = 0;

  for (ldb_iter_seek_first(iter); ldb_iter_valid(iter); ldb_iter_next(iter))
    num_keys++;

  ldb_iter_destroy(iter);

  ASSERT(NUM_KEYS == num_keys);

  /* Close database. */
  ldb_close(db);
  ldb_destroy_db(dbpath, 0);

  return 0;
}
