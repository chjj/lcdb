/*!
 * t-issue178.c - issue178 test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rdb.h>
#include "tests.h"

#define NUM_KEYS 1100000

static rdb_slice_t
test_key1(int i, char *buf) {
  int len = sprintf(buf, "my_key_%d", i);
  return rdb_slice(buf, len);
}

static rdb_slice_t
test_key2(int i, char *buf) {
  int len = sprintf(buf, "my_key_%d_xxx", i);
  return rdb_slice(buf, len);
}

/* Test for issue 178: a manual compaction causes deleted data to reappear. */
int
main(void) {
  rdb_dbopt_t options = *rdb_dbopt_default;
  char dbpath[1024], buf1[100], buf2[100];
  rdb_slice_t least, greatest;
  rdb_batch_t batch;
  rdb_iter_t *iter;
  size_t num_keys;
  rdb_t *db;
  int i;

  /* Get rid of any state from an old run. */
  ASSERT(rdb_test_filename(dbpath, sizeof(dbpath), "leveldb_cbug_test"));

  rdb_destroy_db(dbpath, 0);

  /* Open database. Disable compression since it affects the creation
     of layers and the code below is trying to test against a very
     specific scenario. */
  options.create_if_missing = 1;
  options.compression = RDB_NO_COMPRESSION;

  ASSERT(rdb_open(dbpath, &options, &db) == RDB_OK);

  /* Create first key range. */
  rdb_batch_init(&batch);

  for (i = 0; i < NUM_KEYS; i++) {
    rdb_slice_t key = test_key1(i, buf1);
    rdb_slice_t val = rdb_string("value for range 1 key");

    rdb_batch_put(&batch, &key, &val);
  }

  ASSERT(rdb_write(db, &batch, 0) == RDB_OK);

  /* Create second key range. */
  rdb_batch_reset(&batch);

  for (i = 0; i < NUM_KEYS; i++) {
    rdb_slice_t key = test_key2(i, buf1);
    rdb_slice_t val = rdb_string("value for range 2 key");

    rdb_batch_put(&batch, &key, &val);
  }

  ASSERT(rdb_write(db, &batch, 0) == RDB_OK);

  /* Delete second key range. */
  rdb_batch_reset(&batch);

  for (i = 0; i < NUM_KEYS; i++) {
    rdb_slice_t key = test_key2(i, buf1);

    rdb_batch_del(&batch, &key);
  }

  ASSERT(rdb_write(db, &batch, 0) == RDB_OK);

  rdb_batch_clear(&batch);

  /* Compact database. */
  least = test_key1(0, buf1);
  greatest = test_key1(NUM_KEYS - 1, buf2);

  /* Commenting out the line below causes the example to work correctly. */
  rdb_compact_range(db, &least, &greatest);

  /* Count the keys. */
  iter = rdb_iterator(db, 0);
  num_keys = 0;

  for (rdb_iter_seek_first(iter); rdb_iter_valid(iter); rdb_iter_next(iter))
    num_keys++;

  rdb_iter_destroy(iter);

  ASSERT(NUM_KEYS == num_keys);

  /* Close database. */
  rdb_close(db);
  rdb_destroy_db(dbpath, 0);

  return 0;
}
