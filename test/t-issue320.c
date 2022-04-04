/*!
 * t-issue320.c - issue320 test for lcdb
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
#include <lcdb.h>
#include "tests.h"

static void
batch_put(ldb_batch_t *batch, const char *k, const char *v) {
  ldb_slice_t key = ldb_string(k);
  ldb_slice_t val = ldb_string(v);
  ldb_batch_put(batch, &key, &val);
}

static void
batch_del(ldb_batch_t *batch, const char *k) {
  ldb_slice_t key = ldb_string(k);
  ldb_batch_del(batch, &key);
}

static int
random_number(int max) {
  return rand() % max;
}

static char *
random_string(int index) {
  char *bytes = malloc(1024 + 1);
  size_t i = 0;

  ASSERT(bytes != NULL);

  while (i < 8) {
    bytes[i] = 'a' + ((index >> (4 * i)) & 0xf);
    ++i;
  }

  while (i < 1024) {
    bytes[i] = 'a' + random_number(26);
    ++i;
  }

  bytes[i] = '\0';

  return bytes;
}

int
main(void) {
  ldb_dbopt_t options = *ldb_dbopt_default;
  const ldb_snapshot_t **snapshots;
  char **test_keys, **test_vals;
  uint32_t target_size = 10000;
  int delete_before_put = 0;
  int keep_snapshots = 1;
  uint32_t num_items = 0;
  uint32_t count = 0;
  char dbpath[1024];
  ldb_t *db;

  /* Get rid of any state from an old run. */
  ASSERT(ldb_test_filename(dbpath, sizeof(dbpath), "leveldb_issue320_test"));

  ldb_destroy(dbpath, 0);

  options.create_if_missing = 1;

  ASSERT(ldb_open(dbpath, &options, &db) == LDB_OK);

  snapshots = calloc(100, sizeof(ldb_snapshot_t *));
  test_keys = calloc(10000, sizeof(char *));
  test_vals = calloc(10000, sizeof(char *));

  ASSERT(snapshots != NULL);
  ASSERT(test_keys != NULL);
  ASSERT(test_vals != NULL);

  srand(0);

  while (count < 200000) {
    int index = random_number(10000);
    ldb_batch_t batch;

    ldb_batch_init(&batch);

    if ((++count % 1000) == 0)
      printf("count: %d\n", (int)count);

    if (test_keys[index] == NULL) {
      num_items++;

      test_keys[index] = random_string(index);
      test_vals[index] = random_string(index);

      batch_put(&batch, test_keys[index], test_vals[index]);
    } else {
      ldb_slice_t key = ldb_string(test_keys[index]);
      ldb_slice_t exp = ldb_string(test_vals[index]);
      ldb_slice_t val;

      ASSERT(ldb_get(db, &key, &val, 0) == LDB_OK);

      if (ldb_compare(db, &val, &exp) != 0) {
        printf("ERROR incorrect value returned by Get\n");
        printf("  count=%d\n", (int)count);
        printf("  test_keys[index]=%s\n", test_keys[index]);
        printf("  test_vals[index]=%s\n", test_vals[index]);
        printf("  index=%d\n", index);

        ASSERT(ldb_compare(db, &val, &exp) == 0);
      }

      ldb_free(val.data);

      if (num_items >= target_size && random_number(100) > 30) {
        batch_del(&batch, test_keys[index]);

        free(test_keys[index]);
        free(test_vals[index]);

        test_keys[index] = NULL;
        test_vals[index] = NULL;

        --num_items;
      } else {
        free(test_vals[index]);

        test_vals[index] = random_string(index);

        if (delete_before_put)
          batch_del(&batch, test_keys[index]);

        batch_put(&batch, test_keys[index], test_vals[index]);
      }
    }

    ASSERT(ldb_write(db, &batch, 0) == LDB_OK);

    if (keep_snapshots && random_number(10) == 0) {
      int i = random_number(100);

      if (snapshots[i] != NULL)
        ldb_release(db, snapshots[i]);

      snapshots[i] = ldb_snapshot(db);
    }

    ldb_batch_clear(&batch);
  }

  {
    int i;

    for (i = 0; i < 10000; i++) {
      if (test_keys[i] != NULL) {
        free(test_keys[i]);
        free(test_vals[i]);
      }
    }

    for (i = 0; i < 100; i++) {
      if (snapshots[i] != NULL)
        ldb_release(db, snapshots[i]);
    }

    free(test_keys);
    free(test_vals);
    free(snapshots);
  }

  ldb_close(db);
  ldb_destroy(dbpath, 0);

  return 0;
}
