/*!
 * t-issue320.c - issue320 test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rdb.h>
#include "tests.h"

static void
batch_put(rdb_batch_t *batch, const char *k, const char *v) {
  rdb_slice_t key = rdb_string(k);
  rdb_slice_t val = rdb_string(v);
  rdb_batch_put(batch, &key, &val);
}

static void
batch_del(rdb_batch_t *batch, const char *k) {
  rdb_slice_t key = rdb_string(k);
  rdb_batch_del(batch, &key);
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
  rdb_dbopt_t options = *rdb_dbopt_default;
  const rdb_snapshot_t **snapshots;
  char **test_keys, **test_vals;
  uint32_t target_size = 10000;
  int delete_before_put = 0;
  int keep_snapshots = 1;
  uint32_t num_items = 0;
  uint32_t count = 0;
  char dbpath[1024];
  rdb_t *db;

  /* Get rid of any state from an old run. */
  ASSERT(rdb_test_filename(dbpath, sizeof(dbpath), "leveldb_issue320_test"));

  rdb_destroy_db(dbpath, 0);

  options.create_if_missing = 1;

  ASSERT(rdb_open(dbpath, &options, &db) == RDB_OK);

  snapshots = calloc(100, sizeof(rdb_snapshot_t *));
  test_keys = calloc(10000, sizeof(char *));
  test_vals = calloc(10000, sizeof(char *));

  ASSERT(snapshots != NULL);
  ASSERT(test_keys != NULL);
  ASSERT(test_vals != NULL);

  srand(0);

  while (count < 200000) {
    int index = random_number(10000);
    rdb_batch_t batch;

    rdb_batch_init(&batch);

    if ((++count % 1000) == 0)
      printf("count: %d\n", (int)count);

    if (test_keys[index] == NULL) {
      num_items++;

      test_keys[index] = random_string(index);
      test_vals[index] = random_string(index);

      batch_put(&batch, test_keys[index], test_vals[index]);
    } else {
      rdb_slice_t key = rdb_string(test_keys[index]);
      rdb_slice_t exp = rdb_string(test_vals[index]);
      rdb_slice_t val;

      ASSERT(rdb_get(db, &key, &val, 0) == RDB_OK);

      if (rdb_compare(&val, &exp) != 0) {
        printf("ERROR incorrect value returned by Get\n");
        printf("  count=%d\n", (int)count);
        printf("  test_keys[index]=%s\n", test_keys[index]);
        printf("  test_vals[index]=%s\n", test_vals[index]);
        printf("  index=%d\n", index);

        ASSERT(rdb_compare(&val, &exp) == 0);
      }

      rdb_free(val.data);

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

    ASSERT(rdb_write(db, &batch, 0) == RDB_OK);

    if (keep_snapshots && random_number(10) == 0) {
      int i = random_number(100);

      if (snapshots[i] != NULL)
        rdb_release_snapshot(db, snapshots[i]);

      snapshots[i] = rdb_get_snapshot(db);
    }

    rdb_batch_clear(&batch);
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
        rdb_release_snapshot(db, snapshots[i]);
    }

    free(test_keys);
    free(test_vals);
    free(snapshots);
  }

  rdb_close(db);
  rdb_destroy_db(dbpath, 0);

  return 0;
}
