/*!
 * t-issue200.c - issue200 test for lcdb
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

static int
db_put(ldb_t *db, const char *k, const char *v) {
  ldb_slice_t key = ldb_string(k);
  ldb_slice_t val = ldb_string(v);
  return ldb_put(db, &key, &val, 0);
}

static void
iter_seek(ldb_iter_t *iter, const char *k) {
  ldb_slice_t key = ldb_string(k);
  ldb_iter_seek(iter, &key);
}

static int
iter_equal(ldb_iter_t *iter, int num) {
  ldb_slice_t key = ldb_iter_key(iter);
  return key.size == 1 && ((char *)key.data)[0] == ('0' + num);
}

int
main(void) {
  ldb_dbopt_t options = *ldb_dbopt_default;
  char dbpath[1024];
  ldb_iter_t *iter;
  ldb_t *db;

  /* Get rid of any state from an old run. */
  ASSERT(ldb_test_filename(dbpath, sizeof(dbpath), "leveldb_issue200_test"));

  ldb_destroy_db(dbpath, 0);

  options.create_if_missing = 1;

  ASSERT(ldb_open(dbpath, &options, &db) == LDB_OK);

  ASSERT(db_put(db, "1", "b") == LDB_OK);
  ASSERT(db_put(db, "2", "c") == LDB_OK);
  ASSERT(db_put(db, "3", "d") == LDB_OK);
  ASSERT(db_put(db, "4", "e") == LDB_OK);
  ASSERT(db_put(db, "5", "f") == LDB_OK);

  iter = ldb_iterator(db, 0);

  /* Add an element that should not be reflected in the iterator. */
  ASSERT(db_put(db, "25", "cd") == LDB_OK);

  iter_seek(iter, "5");
  ASSERT(iter_equal(iter, 5));
  ldb_iter_prev(iter);
  ASSERT(iter_equal(iter, 4));
  ldb_iter_prev(iter);
  ASSERT(iter_equal(iter, 3));
  ldb_iter_next(iter);
  ASSERT(iter_equal(iter, 4));
  ldb_iter_next(iter);
  ASSERT(iter_equal(iter, 5));

  ldb_iter_destroy(iter);
  ldb_close(db);
  ldb_destroy_db(dbpath, 0);

  return 0;
}
