/*!
 * t-issue200.c - issue200 test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rdb.h>
#include "tests.h"

static int
db_put(rdb_t *db, const char *k, const char *v) {
  rdb_slice_t key = rdb_string(k);
  rdb_slice_t val = rdb_string(v);
  return rdb_put(db, &key, &val, 0);
}

static void
iter_seek(rdb_iter_t *iter, const char *k) {
  rdb_slice_t key = rdb_string(k);
  rdb_iter_seek(iter, &key);
}

static int
iter_equal(rdb_iter_t *iter, int num) {
  rdb_slice_t key = rdb_iter_key(iter);
  return key.size == 1 && ((char *)key.data)[0] == ('0' + num);
}

int
main(void) {
  rdb_dbopt_t options = *rdb_dbopt_default;
  char dbpath[1024];
  rdb_iter_t *iter;
  rdb_t *db;

  /* Get rid of any state from an old run. */
  ASSERT(rdb_test_filename(dbpath, sizeof(dbpath), "leveldb_issue200_test"));

  rdb_destroy_db(dbpath, 0);

  options.create_if_missing = 1;

  ASSERT(rdb_open(dbpath, &options, &db) == RDB_OK);

  ASSERT(db_put(db, "1", "b") == RDB_OK);
  ASSERT(db_put(db, "2", "c") == RDB_OK);
  ASSERT(db_put(db, "3", "d") == RDB_OK);
  ASSERT(db_put(db, "4", "e") == RDB_OK);
  ASSERT(db_put(db, "5", "f") == RDB_OK);

  iter = rdb_iterator(db, 0);

  /* Add an element that should not be reflected in the iterator. */
  ASSERT(db_put(db, "25", "cd") == RDB_OK);

  iter_seek(iter, "5");
  ASSERT(iter_equal(iter, 5));
  rdb_iter_prev(iter);
  ASSERT(iter_equal(iter, 4));
  rdb_iter_prev(iter);
  ASSERT(iter_equal(iter, 3));
  rdb_iter_next(iter);
  ASSERT(iter_equal(iter, 4));
  rdb_iter_next(iter);
  ASSERT(iter_equal(iter, 5));

  rdb_iter_destroy(iter);
  rdb_close(db);
  rdb_destroy_db(dbpath, 0);

  rdb_env_clear();

  return 0;
}
