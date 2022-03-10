/*!
 * t-simple.c - database test for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <lcdb.h>
#include "tests.h"

int
main(void) {
  ldb_dbopt_t opt = *ldb_dbopt_default;
  ldb_slice_t key, val, ret;
  char path[1024];
  char kbuf[64];
  char vbuf[64];
  ldb_batch_t b;
  ldb_t *db;
  int i, rc;

  ASSERT(ldb_test_filename(path, sizeof(path), "simpledb"));

  ldb_destroy_db(path, 0);

  {
    opt.create_if_missing = 1;
    opt.error_if_exists = 1;
    opt.filter_policy = ldb_bloom_default;

    rc = ldb_open(path, &opt, &db);

    ASSERT(rc == LDB_OK);

    {
      ldb_batch_init(&b);

      for (i = 0; i < 1000000; i++) {
        sprintf(kbuf, "hello %d padding padding paddi", rand());
        sprintf(vbuf, "world %d", i);

        key = ldb_string(kbuf);
        val = ldb_string(vbuf);

        if (i > 0 && (i % 1000) == 0) {
          rc = ldb_write(db, &b, 0);

          ASSERT(rc == LDB_OK);

          ldb_batch_reset(&b);
        }

        ldb_batch_put(&b, &key, &val);

        ASSERT(rc == LDB_OK);
      }

      rc = ldb_write(db, &b, 0);

      ASSERT(rc == LDB_OK);

      ldb_batch_clear(&b);
    }

    {
      rc = ldb_get(db, &key, &ret, 0);

      ASSERT(rc == LDB_OK);
      ASSERT(ldb_compare(&ret, &val) == 0);

      ldb_free(ret.data);
    }

    {
      char *prop;

      if (ldb_get_property(db, "leveldb.stats", &prop)) {
        puts(prop);
        ldb_free(prop);
      }
    }

    {
      char *prop;

      if (ldb_get_property(db, "leveldb.sstables", &prop)) {
        puts(prop);
        ldb_free(prop);
      }
    }

    ldb_close(db);
  }

  {
    opt.create_if_missing = 0;
    opt.error_if_exists = 0;

    rc = ldb_open(path, &opt, &db);

    ASSERT(rc == LDB_OK);

    {
      ret = ldb_slice(0, 0);
      rc = ldb_get(db, &key, &ret, 0);

      ASSERT(rc == LDB_OK);
      ASSERT(ldb_compare(&ret, &val) == 0);

      ldb_free(ret.data);
    }

    {
      ldb_iter_t *it = ldb_iterator(db, 0);
      int total = 0;

      ldb_iter_seek_first(it);

      while (ldb_iter_valid(it)) {
        ldb_slice_t k = ldb_iter_key(it);
        ldb_slice_t v = ldb_iter_value(it);

        ASSERT(k.size >= 7);
        ASSERT(v.size >= 7);

        ASSERT(memcmp(k.data, "hello ", 6) == 0);
        ASSERT(memcmp(v.data, "world ", 6) == 0);

        ldb_iter_next(it);

        total++;
      }

      ASSERT(total >= 999000);
      ASSERT(ldb_iter_status(it) == LDB_OK);

      ldb_iter_destroy(it);
    }

    ldb_close(db);
  }

  ldb_destroy_db(path, 0);

  return 0;
}
