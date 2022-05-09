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

static int
ldb_rand(void) {
  static const uint32_t M = 2147483647;
  static const uint64_t A = 16807;
  static uint32_t seed = 0x5eadbeef;
  uint64_t product = seed * A;

  seed = (uint32_t)((product >> 31) + (product & M));

  if (seed > M)
    seed -= M;

  return seed;
}

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

  ldb_destroy(path, 0);

  {
    opt.create_if_missing = 1;
    opt.error_if_exists = 1;
    opt.compression = LDB_NO_COMPRESSION;
    opt.filter_policy = ldb_bloom_default;

    rc = ldb_open(path, &opt, &db);

    ASSERT(rc == LDB_OK);

    {
      ldb_batch_init(&b);

      for (i = 0; i < 1000000; i++) {
        sprintf(kbuf, "hello %d padding padding paddi", ldb_rand());
        sprintf(vbuf, "world %d", i);

        key = ldb_string(kbuf);
        val = ldb_string(vbuf);

        if (i > 0 && (i % 1000) == 0) {
          rc = ldb_write(db, &b, 0);

          ASSERT(rc == LDB_OK);

          ldb_batch_reset(&b);
        }

        ldb_batch_put(&b, &key, &val);
      }

      rc = ldb_write(db, &b, 0);

      ASSERT(rc == LDB_OK);

      ldb_batch_clear(&b);
    }

    {
      rc = ldb_get(db, &key, &ret, 0);

      ASSERT(rc == LDB_OK);
      ASSERT(ldb_compare(db, &ret, &val) == 0);

      ldb_free(ret.data);
    }

    {
      char *prop;

      if (ldb_property(db, "leveldb.stats", &prop)) {
        puts(prop);
        ldb_free(prop);
      }
    }

    {
      char *prop;

      if (ldb_property(db, "leveldb.sstables", &prop)) {
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
      ASSERT(ldb_compare(db, &ret, &val) == 0);

      ldb_free(ret.data);
    }

    {
      ldb_iter_t *it = ldb_iterator(db, 0);
      int total = 0;

      ldb_iter_each(it) {
        ldb_slice_t k = ldb_iter_key(it);
        ldb_slice_t v = ldb_iter_val(it);

        ASSERT(k.size >= 7);
        ASSERT(v.size >= 7);

        ASSERT(memcmp(k.data, "hello ", 6) == 0);
        ASSERT(memcmp(v.data, "world ", 6) == 0);

        total++;
      }

      ASSERT(total >= 999000);
      ASSERT(ldb_iter_status(it) == LDB_OK);

      ldb_iter_destroy(it);
    }

    ldb_close(db);
  }

  {
    char *path1 = path;
    char path2[1024];
    ldb_t *db1, *db2;
    ldb_iter_t *it1, *it2;
    int total = 0;

    ASSERT(ldb_test_filename(path2, sizeof(path2), "clonedb"));

    ldb_destroy(path2, 0);

    rc = ldb_open(path1, &opt, &db1);

    ASSERT(rc == LDB_OK);

    rc = ldb_backup(db1, path2);

    ASSERT(rc == LDB_OK);

    opt.create_if_missing = 0;
    opt.error_if_exists = 0;

    rc = ldb_open(path2, &opt, &db2);

    ASSERT(rc == LDB_OK);

    it1 = ldb_iterator(db1, 0);
    it2 = ldb_iterator(db2, 0);

    ldb_iter_first(it1);
    ldb_iter_first(it2);

    while (ldb_iter_valid(it1)) {
      ldb_slice_t k1, v1, k2, v2;

      ASSERT(ldb_iter_valid(it2));

      k1 = ldb_iter_key(it1);
      v1 = ldb_iter_val(it1);
      k2 = ldb_iter_key(it2);
      v2 = ldb_iter_val(it2);

      ASSERT(ldb_compare(db1, &k1, &k2) == 0);
      ASSERT(ldb_compare(db1, &v1, &v2) == 0);

      ldb_iter_next(it1);
      ldb_iter_next(it2);

      total++;
    }

    ASSERT(!ldb_iter_valid(it2));

    ASSERT(total >= 999000);
    ASSERT(ldb_iter_status(it1) == LDB_OK);
    ASSERT(ldb_iter_status(it2) == LDB_OK);

    ldb_iter_destroy(it1);
    ldb_iter_destroy(it2);

    ldb_close(db1);
    ldb_close(db2);

    ldb_destroy(path2, 0);
  }

  ldb_destroy(path, 0);

  return 0;
}
