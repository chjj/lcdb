/*!
 * t-simple.c - database test for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 */

#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <lcdb.h>
#include "tests.h"

/*
 * Constants
 */

#define COUNT 1000000

/*
 * Helpers
 */

static unsigned int
perfect_hash(unsigned int x) {
  /* https://stackoverflow.com/a/12996028 */
  x = ((x >> 16) ^ x) * 0x45d9f3b;
  x = ((x >> 16) ^ x) * 0x45d9f3b;
  x = (x >> 16) ^ x;
  return x;
}

static ldb_slice_t
key_encode(int k, char *buffer) {
  unsigned int h = perfect_hash(k);
  int len = sprintf(buffer, "hello %u padding padding paddi", h);
  return ldb_slice(buffer, len);
}

static ldb_slice_t
val_encode(int k, char *buffer) {
  int len = sprintf(buffer, "world %d", k);
  return ldb_slice(buffer, len + 1);
}

static ldb_slice_t
key_from_val(const ldb_slice_t *val, char *buffer) {
  const char *data = val->data;
  size_t size = val->size;
  int k;

  ASSERT(size > 0);
  ASSERT(data[size - 1] == '\0');
  ASSERT(sscanf(data, "world %d", &k) == 1);

  return key_encode(k, buffer);
}

static void
compare_databases(ldb_t *db1, ldb_t *db2) {
  ldb_iter_t *it1 = ldb_iterator(db1, NULL);
  ldb_iter_t *it2 = ldb_iterator(db2, NULL);
  int total = 0;

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

  ASSERT(ldb_iter_status(it1) == LDB_OK);
  ASSERT(ldb_iter_status(it2) == LDB_OK);
  ASSERT(total == COUNT);

  ldb_iter_destroy(it1);
  ldb_iter_destroy(it2);
}

/*
 * Simple Test
 */

static void
test_simple(const char *path) {
  ldb_dbopt_t opt = *ldb_dbopt_default;
  char kbuf[64], vbuf[64];
  ldb_slice_t last;
  ldb_iter_t *it;
  ldb_batch_t b;
  char *prop;
  ldb_t *db;
  int i;

  opt.create_if_missing = 1;
  opt.error_if_exists = 1;
  opt.compression = LDB_NO_COMPRESSION;
  opt.filter_policy = ldb_bloom_default;

  ASSERT(ldb_open(path, &opt, &db) == LDB_OK);

  /* Write 1m values. */
  ldb_batch_init(&b);

  for (i = 0; i < COUNT; i++) {
    ldb_slice_t key = key_encode(i, kbuf);
    ldb_slice_t val = val_encode(i, vbuf);

    if (i > 0 && (i % 1000) == 0) {
      ASSERT(ldb_write(db, &b, NULL) == LDB_OK);

      ldb_batch_reset(&b);
    }

    ldb_batch_put(&b, &key, &val);
  }

  ASSERT(ldb_write(db, &b, NULL) == LDB_OK);

  ldb_batch_clear(&b);

  /* Read some values. */
  for (i = 0; i < 100; i++) {
    ldb_slice_t key = key_encode(i, kbuf);
    ldb_slice_t val = val_encode(i, vbuf);
    ldb_slice_t ret;

    ASSERT(ldb_get(db, &key, &ret, NULL) == LDB_OK);
    ASSERT(ldb_compare(db, &ret, &val) == 0);

    ldb_free(ret.data);
  }

  /* Print stats. */
  if (ldb_property(db, "leveldb.stats", &prop)) {
    printf("%s\n", prop);
    ldb_free(prop);
  }

  /* Print sstables. */
  if (ldb_property(db, "leveldb.sstables", &prop)) {
    printf("%s\n", prop);
    ldb_free(prop);
  }

  /* Print memusage. */
  if (ldb_property(db, "leveldb.approximate-memory-usage", &prop)) {
    unsigned long usage;

    ASSERT(sscanf(prop, "%lu", &usage) == 1);

    printf("Memory Usage: %.2f MB\n", (double)usage / 1024 / 1024);

    ldb_free(prop);
  }

  /* Close database. */
  ldb_close(db);

  /* Reopen database. */
  opt.create_if_missing = 0;
  opt.error_if_exists = 0;

  ASSERT(ldb_open(path, &opt, &db) == LDB_OK);

  /* Sequential read. */
  last = ldb_slice(vbuf, 0);
  it = ldb_iterator(db, NULL);
  i = 0;

  ldb_iter_each(it) {
    ldb_slice_t key = ldb_iter_key(it);
    ldb_slice_t val = ldb_iter_val(it);
    ldb_slice_t exp = key_from_val(&val, kbuf);

    ASSERT(ldb_compare(db, &key, &exp) == 0);
    ASSERT(ldb_compare(db, &key, &last) > 0);

    memcpy(last.data, key.data, key.size);

    last.size = key.size;

    i++;
  }

  ASSERT(ldb_iter_status(it) == LDB_OK);
  ASSERT(i == COUNT);

  ldb_iter_destroy(it);

  /* Close database. */
  ldb_close(db);
}

/*
 * Backup Test
 */

static void
test_backup(const char *path1, const char *path2) {
  ldb_dbopt_t opt = *ldb_dbopt_default;
  ldb_t *db1, *db2;

  opt.create_if_missing = 0;
  opt.error_if_exists = 0;
  opt.compression = LDB_NO_COMPRESSION;
  opt.filter_policy = ldb_bloom_default;

  /* Test backup. */
  ASSERT(ldb_open(path1, &opt, &db1) == LDB_OK);
  ASSERT(ldb_backup(db1, path2) == LDB_OK);
  ASSERT(ldb_open(path2, &opt, &db2) == LDB_OK);

  compare_databases(db1, db2);

  ldb_close(db1);
  ldb_close(db2);

  /* Cleanup. */
  ASSERT(ldb_destroy(path2, NULL) == LDB_OK);

  /* Test copy. */
  ASSERT(ldb_copy(path1, path2, &opt) == LDB_OK);
  ASSERT(ldb_open(path1, &opt, &db1) == LDB_OK);
  ASSERT(ldb_open(path2, &opt, &db2) == LDB_OK);

  compare_databases(db1, db2);

  ldb_close(db1);
  ldb_close(db2);
}

/*
 * Main
 */

int
main(void) {
  char path1[1024];
  char path2[1024];

  ASSERT(ldb_test_filename(path1, sizeof(path1), "simpledb"));
  ASSERT(ldb_test_filename(path2, sizeof(path2), "clonedb"));
  ASSERT(ldb_destroy(path1, NULL) == LDB_OK);
  ASSERT(ldb_destroy(path2, NULL) == LDB_OK);

  test_simple(path1);
  test_backup(path1, path2);

  ASSERT(ldb_destroy(path1, NULL) == LDB_OK);
  ASSERT(ldb_destroy(path2, NULL) == LDB_OK);

  return 0;
}
