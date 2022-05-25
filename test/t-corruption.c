/*!
 * t-corruption.c - corruption test for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * See LICENSE for more information.
 */

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "table/iterator.h"

#include "util/buffer.h"
#include "util/cache.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/random.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/strutil.h"
#include "util/testutil.h"

#include "db_impl.h"
#include "dbformat.h"
#include "filename.h"
#include "log_format.h"
#include "write_batch.h"

/*
 * CorruptionTest
 */

static const int ctest_value_size = 1000;

typedef struct ctest_s {
  char dbname[LDB_PATH_MAX];
  ldb_lru_t *tiny_cache;
  ldb_dbopt_t options;
  ldb_t *db;
  char buf[100];
} ctest_t;

static int
ctest_try_reopen(ctest_t *t) {
  if (t->db != NULL)
    ldb_close(t->db);

  t->db = NULL;

  return ldb_open(t->dbname, &t->options, &t->db);
}

static void
ctest_reopen(ctest_t *t) {
  ASSERT(ctest_try_reopen(t) == LDB_OK);
}

static void
ctest_destroy(ctest_t *t) {
  ldb_destroy(t->dbname, 0);
}

static void
ctest_init(ctest_t *t) {
  ASSERT(ldb_test_filename(t->dbname, sizeof(t->dbname), "corruption_test"));

  t->tiny_cache = ldb_lru_create(100);
  t->options = *ldb_dbopt_default;
  t->options.block_cache = t->tiny_cache;
  t->db = NULL;

  ctest_destroy(t);

  t->options.create_if_missing = 1;

  ctest_reopen(t);

  t->options.create_if_missing = 0;
}

static void
ctest_clear(ctest_t *t) {
  if (t->db != NULL)
    ldb_close(t->db);

  ldb_lru_destroy(t->tiny_cache);
  ctest_destroy(t);
}

static void
ctest_repair(ctest_t *t) {
  if (t->db != NULL)
    ldb_close(t->db);

  t->db = NULL;

  ASSERT(ldb_repair(t->dbname, &t->options) == LDB_OK);
}

/* Return the ith key. */
static ldb_slice_t *
ctest_key(int i, ldb_buffer_t *storage) {
  char buf[100];

  sprintf(buf, "%016d", i);

  ldb_buffer_set_str(storage, buf);

  return storage;
}

/* Return the value to associate with the specified key. */
static ldb_slice_t *
ctest_val(int k, ldb_buffer_t *storage) {
  ldb_rand_t rnd;

  ldb_rand_init(&rnd, k);

  return ldb_random_string(storage, &rnd, ctest_value_size);
}

static void
ctest_build(ctest_t *t, int n) {
  ldb_writeopt_t options = *ldb_writeopt_default;
  ldb_buffer_t key_space, val_space;
  ldb_batch_t batch;
  int i;

  ldb_buffer_init(&key_space);
  ldb_buffer_init(&val_space);
  ldb_batch_init(&batch);

  for (i = 0; i < n; i++) {
    /* if ((i % 100) == 0) fprintf(stderr, "@ %d of %d\n", i, n); */
    ldb_batch_reset(&batch);

    ldb_batch_put(&batch, ctest_key(i, &key_space),
                          ctest_val(i, &val_space));

    /* corrupt() doesn't work without this sync
       on windows; stat reports 0 for the file size. */
    if (i == n - 1)
      options.sync = 1;

    ASSERT(ldb_write(t->db, &batch, &options) == LDB_OK);
  }

  ldb_buffer_clear(&key_space);
  ldb_buffer_clear(&val_space);
  ldb_batch_clear(&batch);
}

static void
ctest_check(ctest_t *t, int min_expected, int max_expected) {
  ldb_buffer_t storage;
  int next_expected = 0;
  int missed = 0;
  int bad_keys = 0;
  int bad_values = 0;
  int correct = 0;
  ldb_iter_t *iter;
  char buf[256];

  ldb_buffer_init(&storage);

  iter = ldb_iterator(t->db, ldb_readopt_default);

  for (ldb_iter_first(iter); ldb_iter_valid(iter); ldb_iter_next(iter)) {
    const char *sp = buf;
    ldb_slice_t key = ldb_iter_key(iter);
    ldb_slice_t val;
    uint64_t k;

    ASSERT(key.size < sizeof(buf));

    memcpy(buf, key.data, key.size);

    buf[key.size] = '\0';

    if (strcmp(sp, "") == 0 || strcmp(sp, "~") == 0) {
      /* Ignore boundary keys. */
      continue;
    }

    if (!ldb_decode_int(&k, &sp) || *sp != '\0' || (int)k < next_expected) {
      bad_keys++;
      continue;
    }

    missed += (k - next_expected);
    next_expected = k + 1;

    val = ldb_iter_value(iter);

    if (!ldb_slice_equal(&val, ctest_val(k, &storage)))
      bad_values++;
    else
      correct++;
  }

  ldb_iter_destroy(iter);
  ldb_buffer_clear(&storage);

  fprintf(stderr,
    "expected=%d..%d; got=%d; bad_keys=%d; bad_values=%d; missed=%d\n",
    min_expected, max_expected, correct, bad_keys, bad_values, missed);

  ASSERT(min_expected <= correct);
  ASSERT(max_expected >= correct);
}

static void
ctest_corrupt(ctest_t *t, ldb_filetype_t target, int offset, int bytes) {
  /* Pick file to corrupt. */
  char fname[LDB_PATH_MAX];
  int picked_number = -1;
  ldb_buffer_t contents;
  ldb_filetype_t type;
  uint64_t file_size;
  char **filenames;
  uint64_t number;
  int rc = LDB_OK;
  int i, len;

  len = ldb_get_children(t->dbname, &filenames);

  ASSERT(len > 0);

  for (i = 0; i < len; i++) {
    if (!ldb_parse_filename(&type, &number, filenames[i]))
      continue;

    if (type != target)
      continue;

    ASSERT(number <= INT_MAX);

    if ((int)number <= picked_number) /* Pick latest file. */
      continue;

    ASSERT(ldb_join(fname, sizeof(fname), t->dbname, filenames[i]));

    picked_number = number;
  }

  ldb_free_children(filenames, len);

  ASSERT(picked_number != -1);
  ASSERT(ldb_file_size(fname, &file_size) == LDB_OK);
  ASSERT(file_size <= (INT_MAX / 2));

  if (offset < 0) {
    /* Relative to end of file; make it absolute. */
    if (-offset > (int)file_size)
      offset = 0;
    else
      offset = file_size + offset;
  }

  if (offset > (int)file_size)
    offset = file_size;

  if (offset + bytes > (int)file_size)
    bytes = file_size - offset;

  /* Do it. */
  ldb_buffer_init(&contents);

  rc = ldb_read_file(fname, &contents);

  ASSERT(rc == LDB_OK);

  for (i = 0; i < bytes; i++)
    contents.data[i + offset] ^= 0x80;

  rc = ldb_write_file(fname, &contents, 0);

  ASSERT(rc == LDB_OK);

  ldb_buffer_clear(&contents);
}

static int
ctest_files_at_level(ctest_t *t, int level) {
  int result = -1;
  char name[100];
  char *value;

  sprintf(name, "leveldb.num-files-at-level%d", level);

  ASSERT(ldb_property(t->db, name, &value));
  ASSERT(sscanf(value, "%d", &result) == 1);

  ldb_free(value);

  return result;
}

static int
ctest_put(ctest_t *t, const char *k, const char *v) {
  ldb_slice_t key = ldb_string(k);
  ldb_slice_t val = ldb_string(v);

  return ldb_put(t->db, &key, &val, NULL);
}

static const char *
ctest_get(ctest_t *t, const char *k) {
  ldb_slice_t key = ldb_string(k);
  ldb_slice_t val;
  int rc;

  rc = ldb_get(t->db, &key, &val, NULL);

  if (rc != LDB_OK)
    return ldb_strerror(rc);

  ASSERT(val.size < sizeof(t->buf));

  memcpy(t->buf, val.data, val.size);

  t->buf[val.size] = '\0';

  ldb_free(val.data);

  return t->buf;
}

/*
 * Tests
 */

static void
test_corrupt_recovery(ctest_t *t) {
  ctest_build(t, 100);
  ctest_check(t, 100, 100);
  ctest_corrupt(t, LDB_FILE_LOG, 19, 1); /* WriteBatch tag for first record. */
  ctest_corrupt(t, LDB_FILE_LOG, LDB_BLOCK_SIZE + 1000, 1); /* Somewhere in
                                                               second block. */
  ctest_reopen(t);

  /* The 64 records in the first two log blocks are completely lost. */
  ctest_check(t, 36, 36);
}

#if 0
static void
test_corrupt_recover_write_error(ctest_t *t) {
  ldb_env_writable_file_error(1);

  ASSERT(ctest_try_reopen(t) != LDB_OK);

  ldb_env_writable_file_error(0);
}

static void
test_corrupt_new_file_error_during_write(ctest_t *t) {
  /* Do enough writing to force minor compaction. */
  int num = 3 + (ldb_dbopt_default->write_buffer_size / ctest_value_size);
  ldb_buffer_t storage;
  ldb_batch_t batch;
  int rc = LDB_OK;
  int i;

  ldb_buffer_init(&storage);
  ldb_batch_init(&batch);
  ldb_env_writable_file_error(1);

  for (i = 0; rc == LDB_OK && i < num; i++) {
    ldb_slice_t key = ldb_string("a");

    ldb_batch_reset(&batch);

    ldb_batch_put(&batch, &key, ctest_val(100, &storage));

    rc = ldb_write(t->db, &batch, NULL);
  }

  ASSERT(rc != LDB_OK);
  ASSERT(ldb_env_writable_file_errors() >= 1);

  ldb_env_writable_file_error(0);
  ldb_batch_clear(&batch);
  ldb_buffer_clear(&storage);

  ctest_reopen(t);
}
#endif

static void
test_corrupt_table_file(ctest_t *t) {
  ctest_build(t, 100);

  ldb_test_compact_memtable(t->db);
  ldb_test_compact_range(t->db, 0, NULL, NULL);
  ldb_test_compact_range(t->db, 1, NULL, NULL);

  ctest_corrupt(t, LDB_FILE_TABLE, 100, 1);
  ctest_check(t, 90, 99);
}

static void
test_corrupt_table_file_repair(ctest_t *t) {
  t->options.block_size = 2 * ctest_value_size; /* Limit scope of corruption. */
  t->options.paranoid_checks = 1;

  ctest_reopen(t);
  ctest_build(t, 100);

  ldb_test_compact_memtable(t->db);
  ldb_test_compact_range(t->db, 0, NULL, NULL);
  ldb_test_compact_range(t->db, 1, NULL, NULL);

  ctest_corrupt(t, LDB_FILE_TABLE, 100, 1);
  ctest_repair(t);
  ctest_reopen(t);
  ctest_check(t, 95, 99);
}

static void
test_corrupt_table_file_index_data(ctest_t *t) {
  ctest_build(t, 10000); /* Enough to build multiple tables. */

  ldb_test_compact_memtable(t->db);

  ctest_corrupt(t, LDB_FILE_TABLE, -2000, 500);
  ctest_reopen(t);
  ctest_check(t, 5000, 9999);
}

static void
test_corrupt_missing_descriptor(ctest_t *t) {
  ctest_build(t, 1000);
  ctest_repair(t);
  ctest_reopen(t);
  ctest_check(t, 1000, 1000);
}

static void
test_corrupt_sequence_number_recovery(ctest_t *t) {
  ASSERT(ctest_put(t, "foo", "v1") == LDB_OK);
  ASSERT(ctest_put(t, "foo", "v2") == LDB_OK);
  ASSERT(ctest_put(t, "foo", "v3") == LDB_OK);
  ASSERT(ctest_put(t, "foo", "v4") == LDB_OK);
  ASSERT(ctest_put(t, "foo", "v5") == LDB_OK);

  ctest_repair(t);
  ctest_reopen(t);

  ASSERT_EQ("v5", ctest_get(t, "foo"));

  /* Write something. If sequence number was not recovered
     properly, it will be hidden by an earlier write. */
  ASSERT(ctest_put(t, "foo", "v6") == LDB_OK);
  ASSERT_EQ("v6", ctest_get(t, "foo"));

  ctest_reopen(t);

  ASSERT_EQ("v6", ctest_get(t, "foo"));
}

static void
test_corrupt_corrupted_descriptor(ctest_t *t) {
  int rc;

  ASSERT(ctest_put(t, "foo", "hello") == LDB_OK);

  ldb_test_compact_memtable(t->db);
  ldb_test_compact_range(t->db, 0, NULL, NULL);

  ctest_corrupt(t, LDB_FILE_DESC, 0, 1000);

  rc = ctest_try_reopen(t);

  ASSERT(rc != LDB_OK);

  ctest_repair(t);
  ctest_reopen(t);

  ASSERT_EQ("hello", ctest_get(t, "foo"));
}

static void
test_corrupt_compaction_input_error(ctest_t *t) {
  const int last = LDB_MAX_MEM_COMPACT_LEVEL;

  ctest_build(t, 10);

  ldb_test_compact_memtable(t->db);

  ASSERT(1 == ctest_files_at_level(t, last));

  ctest_corrupt(t, LDB_FILE_TABLE, 100, 1);
  ctest_check(t, 5, 9);

  /* Force compactions by writing lots of values. */
  ctest_build(t, 10000);
  ctest_check(t, 10000, 10000);
}

static void
test_corrupt_compaction_input_error_paranoid(ctest_t *t) {
  ldb_buffer_t tmp1, tmp2;
  int i, rc;

  ldb_buffer_init(&tmp1);
  ldb_buffer_init(&tmp2);

  t->options.write_buffer_size = 512 << 10;
  t->options.paranoid_checks = 1;

  ctest_reopen(t);

  /* Make multiple inputs so we need to compact. */
  for (i = 0; i < 2; i++) {
    ctest_build(t, 10);

    ldb_test_compact_memtable(t->db);

    ctest_corrupt(t, LDB_FILE_TABLE, 100, 1);

    ldb_sleep_usec(100000);
  }

  ldb_compact(t->db, NULL, NULL);

  /* Write must fail because of corrupted table. */
  rc = ldb_put(t->db, ctest_key(5, &tmp1), ctest_val(5, &tmp2), NULL);

  ASSERT(rc != LDB_OK);

  ldb_buffer_clear(&tmp1);
  ldb_buffer_clear(&tmp2);
}

static void
test_corrupt_unrelated_keys(ctest_t *t) {
  ldb_buffer_t tmp1, tmp2;
  ldb_slice_t val;
  int rc;

  ldb_buffer_init(&tmp1);
  ldb_buffer_init(&tmp2);

  ctest_build(t, 10);

  ldb_test_compact_memtable(t->db);

  ctest_corrupt(t, LDB_FILE_TABLE, 100, 1);

  rc = ldb_put(t->db, ctest_key(1000, &tmp1),
                      ctest_val(1000, &tmp2),
                      NULL);

  ASSERT(rc == LDB_OK);

  rc = ldb_get(t->db, ctest_key(1000, &tmp1), &val, NULL);

  ASSERT(rc == LDB_OK);
  ASSERT(ldb_slice_equal(ctest_val(1000, &tmp2), &val));

  ldb_free(val.data);

  ldb_test_compact_memtable(t->db);

  rc = ldb_get(t->db, ctest_key(1000, &tmp1), &val, NULL);

  ASSERT(rc == LDB_OK);
  ASSERT(ldb_slice_equal(ctest_val(1000, &tmp2), &val));

  ldb_free(val.data);

  ldb_buffer_clear(&tmp1);
  ldb_buffer_clear(&tmp2);
}

/*
 * Execute
 */

int
main(void) {
  static void (*tests[])(ctest_t *) = {
    test_corrupt_recovery,
#if 0
    test_corrupt_recover_write_error,
    test_corrupt_new_file_error_during_write,
#endif
    test_corrupt_table_file,
    test_corrupt_table_file_repair,
    test_corrupt_table_file_index_data,
    test_corrupt_missing_descriptor,
    test_corrupt_sequence_number_recovery,
    test_corrupt_corrupted_descriptor,
    test_corrupt_compaction_input_error,
    test_corrupt_compaction_input_error_paranoid,
    test_corrupt_unrelated_keys
  };

  size_t i;

  for (i = 0; i < lengthof(tests); i++) {
    ctest_t t;

    ctest_init(&t);

    tests[i](&t);

    ctest_clear(&t);
  }

  return 0;
}
