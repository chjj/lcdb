/*!
 * corruption_test.c - corruption test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
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
#include "util/extern.h"
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
  char dbname[RDB_PATH_MAX];
  rdb_lru_t *tiny_cache;
  rdb_dbopt_t options;
  rdb_t *db;
  char buf[100];
} ctest_t;

static int
ctest_try_reopen(ctest_t *t) {
  if (t->db != NULL)
    rdb_close(t->db);

  t->db = NULL;

  return rdb_open(t->dbname, &t->options, &t->db);
}

static void
ctest_reopen(ctest_t *t) {
  ASSERT(ctest_try_reopen(t) == RDB_OK);
}

static void
ctest_destroy(ctest_t *t) {
  char lost[RDB_PATH_MAX];

  ASSERT(rdb_join(lost, sizeof(lost), t->dbname, "lost"));

  rdb_destroy_db(lost, 0);
  rdb_destroy_db(t->dbname, 0);
}

static void
ctest_init(ctest_t *t) {
  ASSERT(rdb_test_filename(t->dbname, sizeof(t->dbname), "corruption_test"));

  t->tiny_cache = rdb_lru_create(100);
  t->options = *rdb_dbopt_default;
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
    rdb_close(t->db);

  rdb_lru_destroy(t->tiny_cache);
  ctest_destroy(t);
}

static void
ctest_repair(ctest_t *t) {
  if (t->db != NULL)
    rdb_close(t->db);

  t->db = NULL;

  ASSERT(rdb_repair_db(t->dbname, &t->options) == RDB_OK);
}

/* Return the ith key. */
static rdb_slice_t *
ctest_key(int i, rdb_buffer_t *storage) {
  char buf[100];

  sprintf(buf, "%016d", i);

  rdb_buffer_set_str(storage, buf);

  return storage;
}

/* Return the value to associate with the specified key. */
static rdb_slice_t *
ctest_val(int k, rdb_buffer_t *storage) {
  rdb_rand_t rnd;

  rdb_rand_init(&rnd, k);

  return rdb_random_string(storage, &rnd, ctest_value_size);
}

static void
ctest_build(ctest_t *t, int n) {
  rdb_writeopt_t options = *rdb_writeopt_default;
  rdb_buffer_t key_space, val_space;
  rdb_batch_t batch;
  int i;

  rdb_buffer_init(&key_space);
  rdb_buffer_init(&val_space);
  rdb_batch_init(&batch);

  for (i = 0; i < n; i++) {
    /* if ((i % 100) == 0) fprintf(stderr, "@ %d of %d\n", i, n); */
    rdb_batch_reset(&batch);

    rdb_batch_put(&batch, ctest_key(i, &key_space),
                          ctest_val(i, &val_space));

    /* Corrupt() doesn't work without this sync
       on windows; stat reports 0 for the file size. */
    if (i == n - 1)
      options.sync = 1;

    ASSERT(rdb_write(t->db, &batch, &options) == RDB_OK);
  }

  rdb_buffer_clear(&key_space);
  rdb_buffer_clear(&val_space);
  rdb_batch_clear(&batch);
}

static void
ctest_check(ctest_t *t, int min_expected, int max_expected) {
  rdb_buffer_t storage;
  int next_expected = 0;
  int missed = 0;
  int bad_keys = 0;
  int bad_values = 0;
  int correct = 0;
  rdb_iter_t *iter;
  char buf[256];

  rdb_buffer_init(&storage);

  iter = rdb_iterator(t->db, 0);

  for (rdb_iter_seek_first(iter); rdb_iter_valid(iter); rdb_iter_next(iter)) {
    const char *sp = buf;
    rdb_slice_t key = rdb_iter_key(iter);
    rdb_slice_t val;
    uint64_t k;

    ASSERT(key.size < sizeof(buf));

    memcpy(buf, key.data, key.size);

    buf[key.size] = '\0';

    if (strcmp(sp, "") == 0 || strcmp(sp, "~") == 0) {
      /* Ignore boundary keys. */
      continue;
    }

    if (!rdb_decode_int(&k, &sp) || *sp != '\0' || (int)k < next_expected) {
      bad_keys++;
      continue;
    }

    missed += (k - next_expected);
    next_expected = k + 1;

    val = rdb_iter_value(iter);

    if (!rdb_slice_equal(&val, ctest_val(k, &storage)))
      bad_values++;
    else
      correct++;
  }

  rdb_iter_destroy(iter);
  rdb_buffer_clear(&storage);

  fprintf(stderr,
    "expected=%d..%d; got=%d; bad_keys=%d; bad_values=%d; missed=%d\n",
    min_expected, max_expected, correct, bad_keys, bad_values, missed);

  ASSERT(min_expected <= correct);
  ASSERT(max_expected >= correct);
}

static void
ctest_corrupt(ctest_t *t, rdb_filetype_t target, int offset, int bytes) {
  /* Pick file to corrupt. */
  char fname[RDB_PATH_MAX];
  int picked_number = -1;
  rdb_buffer_t contents;
  rdb_filetype_t type;
  uint64_t file_size;
  char **filenames;
  uint64_t number;
  int rc = RDB_OK;
  int i, len;

  len = rdb_get_children(t->dbname, &filenames);

  ASSERT(len > 0);

  for (i = 0; i < len; i++) {
    if (!rdb_parse_filename(&type, &number, filenames[i]))
      continue;

    if (type != target)
      continue;

    ASSERT(number <= INT_MAX);

    if ((int)number <= picked_number) /* Pick latest file. */
      continue;

    ASSERT(rdb_join(fname, sizeof(fname), t->dbname, filenames[i]));

    picked_number = number;
  }

  rdb_free_children(filenames, len);

  ASSERT(picked_number != -1);
  ASSERT(rdb_get_file_size(fname, &file_size) == RDB_OK);
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
  rdb_buffer_init(&contents);

  rc = rdb_read_file(fname, &contents);

  ASSERT(rc == RDB_OK);

  for (i = 0; i < bytes; i++)
    contents.data[i + offset] ^= 0x80;

  rc = rdb_write_file(fname, &contents, 0);

  ASSERT(rc == RDB_OK);

  rdb_buffer_clear(&contents);
}

static int
ctest_files_at_level(ctest_t *t, int level) {
  int result = -1;
  char name[100];
  char *value;

  sprintf(name, "leveldb.num-files-at-level%d", level);

  ASSERT(rdb_get_property(t->db, name, &value));
  ASSERT(sscanf(value, "%d", &result) == 1);

  rdb_free(value);

  return result;
}

static int
ctest_put(ctest_t *t, const char *k, const char *v) {
  rdb_slice_t key = rdb_string(k);
  rdb_slice_t val = rdb_string(v);

  return rdb_put(t->db, &key, &val, 0);
}

static const char *
ctest_get(ctest_t *t, const char *k) {
  rdb_slice_t key = rdb_string(k);
  rdb_slice_t val;
  int rc;

  rc = rdb_get(t->db, &key, &val, 0);

  if (rc != RDB_OK)
    return rdb_strerror(rc);

  ASSERT(val.size < sizeof(t->buf));

  memcpy(t->buf, val.data, val.size);

  t->buf[val.size] = '\0';

  rdb_free(val.data);

  return t->buf;
}

/*
 * Tests
 */

static void
test_corrupt_recovery(ctest_t *t) {
  ctest_build(t, 100);
  ctest_check(t, 100, 100);
  ctest_corrupt(t, RDB_FILE_LOG, 19, 1); /* WriteBatch tag for first record. */
  ctest_corrupt(t, RDB_FILE_LOG, RDB_BLOCK_SIZE + 1000, 1); /* Somewhere in
                                                               second block. */
  ctest_reopen(t);

  /* The 64 records in the first two log blocks are completely lost. */
  ctest_check(t, 36, 36);
}

#if 0
static void
test_corrupt_recover_write_error(ctest_t *t) {
  rdb_env_writable_file_error(1);

  ASSERT(ctest_try_reopen(t) != RDB_OK);

  rdb_env_writable_file_error(0);
}

static void
test_corrupt_new_file_error_during_write(ctest_t *t) {
  /* Do enough writing to force minor compaction. */
  int num = 3 + (rdb_dbopt_default->write_buffer_size / ctest_value_size);
  rdb_buffer_t storage;
  rdb_batch_t batch;
  int rc = RDB_OK;
  int i;

  rdb_buffer_init(&storage);
  rdb_batch_init(&batch);
  rdb_env_writable_file_error(1);

  for (i = 0; rc == RDB_OK && i < num; i++) {
    rdb_slice_t key = rdb_string("a");

    rdb_batch_reset(&batch);

    rdb_batch_put(&batch, &key, ctest_val(100, &storage));

    rc = rdb_write(t->db, &batch, 0);
  }

  ASSERT(rc != RDB_OK);
  ASSERT(rdb_env_writable_file_errors() >= 1);

  rdb_env_writable_file_error(0);
  rdb_batch_clear(&batch);
  rdb_buffer_clear(&storage);

  ctest_reopen(t);
}
#endif

static void
test_corrupt_table_file(ctest_t *t) {
  ctest_build(t, 100);

  rdb_test_compact_memtable(t->db);
  rdb_test_compact_range(t->db, 0, NULL, NULL);
  rdb_test_compact_range(t->db, 1, NULL, NULL);

  ctest_corrupt(t, RDB_FILE_TABLE, 100, 1);
  ctest_check(t, 90, 99);
}

static void
test_corrupt_table_file_repair(ctest_t *t) {
  t->options.block_size = 2 * ctest_value_size; /* Limit scope of corruption. */
  t->options.paranoid_checks = 1;

  ctest_reopen(t);
  ctest_build(t, 100);

  rdb_test_compact_memtable(t->db);
  rdb_test_compact_range(t->db, 0, NULL, NULL);
  rdb_test_compact_range(t->db, 1, NULL, NULL);

  ctest_corrupt(t, RDB_FILE_TABLE, 100, 1);
  ctest_repair(t);
  ctest_reopen(t);
  ctest_check(t, 95, 99);
}

static void
test_corrupt_table_file_index_data(ctest_t *t) {
  ctest_build(t, 10000); /* Enough to build multiple tables. */

  rdb_test_compact_memtable(t->db);

  ctest_corrupt(t, RDB_FILE_TABLE, -2000, 500);
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
  ASSERT(ctest_put(t, "foo", "v1") == RDB_OK);
  ASSERT(ctest_put(t, "foo", "v2") == RDB_OK);
  ASSERT(ctest_put(t, "foo", "v3") == RDB_OK);
  ASSERT(ctest_put(t, "foo", "v4") == RDB_OK);
  ASSERT(ctest_put(t, "foo", "v5") == RDB_OK);

  ctest_repair(t);
  ctest_reopen(t);

  ASSERT(strcmp("v5", ctest_get(t, "foo")) == 0);

  /* Write something. If sequence number was not recovered
     properly, it will be hidden by an earlier write. */
  ASSERT(ctest_put(t, "foo", "v6") == RDB_OK);
  ASSERT(strcmp("v6", ctest_get(t, "foo")) == 0);

  ctest_reopen(t);

  ASSERT(strcmp("v6", ctest_get(t, "foo")) == 0);
}

static void
test_corrupt_corrupted_descriptor(ctest_t *t) {
  int rc;

  ASSERT(ctest_put(t, "foo", "hello") == RDB_OK);

  rdb_test_compact_memtable(t->db);
  rdb_test_compact_range(t->db, 0, NULL, NULL);

  ctest_corrupt(t, RDB_FILE_DESC, 0, 1000);

  rc = ctest_try_reopen(t);

  ASSERT(rc != RDB_OK);

  ctest_repair(t);
  ctest_reopen(t);

  ASSERT(strcmp("hello", ctest_get(t, "foo")) == 0);
}

static void
test_corrupt_compaction_input_error(ctest_t *t) {
  const int last = RDB_MAX_MEM_COMPACT_LEVEL;

  ctest_build(t, 10);

  rdb_test_compact_memtable(t->db);

  ASSERT(1 == ctest_files_at_level(t, last));

  ctest_corrupt(t, RDB_FILE_TABLE, 100, 1);
  ctest_check(t, 5, 9);

  /* Force compactions by writing lots of values. */
  ctest_build(t, 10000);
  ctest_check(t, 10000, 10000);
}

static void
test_corrupt_compaction_input_error_paranoid(ctest_t *t) {
  rdb_buffer_t tmp1, tmp2;
  int i, rc;

  rdb_buffer_init(&tmp1);
  rdb_buffer_init(&tmp2);

  t->options.write_buffer_size = 512 << 10;
  t->options.paranoid_checks = 1;

  ctest_reopen(t);

  /* Make multiple inputs so we need to compact. */
  for (i = 0; i < 2; i++) {
    ctest_build(t, 10);

    rdb_test_compact_memtable(t->db);

    ctest_corrupt(t, RDB_FILE_TABLE, 100, 1);

    rdb_sleep_usec(100000);
  }

  rdb_compact_range(t->db, NULL, NULL);

  /* Write must fail because of corrupted table. */
  rc = rdb_put(t->db, ctest_key(5, &tmp1), ctest_val(5, &tmp2), 0);

  ASSERT(rc != RDB_OK);

  rdb_buffer_clear(&tmp1);
  rdb_buffer_clear(&tmp2);
}

static void
test_corrupt_unrelated_keys(ctest_t *t) {
  rdb_buffer_t tmp1, tmp2;
  rdb_slice_t val;
  int rc;

  rdb_buffer_init(&tmp1);
  rdb_buffer_init(&tmp2);

  ctest_build(t, 10);

  rdb_test_compact_memtable(t->db);

  ctest_corrupt(t, RDB_FILE_TABLE, 100, 1);

  rc = rdb_put(t->db, ctest_key(1000, &tmp1),
                      ctest_val(1000, &tmp2),
                      0);

  ASSERT(rc == RDB_OK);

  rc = rdb_get(t->db, ctest_key(1000, &tmp1), &val, 0);

  ASSERT(rc == RDB_OK);
  ASSERT(rdb_slice_equal(ctest_val(1000, &tmp2), &val));

  rdb_free(val.data);

  rdb_test_compact_memtable(t->db);

  rc = rdb_get(t->db, ctest_key(1000, &tmp1), &val, 0);

  ASSERT(rc == RDB_OK);
  ASSERT(rdb_slice_equal(ctest_val(1000, &tmp2), &val));

  rdb_free(val.data);

  rdb_buffer_clear(&tmp1);
  rdb_buffer_clear(&tmp2);
}

/*
 * Execute
 */

RDB_EXTERN int
rdb_test_corruption(void);

int
rdb_test_corruption(void) {
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

  rdb_env_init();

  for (i = 0; i < lengthof(tests); i++) {
    ctest_t t;

    ctest_init(&t);

    tests[i](&t);

    ctest_clear(&t);
  }

  rdb_env_clear();

  return 0;
}
