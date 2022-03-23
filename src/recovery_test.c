/*!
 * recovery_test.c - recovery test for lcdb
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

#include "util/array.h"
#include "util/buffer.h"
#include "util/env.h"
#include "util/extern.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/strutil.h"
#include "util/testutil.h"

#include "db_impl.h"
#include "dbformat.h"
#include "filename.h"
#include "log_format.h"
#include "log_reader.h"
#include "log_writer.h"
#include "write_batch.h"

/*
 * RecoveryTest
 */

typedef struct rtest_s {
  char dbname[LDB_PATH_MAX];
  char buf[256];
  ldb_t *db;
} rtest_t;

static void
rtest_close(rtest_t *t) {
  if (t->db != NULL)
    ldb_close(t->db);

  t->db = NULL;
}

static int
rtest_open_with_status(rtest_t *t, const ldb_dbopt_t *options) {
  ldb_dbopt_t opts;

  rtest_close(t);

  if (options != NULL) {
    opts = *options;
  } else {
    opts = *ldb_dbopt_default;
    opts.reuse_logs = 1; /* TODO: test both ways */
    opts.create_if_missing = 1;
  }

  return ldb_open(t->dbname, &opts, &t->db);
}

static int
rtest_num_logs(rtest_t *t);

static void
rtest_open(rtest_t *t, const ldb_dbopt_t *options) {
  ASSERT(rtest_open_with_status(t, options) == LDB_OK);
  ASSERT(1 == rtest_num_logs(t));
}

static void
rtest_init(rtest_t *t) {
  ASSERT(ldb_test_filename(t->dbname, sizeof(t->dbname), "recovery_test"));

  t->db = NULL;

  ldb_destroy(t->dbname, 0);

  rtest_open(t, 0);
}

static void
rtest_clear(rtest_t *t) {
  rtest_close(t);
  ldb_destroy(t->dbname, 0);
}

static int
rtest_can_append(rtest_t *t) {
  char fname[LDB_PATH_MAX];
  ldb_wfile_t *tmp;
  int rc;

  ASSERT(ldb_current_filename(fname, sizeof(fname), t->dbname));

  rc = ldb_appendfile_create(fname, &tmp);

  if (rc == LDB_NOSUPPORT)
    return 0;

  ldb_wfile_destroy(tmp);

  return 1;
}

static int
rtest_put(rtest_t *t, const char *k, const char *v) {
  ldb_slice_t key = ldb_string(k);
  ldb_slice_t val = ldb_string(v);

  return ldb_put(t->db, &key, &val, 0);
}

static const char *
rtest_get(rtest_t *t, const char *k) {
  ldb_slice_t key = ldb_string(k);
  ldb_slice_t val;
  int rc;

  rc = ldb_get(t->db, &key, &val, 0);

  if (rc == LDB_NOTFOUND)
    return "NOT_FOUND";

  if (rc != LDB_OK)
    return ldb_strerror(rc);

  ASSERT(val.size < sizeof(t->buf));

  memcpy(t->buf, val.data, val.size);

  t->buf[val.size] = '\0';

  ldb_free(val.data);

  return t->buf;
}

static void
rtest_manifest_filename(rtest_t *t, char *buf, size_t size) {
  char fname[LDB_PATH_MAX];
  ldb_buffer_t current;

  ldb_buffer_init(&current);

  ASSERT(ldb_current_filename(fname, sizeof(fname), t->dbname));
  ASSERT(ldb_read_file(fname, &current) == LDB_OK);
  ASSERT(current.size > 0 && current.data[current.size - 1] == '\n');
  ASSERT(current.size < sizeof(fname));

  memcpy(fname, current.data, current.size - 1);

  fname[current.size - 1] = '\0';

  ASSERT(ldb_join(buf, size, t->dbname, fname));

  ldb_buffer_clear(&current);
}

static void
rtest_log_name(rtest_t *t, char *buf, size_t size, uint64_t number) {
  ASSERT(ldb_log_filename(buf, size, t->dbname, number));
}

static size_t
rtest_get_files(rtest_t *t, ldb_array_t *result, ldb_filetype_t target) {
  char **names;
  int i, len;

  ldb_array_reset(result);

  len = ldb_get_children(t->dbname, &names);

  ASSERT(len >= 0);

  for (i = 0; i < len; i++) {
    ldb_filetype_t type;
    uint64_t number;

    if (!ldb_parse_filename(&type, &number, names[i]))
      continue;

    if (type != target)
      continue;

    ldb_array_push(result, number);
  }

  ldb_free_children(names, len);

  return result->length;
}

static size_t
rtest_remove_log_files(rtest_t *t) {
  /* Linux allows unlinking open files, but Windows does not. */
  /* Closing the db allows for file deletion. */
  char fname[LDB_PATH_MAX];
  ldb_array_t files;
  size_t i, len;

  ldb_array_init(&files);

  rtest_close(t);

  len = rtest_get_files(t, &files, LDB_FILE_LOG);

  for (i = 0; i < files.length; i++) {
    rtest_log_name(t, fname, sizeof(fname), files.items[i]);

    ASSERT(ldb_remove_file(fname) == LDB_OK);
  }

  ldb_array_clear(&files);

  return len;
}

static void
rtest_remove_manifest(rtest_t *t) {
  char fname[LDB_PATH_MAX];

  rtest_manifest_filename(t, fname, sizeof(fname));

  ASSERT(ldb_remove_file(fname) == LDB_OK);
}

static uint64_t
rtest_first_log(rtest_t *t) {
  ldb_array_t files;
  uint64_t number;

  ldb_array_init(&files);

  ASSERT(rtest_get_files(t, &files, LDB_FILE_LOG) > 0);

  number = files.items[0];

  ldb_array_clear(&files);

  return number;
}

static int
rtest_num_logs(rtest_t *t) {
  ldb_array_t files;
  int len;

  ldb_array_init(&files);

  len = rtest_get_files(t, &files, LDB_FILE_LOG);

  ldb_array_clear(&files);

  return len;
}

static int
rtest_num_tables(rtest_t *t) {
  ldb_array_t files;
  int len;

  ldb_array_init(&files);

  len = rtest_get_files(t, &files, LDB_FILE_TABLE);

  ldb_array_clear(&files);

  return len;
}

static uint64_t
get_file_size(const char *fname) {
  uint64_t result;
  ASSERT(ldb_get_file_size(fname, &result) == LDB_OK);
  return result;
}

static void
rtest_compact_memtable(rtest_t *t) {
  ldb_test_compact_memtable(t->db);
}

/* Directly construct a log file that sets key to val. */
static void
rtest_make_log(rtest_t *t,
               uint64_t lognum,
               ldb_seqnum_t seq,
               const char *key,
               const char *val) {
  ldb_slice_t k = ldb_string(key);
  ldb_slice_t v = ldb_string(val);
  char fname[LDB_PATH_MAX];
  ldb_logwriter_t writer;
  ldb_slice_t contents;
  ldb_wfile_t *file;
  ldb_batch_t batch;

  ldb_batch_init(&batch);

  ASSERT(ldb_log_filename(fname, sizeof(fname), t->dbname, lognum));
  ASSERT(ldb_truncfile_create(fname, &file) == LDB_OK);

  ldb_logwriter_init(&writer, file, 0);

  ldb_batch_put(&batch, &k, &v);
  ldb_batch_set_sequence(&batch, seq);

  contents = ldb_batch_contents(&batch);

  ASSERT(ldb_logwriter_add_record(&writer, &contents) == 0);
  ASSERT(ldb_wfile_flush(file) == LDB_OK);

  ldb_wfile_destroy(file);
  ldb_batch_clear(&batch);
}

/*
 * Tests
 */

static void
test_recovery_manifest_reused(rtest_t *t) {
  char old[LDB_PATH_MAX];
  char cur[LDB_PATH_MAX];

  if (!rtest_can_append(t)) {
    fprintf(stderr, "skipping test because env does not support appending\n");
    return;
  }

  ASSERT(rtest_put(t, "foo", "bar") == LDB_OK);

  rtest_close(t);

  rtest_manifest_filename(t, old, sizeof(old));

  rtest_open(t, 0);

  rtest_manifest_filename(t, cur, sizeof(cur));

  ASSERT_EQ(old, cur);
  ASSERT_EQ("bar", rtest_get(t, "foo"));

  rtest_open(t, 0);

  rtest_manifest_filename(t, cur, sizeof(cur));

  ASSERT_EQ(old, cur);
  ASSERT_EQ("bar", rtest_get(t, "foo"));
}

static void
test_recovery_large_manifest_compacted(rtest_t *t) {
  char old[LDB_PATH_MAX];
  char cur[LDB_PATH_MAX];

  if (!rtest_can_append(t)) {
    fprintf(stderr, "skipping test because env does not support appending\n");
    return;
  }

  ASSERT(rtest_put(t, "foo", "bar") == LDB_OK);

  rtest_close(t);

  rtest_manifest_filename(t, old, sizeof(old));

  /* Pad with zeroes to make manifest file very big. */
  {
    uint64_t len = get_file_size(old);
    ldb_buffer_t zeroes;
    ldb_wfile_t *file;

    ASSERT(ldb_appendfile_create(old, &file) == LDB_OK);

    ldb_buffer_init(&zeroes);
    ldb_buffer_pad(&zeroes, 3 * 1048576 - len);

    ASSERT(ldb_wfile_append(file, &zeroes) == LDB_OK);
    ASSERT(ldb_wfile_flush(file) == LDB_OK);

    ldb_buffer_clear(&zeroes);
    ldb_wfile_destroy(file);
  }

  rtest_open(t, 0);

  rtest_manifest_filename(t, cur, sizeof(cur));

  ASSERT_NE(old, cur);
  ASSERT(10000 > get_file_size(cur));
  ASSERT_EQ("bar", rtest_get(t, "foo"));

  strcpy(old, cur);

  rtest_open(t, 0);

  rtest_manifest_filename(t, cur, sizeof(cur));

  ASSERT_EQ(old, cur);
  ASSERT_EQ("bar", rtest_get(t, "foo"));
}

static void
test_recovery_no_log_files(rtest_t *t) {
  ASSERT(rtest_put(t, "foo", "bar") == LDB_OK);
  ASSERT(1 == rtest_remove_log_files(t));

  rtest_open(t, 0);

  ASSERT_EQ("NOT_FOUND", rtest_get(t, "foo"));

  rtest_open(t, 0);

  ASSERT_EQ("NOT_FOUND", rtest_get(t, "foo"));
}

static void
test_recovery_log_file_reuse(rtest_t *t) {
  char fname[LDB_PATH_MAX];
  uint64_t number;
  int i;

  if (!rtest_can_append(t)) {
    fprintf(stderr, "skipping test because env does not support appending\n");
    return;
  }

  for (i = 0; i < 2; i++) {
    ASSERT(rtest_put(t, "foo", "bar") == LDB_OK);

    if (i == 0) {
      /* Compact to ensure current log is empty. */
      rtest_compact_memtable(t);
    }

    rtest_close(t);

    ASSERT(1 == rtest_num_logs(t));

    number = rtest_first_log(t);

    rtest_log_name(t, fname, sizeof(fname), number);

    if (i == 0)
      ASSERT(0 == get_file_size(fname));
    else
      ASSERT(0 < get_file_size(fname));

    rtest_open(t, 0);

    ASSERT(1 == rtest_num_logs(t));
    ASSERT(number == rtest_first_log(t));
    ASSERT_EQ("bar", rtest_get(t, "foo"));

    rtest_open(t, 0);

    ASSERT(1 == rtest_num_logs(t));
    ASSERT(number == rtest_first_log(t));
    ASSERT_EQ("bar", rtest_get(t, "foo"));
  }
}

static void
test_recovery_multiple_memtables(rtest_t *t) {
  ldb_dbopt_t opt = *ldb_dbopt_default;
  const int num = 1000;
  uint64_t old_log;
  int i;

  /* Make a large log. */
  for (i = 0; i < num; i++) {
    char buf[100];

    sprintf(buf, "%050d", i);

    ASSERT(rtest_put(t, buf, buf) == LDB_OK);
  }

  ASSERT(0 == rtest_num_tables(t));

  rtest_close(t);

  ASSERT(0 == rtest_num_tables(t));
  ASSERT(1 == rtest_num_logs(t));

  old_log = rtest_first_log(t);

  /* Force creation of multiple memtables
     by reducing the write buffer size. */
  opt.reuse_logs = 1;
  opt.write_buffer_size = (num * 100) / 2;

  rtest_open(t, &opt);

  ASSERT(2 <= rtest_num_tables(t));
  ASSERT(1 == rtest_num_logs(t));
  ASSERT(old_log != rtest_first_log(t));

  for (i = 0; i < num; i++) {
    char buf[100];

    sprintf(buf, "%050d", i);

    ASSERT_EQ(buf, rtest_get(t, buf));
  }
}

static void
test_recovery_multiple_log_files(rtest_t *t) {
  uint64_t old_log, new_log;

  ASSERT(rtest_put(t, "foo", "bar") == LDB_OK);

  rtest_close(t);

  ASSERT(1 == rtest_num_logs(t));

  /* Make a bunch of uncompacted log files. */
  old_log = rtest_first_log(t);

  rtest_make_log(t, old_log + 1, 1000, "hello", "world");
  rtest_make_log(t, old_log + 2, 1001, "hi", "there");
  rtest_make_log(t, old_log + 3, 1002, "foo", "bar2");

  /* Recover and check that all log files were processed. */
  rtest_open(t, 0);

  ASSERT(1 <= rtest_num_tables(t));
  ASSERT(1 == rtest_num_logs(t));

  new_log = rtest_first_log(t);

  ASSERT(old_log + 3 <= new_log);
  ASSERT_EQ("bar2", rtest_get(t, "foo"));
  ASSERT_EQ("world", rtest_get(t, "hello"));
  ASSERT_EQ("there", rtest_get(t, "hi"));

  /* Test that previous recovery produced recoverable state. */
  rtest_open(t, 0);

  ASSERT(1 <= rtest_num_tables(t));
  ASSERT(1 == rtest_num_logs(t));

  if (rtest_can_append(t))
    ASSERT(new_log == rtest_first_log(t));

  ASSERT_EQ("bar2", rtest_get(t, "foo"));
  ASSERT_EQ("world", rtest_get(t, "hello"));
  ASSERT_EQ("there", rtest_get(t, "hi"));

  /* Check that introducing an older log
     file does not cause it to be re-read. */
  rtest_close(t);

  rtest_make_log(t, old_log + 1, 2000, "hello", "stale write");

  rtest_open(t, 0);

  ASSERT(1 <= rtest_num_tables(t));
  ASSERT(1 == rtest_num_logs(t));

  if (rtest_can_append(t))
    ASSERT(new_log == rtest_first_log(t));

  ASSERT_EQ("bar2", rtest_get(t, "foo"));
  ASSERT_EQ("world", rtest_get(t, "hello"));
  ASSERT_EQ("there", rtest_get(t, "hi"));
}

static void
test_recovery_manifest_missing(rtest_t *t) {
  int rc;

  ASSERT(rtest_put(t, "foo", "bar") == LDB_OK);

  rtest_close(t);
  rtest_remove_manifest(t);

  rc = rtest_open_with_status(t, 0);

  ASSERT(rc == LDB_CORRUPTION);
}

/*
 * Execute
 */

LDB_EXTERN int
ldb_test_recovery(void);

int
ldb_test_recovery(void) {
  static void (*tests[])(rtest_t *) = {
    test_recovery_manifest_reused,
    test_recovery_large_manifest_compacted,
    test_recovery_no_log_files,
    test_recovery_log_file_reuse,
    test_recovery_multiple_memtables,
    test_recovery_multiple_log_files,
    test_recovery_manifest_missing
  };

  size_t i;

  for (i = 0; i < lengthof(tests); i++) {
    rtest_t t;

    rtest_init(&t);

    tests[i](&t);

    rtest_clear(&t);
  }

  return 0;
}
