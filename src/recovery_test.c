/*!
 * recovery_test.c - recovery test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
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
  char dbname[RDB_PATH_MAX];
  char buf[256];
  rdb_t *db;
} rtest_t;

static void
rtest_close(rtest_t *t) {
  if (t->db != NULL)
    rdb_close(t->db);

  t->db = NULL;
}

static int
rtest_open_with_status(rtest_t *t, const rdb_dbopt_t *options) {
  rdb_dbopt_t opts;

  rtest_close(t);

  if (options != NULL) {
    opts = *options;
  } else {
    opts = *rdb_dbopt_default;
    opts.reuse_logs = 1; /* TODO: test both ways */
    opts.create_if_missing = 1;
  }

  return rdb_open(t->dbname, &opts, &t->db);
}

static int
rtest_num_logs(rtest_t *t);

static void
rtest_open(rtest_t *t, const rdb_dbopt_t *options) {
  ASSERT(rtest_open_with_status(t, options) == RDB_OK);
  ASSERT(1 == rtest_num_logs(t));
}

static void
rtest_init(rtest_t *t) {
  ASSERT(rdb_test_filename(t->dbname, sizeof(t->dbname), "recovery_test"));

  t->db = NULL;

  rdb_destroy_db(t->dbname, 0);

  rtest_open(t, 0);
}

static void
rtest_clear(rtest_t *t) {
  rtest_close(t);
  rdb_destroy_db(t->dbname, 0);
}

static int
rtest_can_append(rtest_t *t) {
  char fname[RDB_PATH_MAX];
  rdb_wfile_t *tmp;
  int rc;

  ASSERT(rdb_current_filename(fname, sizeof(fname), t->dbname));

  rc = rdb_appendfile_create(fname, &tmp);

  if (rc == RDB_NOSUPPORT)
    return 0;

  rdb_wfile_destroy(tmp);

  return 1;
}

static int
rtest_put(rtest_t *t, const char *k, const char *v) {
  rdb_slice_t key = rdb_string(k);
  rdb_slice_t val = rdb_string(v);

  return rdb_put(t->db, &key, &val, 0);
}

static const char *
rtest_get(rtest_t *t, const char *k) {
  rdb_slice_t key = rdb_string(k);
  rdb_slice_t val;
  int rc;

  rc = rdb_get(t->db, &key, &val, 0);

  if (rc == RDB_NOTFOUND)
    return "NOT_FOUND";

  if (rc != RDB_OK)
    return rdb_strerror(rc);

  ASSERT(val.size < sizeof(t->buf));

  memcpy(t->buf, val.data, val.size);

  t->buf[val.size] = '\0';

  rdb_free(val.data);

  return t->buf;
}

static void
rtest_manifest_filename(rtest_t *t, char *buf, size_t size) {
  char fname[RDB_PATH_MAX];
  rdb_buffer_t current;

  rdb_buffer_init(&current);

  ASSERT(rdb_current_filename(fname, sizeof(fname), t->dbname));
  ASSERT(rdb_read_file(fname, &current) == RDB_OK);
  ASSERT(current.size > 0 && current.data[current.size - 1] == '\n');
  ASSERT(current.size < sizeof(fname));

  memcpy(fname, current.data, current.size - 1);

  fname[current.size - 1] = '\0';

  ASSERT(rdb_join(buf, size, t->dbname, fname));

  rdb_buffer_clear(&current);
}

static void
rtest_log_name(rtest_t *t, char *buf, size_t size, uint64_t number) {
  ASSERT(rdb_log_filename(buf, size, t->dbname, number));
}

static size_t
rtest_get_files(rtest_t *t, rdb_array_t *result, rdb_filetype_t target) {
  char **names;
  int i, len;

  rdb_array_reset(result);

  len = rdb_get_children(t->dbname, &names);

  ASSERT(len >= 0);

  for (i = 0; i < len; i++) {
    rdb_filetype_t type;
    uint64_t number;

    if (!rdb_parse_filename(&type, &number, names[i]))
      continue;

    if (type != target)
      continue;

    rdb_array_push(result, number);
  }

  rdb_free_children(names, len);

  return result->length;
}

static size_t
rtest_remove_log_files(rtest_t *t) {
  /* Linux allows unlinking open files, but Windows does not. */
  /* Closing the db allows for file deletion. */
  char fname[RDB_PATH_MAX];
  rdb_array_t files;
  size_t i, len;

  rdb_array_init(&files);

  rtest_close(t);

  len = rtest_get_files(t, &files, RDB_FILE_LOG);

  for (i = 0; i < files.length; i++) {
    rtest_log_name(t, fname, sizeof(fname), files.items[i]);

    ASSERT(rdb_remove_file(fname) == RDB_OK);
  }

  rdb_array_clear(&files);

  return len;
}

static void
rtest_remove_manifest(rtest_t *t) {
  char fname[RDB_PATH_MAX];

  rtest_manifest_filename(t, fname, sizeof(fname));

  ASSERT(rdb_remove_file(fname) == RDB_OK);
}

static uint64_t
rtest_first_log(rtest_t *t) {
  rdb_array_t files;
  uint64_t number;

  rdb_array_init(&files);

  ASSERT(rtest_get_files(t, &files, RDB_FILE_LOG) > 0);

  number = files.items[0];

  rdb_array_clear(&files);

  return number;
}

static int
rtest_num_logs(rtest_t *t) {
  rdb_array_t files;
  int len;

  rdb_array_init(&files);

  len = rtest_get_files(t, &files, RDB_FILE_LOG);

  rdb_array_clear(&files);

  return len;
}

static int
rtest_num_tables(rtest_t *t) {
  rdb_array_t files;
  int len;

  rdb_array_init(&files);

  len = rtest_get_files(t, &files, RDB_FILE_TABLE);

  rdb_array_clear(&files);

  return len;
}

static uint64_t
get_file_size(const char *fname) {
  uint64_t result;
  ASSERT(rdb_get_file_size(fname, &result) == RDB_OK);
  return result;
}

static void
rtest_compact_memtable(rtest_t *t) {
  rdb_test_compact_memtable(t->db);
}

/* Directly construct a log file that sets key to val. */
static void
rtest_make_log(rtest_t *t,
               uint64_t lognum,
               rdb_seqnum_t seq,
               const char *key,
               const char *val) {
  rdb_slice_t k = rdb_string(key);
  rdb_slice_t v = rdb_string(val);
  char fname[RDB_PATH_MAX];
  rdb_logwriter_t writer;
  rdb_slice_t contents;
  rdb_wfile_t *file;
  rdb_batch_t batch;

  rdb_batch_init(&batch);

  ASSERT(rdb_log_filename(fname, sizeof(fname), t->dbname, lognum));
  ASSERT(rdb_truncfile_create(fname, &file) == RDB_OK);

  rdb_logwriter_init(&writer, file, 0);

  rdb_batch_put(&batch, &k, &v);
  rdb_batch_set_sequence(&batch, seq);

  contents = rdb_batch_contents(&batch);

  ASSERT(rdb_logwriter_add_record(&writer, &contents) == 0);
  ASSERT(rdb_wfile_flush(file) == RDB_OK);

  rdb_wfile_destroy(file);
  rdb_batch_clear(&batch);
}

/*
 * Tests
 */

static void
test_recovery_manifest_reused(rtest_t *t) {
  char old[RDB_PATH_MAX];
  char cur[RDB_PATH_MAX];

  if (!rtest_can_append(t)) {
    fprintf(stderr, "skipping test because env does not support appending\n");
    return;
  }

  ASSERT(rtest_put(t, "foo", "bar") == RDB_OK);

  rtest_close(t);

  rtest_manifest_filename(t, old, sizeof(old));

  rtest_open(t, 0);

  rtest_manifest_filename(t, cur, sizeof(cur));

  ASSERT(strcmp(old, cur) == 0);
  ASSERT(strcmp("bar", rtest_get(t, "foo")) == 0);

  rtest_open(t, 0);

  rtest_manifest_filename(t, cur, sizeof(cur));

  ASSERT(strcmp(old, cur) == 0);
  ASSERT(strcmp("bar", rtest_get(t, "foo")) == 0);
}

static void
test_recovery_large_manifest_compacted(rtest_t *t) {
  char old[RDB_PATH_MAX];
  char cur[RDB_PATH_MAX];

  if (!rtest_can_append(t)) {
    fprintf(stderr, "skipping test because env does not support appending\n");
    return;
  }

  ASSERT(rtest_put(t, "foo", "bar") == RDB_OK);

  rtest_close(t);

  rtest_manifest_filename(t, old, sizeof(old));

  /* Pad with zeroes to make manifest file very big. */
  {
    uint64_t len = get_file_size(old);
    rdb_buffer_t zeroes;
    rdb_wfile_t *file;

    ASSERT(rdb_appendfile_create(old, &file) == RDB_OK);

    rdb_buffer_init(&zeroes);
    rdb_buffer_pad(&zeroes, 3 * 1048576 - len);

    ASSERT(rdb_wfile_append(file, &zeroes) == RDB_OK);
    ASSERT(rdb_wfile_flush(file) == RDB_OK);

    rdb_buffer_clear(&zeroes);
    rdb_wfile_destroy(file);
  }

  rtest_open(t, 0);

  rtest_manifest_filename(t, cur, sizeof(cur));

  ASSERT(strcmp(old, cur) != 0);
  ASSERT(10000 > get_file_size(cur));
  ASSERT(strcmp("bar", rtest_get(t, "foo")) == 0);

  strcpy(old, cur);

  rtest_open(t, 0);

  rtest_manifest_filename(t, cur, sizeof(cur));

  ASSERT(strcmp(old, cur) == 0);
  ASSERT(strcmp("bar", rtest_get(t, "foo")) == 0);
}

static void
test_recovery_no_log_files(rtest_t *t) {
  ASSERT(rtest_put(t, "foo", "bar") == RDB_OK);
  ASSERT(1 == rtest_remove_log_files(t));

  rtest_open(t, 0);

  ASSERT(strcmp("NOT_FOUND", rtest_get(t, "foo")) == 0);

  rtest_open(t, 0);

  ASSERT(strcmp("NOT_FOUND", rtest_get(t, "foo")) == 0);
}

static void
test_recovery_log_file_reuse(rtest_t *t) {
  char fname[RDB_PATH_MAX];
  uint64_t number;
  int i;

  if (!rtest_can_append(t)) {
    fprintf(stderr, "skipping test because env does not support appending\n");
    return;
  }

  for (i = 0; i < 2; i++) {
    ASSERT(rtest_put(t, "foo", "bar") == RDB_OK);

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
    ASSERT(strcmp("bar", rtest_get(t, "foo")) == 0);

    rtest_open(t, 0);

    ASSERT(1 == rtest_num_logs(t));
    ASSERT(number == rtest_first_log(t));
    ASSERT(strcmp("bar", rtest_get(t, "foo")) == 0);
  }
}

static void
test_recovery_multiple_memtables(rtest_t *t) {
  rdb_dbopt_t opt = *rdb_dbopt_default;
  const int num = 1000;
  uint64_t old_log;
  int i;

  /* Make a large log. */
  for (i = 0; i < num; i++) {
    char buf[100];

    sprintf(buf, "%050d", i);

    ASSERT(rtest_put(t, buf, buf) == RDB_OK);
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

    ASSERT(strcmp(buf, rtest_get(t, buf)) == 0);
  }
}

static void
test_recovery_multiple_log_files(rtest_t *t) {
  uint64_t old_log, new_log;

  ASSERT(rtest_put(t, "foo", "bar") == RDB_OK);

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
  ASSERT(strcmp("bar2", rtest_get(t, "foo")) == 0);
  ASSERT(strcmp("world", rtest_get(t, "hello")) == 0);
  ASSERT(strcmp("there", rtest_get(t, "hi")) == 0);

  /* Test that previous recovery produced recoverable state. */
  rtest_open(t, 0);

  ASSERT(1 <= rtest_num_tables(t));
  ASSERT(1 == rtest_num_logs(t));

  if (rtest_can_append(t))
    ASSERT(new_log == rtest_first_log(t));

  ASSERT(strcmp("bar2", rtest_get(t, "foo")) == 0);
  ASSERT(strcmp("world", rtest_get(t, "hello")) == 0);
  ASSERT(strcmp("there", rtest_get(t, "hi")) == 0);

  /* Check that introducing an older log
     file does not cause it to be re-read. */
  rtest_close(t);

  rtest_make_log(t, old_log + 1, 2000, "hello", "stale write");

  rtest_open(t, 0);

  ASSERT(1 <= rtest_num_tables(t));
  ASSERT(1 == rtest_num_logs(t));

  if (rtest_can_append(t))
    ASSERT(new_log == rtest_first_log(t));

  ASSERT(strcmp("bar2", rtest_get(t, "foo")) == 0);
  ASSERT(strcmp("world", rtest_get(t, "hello")) == 0);
  ASSERT(strcmp("there", rtest_get(t, "hi")) == 0);
}

static void
test_recovery_manifest_missing(rtest_t *t) {
  int rc;

  ASSERT(rtest_put(t, "foo", "bar") == RDB_OK);

  rtest_close(t);
  rtest_remove_manifest(t);

  rc = rtest_open_with_status(t, 0);

  ASSERT(rc == RDB_CORRUPTION);
}

/*
 * Execute
 */

RDB_EXTERN int
rdb_test_recovery(void);

int
rdb_test_recovery(void) {
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

  rdb_env_init();

  for (i = 0; i < lengthof(tests); i++) {
    rtest_t t;

    rtest_init(&t);

    tests[i](&t);

    rtest_clear(&t);
  }

  rdb_env_clear();

  return 0;
}
