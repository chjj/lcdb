/*!
 * db_test.c - db test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "table/iterator.h"

#include "util/atomic.h"
#include "util/bloom.h"
#include "util/buffer.h"
#include "util/cache.h"
#include "util/comparator.h"
#include "util/env.h"
#include "util/extern.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/port.h"
#include "util/random.h"
#include "util/rbt.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/strutil.h"
#include "util/testutil.h"
#include "util/vector.h"

#include "db_impl.h"
#include "dbformat.h"
#include "filename.h"
#include "log_format.h"
#include "snapshot.h"
#include "write_batch.h"

/*
 * Helpers
 */

static void
rdb_sleep_msec(int64_t ms) {
  rdb_sleep_usec(ms * 1000);
}

/*
 * DBTest
 */

/* Sequence of option configurations to try. */
enum option_config {
  CONFIG_DEFAULT,
  CONFIG_REUSE,
  CONFIG_FILTER,
  CONFIG_UNCOMPRESSED,
  CONFIG_END
};

typedef struct test_s {
  char dbname[RDB_PATH_MAX];
  rdb_dbopt_t last_options;
  int config;
  rdb_bloom_t *policy;
  rdb_t *db;
  rdb_vector_t arena;
} test_t;

static void
test_reopen(test_t *t, const rdb_dbopt_t *options);

static void
test_destroy_and_reopen(test_t *t, const rdb_dbopt_t *options);

static void
test_init(test_t *t) {
  ASSERT(rdb_test_filename(t->dbname, sizeof(t->dbname), "db_test"));

  t->last_options = *rdb_dbopt_default;
  t->last_options.comparator = rdb_bytewise_comparator;
  t->last_options.compression = RDB_SNAPPY_COMPRESSION;

  t->config = CONFIG_DEFAULT;
  t->policy = rdb_bloom_create(10);
  t->db = NULL;

  rdb_vector_init(&t->arena);

  rdb_destroy_db(t->dbname, 0);

  test_reopen(t, 0);
}

static void
test_clear(test_t *t) {
  size_t i;

  if (t->db != NULL)
    rdb_close(t->db);

  rdb_destroy_db(t->dbname, 0);
  rdb_bloom_destroy(t->policy);

  for (i = 0; i < t->arena.length; i++)
    rdb_free(t->arena.items[i]);

  rdb_vector_clear(&t->arena);
}

static void
test_reset(test_t *t) {
  size_t i;

  for (i = 0; i < t->arena.length; i++)
    rdb_free(t->arena.items[i]);

  rdb_vector_reset(&t->arena);
}

static const char *
test_key(test_t *t, int i) {
  char *zp = rdb_malloc(15);

  sprintf(zp, "key%06d", i);

  rdb_vector_push(&t->arena, zp);

  return zp;
}

static const char *
test_key2(test_t *t, int i, const char *suffix) {
  char *zp = rdb_malloc(15 + 8);

  ASSERT(strlen(suffix) <= 8);

  sprintf(zp, "key%06d%s", i, suffix);

  rdb_vector_push(&t->arena, zp);

  return zp;
}

static const char *
random_key(test_t *t, rdb_rand_t *rnd) {
  int len = (rdb_rand_one_in(rnd, 3)
    ? 1 /* Short sometimes to encourage collisions. */
    : (rdb_rand_one_in(rnd, 100)
      ? rdb_rand_skewed(rnd, 10)
      : rdb_rand_uniform(rnd, 10)));

  rdb_buffer_t z;

  rdb_buffer_init(&z);
  rdb_random_key(&z, rnd, len);

  rdb_vector_push(&t->arena, z.data);

  return (char *)z.data;
}

static const char *
random_string(test_t *t, rdb_rand_t *rnd, size_t len) {
  rdb_buffer_t z;

  rdb_buffer_init(&z);
  rdb_random_string(&z, rnd, len);

  rdb_vector_push(&t->arena, z.data);

  return (char *)z.data;
}

static const char *
string_fill(test_t *t, int ch, size_t len) {
  char *zp = rdb_malloc(len + 1);

  memset(zp, ch, len);

  zp[len] = '\0';

  rdb_vector_push(&t->arena, zp);

  return zp;
}

static const char *
string_fill2(test_t *t, const char *prefix, int ch, size_t len) {
  size_t plen = strlen(prefix);
  char *buf = rdb_malloc(plen + len + 1);
  char *zp = buf;

  memcpy(zp, prefix, plen);
  zp += plen;

  memset(zp, ch, len);
  zp += len;

  *zp = '\0';

  rdb_vector_push(&t->arena, buf);

  return buf;
}

/* Switch to a fresh database with the next option configuration to
   test. Return false if there are no more configurations to test. */
static int
test_change_options(test_t *t) {
  if (++t->config >= CONFIG_END)
    return 0;

  test_destroy_and_reopen(t, 0);
  test_reset(t);

  return 1;
}

/* Return the current option configuration. */
static rdb_dbopt_t
test_current_options(test_t *t) {
  rdb_dbopt_t options = *rdb_dbopt_default;

  options.comparator = rdb_bytewise_comparator;
  options.compression = RDB_SNAPPY_COMPRESSION;
  options.reuse_logs = 0;

  switch (t->config) {
    case CONFIG_REUSE:
      options.reuse_logs = 1;
      break;
    case CONFIG_FILTER:
      options.filter_policy = t->policy;
      break;
    case CONFIG_UNCOMPRESSED:
      options.compression = RDB_NO_COMPRESSION;
      break;
    default:
      break;
  }

  return options;
}

static int
test_try_reopen(test_t *t, const rdb_dbopt_t *options) {
  rdb_dbopt_t opts;

  if (t->db != NULL)
    rdb_close(t->db);

  t->db = NULL;

  if (options != NULL) {
    opts = *options;
  } else {
    opts = test_current_options(t);
    opts.create_if_missing = 1;
  }

  t->last_options = opts;

  if (t->last_options.comparator == NULL)
    t->last_options.comparator = rdb_bytewise_comparator;

  return rdb_open(t->dbname, &opts, &t->db);
}

static void
test_reopen(test_t *t, const rdb_dbopt_t *options) {
  ASSERT(test_try_reopen(t, options) == RDB_OK);
}

static void
test_destroy_and_reopen(test_t *t, const rdb_dbopt_t *options) {
  if (t->db != NULL)
    rdb_close(t->db);

  t->db = NULL;

  rdb_destroy_db(t->dbname, 0);

  ASSERT(test_try_reopen(t, options) == RDB_OK);
}

static void
test_close(test_t *t) {
  if (t->db != NULL)
    rdb_close(t->db);

  t->db = NULL;
}

static int
test_put(test_t *t, const char *k, const char *v) {
  rdb_slice_t key = rdb_string(k);
  rdb_slice_t val = rdb_string(v);

  return rdb_put(t->db, &key, &val, 0);
}

static int
test_del(test_t *t, const char *k) {
  rdb_slice_t key = rdb_string(k);

  return rdb_del(t->db, &key, 0);
}

static const char *
test_get2(test_t *t, const char *k, const rdb_snapshot_t *snap) {
  rdb_readopt_t opt = *rdb_readopt_default;
  rdb_slice_t key = rdb_string(k);
  rdb_slice_t val;
  char *zp;
  int rc;

  opt.snapshot = snap;

  rc = rdb_get(t->db, &key, &val, &opt);

  if (rc == RDB_NOTFOUND)
    return "NOT_FOUND";

  if (rc != RDB_OK)
    return rdb_strerror(rc);

  zp = rdb_malloc(val.size + 1);

  memcpy(zp, val.data, val.size);

  zp[val.size] = '\0';

  rdb_free(val.data);

  rdb_vector_push(&t->arena, zp);

  return zp;
}

static const char *
test_get(test_t *t, const char *k) {
  return test_get2(t, k, NULL);
}

static int
test_has(test_t *t, const char *k) {
  rdb_slice_t key = rdb_string(k);
  int rc;

  rc = rdb_has(t->db, &key, 0);

  if (rc == RDB_NOTFOUND)
    return 0;

  ASSERT(rc == RDB_OK);

  return 1;
}

static const char *
iter_status(test_t *t, rdb_iter_t *iter) {
  rdb_slice_t key, val;
  rdb_buffer_t z;

  if (!rdb_iter_valid(iter))
    return "(invalid)";

  key = rdb_iter_key(iter);
  val = rdb_iter_value(iter);

  rdb_buffer_init(&z);
  rdb_buffer_concat(&z, &key);
  rdb_buffer_string(&z, "->");
  rdb_buffer_concat(&z, &val);
  rdb_buffer_push(&z, 0);

  rdb_vector_push(&t->arena, z.data);

  return (char *)z.data;
}

static void
iter_seek(rdb_iter_t *iter, const char *k) {
  rdb_slice_t key = rdb_string(k);
  rdb_iter_seek(iter, &key);
}

/* Return a string that contains all key,value pairs in order,
   formatted like "(k1->v1)(k2->v2)". */
static const char *
test_contents(test_t *t) {
  rdb_vector_t forward;
  rdb_iter_t *iter;
  rdb_buffer_t z;
  size_t matched = 0;

  rdb_buffer_init(&z);
  rdb_vector_init(&forward);

  iter = rdb_iterator(t->db, 0);

  for (rdb_iter_seek_first(iter); rdb_iter_valid(iter); rdb_iter_next(iter)) {
    const char *s = iter_status(t, iter);

    rdb_buffer_push(&z, '(');
    rdb_buffer_string(&z, s);
    rdb_buffer_push(&z, ')');

    rdb_vector_push(&forward, s);
  }

  rdb_buffer_push(&z, 0);

  /* Check reverse iteration results are the reverse of forward results. */
  for (rdb_iter_seek_last(iter); rdb_iter_valid(iter); rdb_iter_prev(iter)) {
    size_t index = forward.length - matched - 1;

    ASSERT(matched < forward.length);
    ASSERT_EQ(iter_status(t, iter), forward.items[index]);

    matched++;
  }

  ASSERT(matched == forward.length);

  rdb_iter_destroy(iter);

  rdb_vector_clear(&forward);

  rdb_vector_push(&t->arena, z.data);

  return (char *)z.data;
}

static const char *
test_all_entries(test_t *t, const char *user_key) {
  rdb_slice_t ukey = rdb_string(user_key);
  rdb_iter_t *iter;
  rdb_ikey_t ikey;
  rdb_buffer_t z;
  int first = 1;
  int rc;

  rdb_buffer_init(&z);
  rdb_ikey_init(&ikey);

  rdb_ikey_set(&ikey, &ukey, RDB_MAX_SEQUENCE, RDB_TYPE_VALUE);

  iter = rdb_test_internal_iterator(t->db);

  rdb_iter_seek(iter, &ikey);

  rc = rdb_iter_status(iter);

  if (rc != RDB_OK) {
    rdb_buffer_string(&z, rdb_strerror(rc));
  } else {
    rdb_buffer_string(&z, "[ ");

    while (rdb_iter_valid(iter)) {
      rdb_slice_t key = rdb_iter_key(iter);
      rdb_slice_t val;
      rdb_pkey_t pkey;

      if (!rdb_pkey_import(&pkey, &key)) {
        rdb_buffer_string(&z, "CORRUPTED");
      } else {
        const rdb_comparator_t *cmp = t->last_options.comparator;

        if (rdb_compare(cmp, &pkey.user_key, &ukey) != 0)
          break;

        if (!first)
          rdb_buffer_string(&z, ", ");

        first = 0;

        switch (pkey.type) {
          case RDB_TYPE_VALUE:
            val = rdb_iter_value(iter);
            rdb_buffer_concat(&z, &val);
            break;
          case RDB_TYPE_DELETION:
            rdb_buffer_string(&z, "DEL");
            break;
        }
      }

      rdb_iter_next(iter);
    }

    if (!first)
      rdb_buffer_string(&z, " ");

    rdb_buffer_string(&z, "]");
  }

  rdb_buffer_push(&z, 0);

  rdb_iter_destroy(iter);
  rdb_ikey_clear(&ikey);

  rdb_vector_push(&t->arena, z.data);

  return (char *)z.data;
}

static int
test_files_at_level(test_t *t, int level) {
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
test_total_files(test_t *t) {
  int result = 0;
  int level;

  for (level = 0; level < RDB_NUM_LEVELS; level++)
    result += test_files_at_level(t, level);

  return result;
}

/* Return spread of files per level. */
static const char *
test_files_per_level(test_t *t) {
  char *buf = rdb_malloc(256);
  char *last = buf;
  char *zp = buf;
  int level;

  for (level = 0; level < RDB_NUM_LEVELS; level++) {
    int f = test_files_at_level(t, level);

    zp += sprintf(zp, "%s%d", (level ? "," : ""), f);

    if (f > 0)
      last = zp;
  }

  last[0] = '\0';

  rdb_vector_push(&t->arena, buf);

  return buf;
}

static int
test_count_files(test_t *t) {
  char **files;
  int len;

  len = rdb_get_children(t->dbname, &files);

  if (len >= 0)
    rdb_free_children(files, len);

  if (len < 0)
    len = 0;

  return len;
}

static uint64_t
test_size(test_t *t, const char *start, const char *limit) {
  rdb_range_t r;
  uint64_t size;

  r.start = rdb_string(start);
  r.limit = rdb_string(limit);

  rdb_get_approximate_sizes(t->db, &r, 1, &size);

  return size;
}

static void
test_compact(test_t *t, const char *start, const char *limit) {
  rdb_slice_t s = rdb_string(start);
  rdb_slice_t l = rdb_string(limit);

  rdb_compact_range(t->db, &s, &l);
}

/* Do n memtable compactions, each of which produces an sstable
   covering the range [small_key,large_key]. */
static void
test_make_tables(test_t *t, int n, const char *small, const char *large) {
  int i;

  for (i = 0; i < n; i++) {
    test_put(t, small, "begin");
    test_put(t, large, "end");

    rdb_test_compact_memtable(t->db);
  }
}

/* Prevent pushing of new sstables into deeper levels by adding
   tables that cover a specified range to all levels. */
static void
test_fill_levels(test_t *t, const char *small, const char *large) {
  test_make_tables(t, RDB_NUM_LEVELS, small, large);
}

RDB_UNUSED static void
test_dump_file_counts(test_t *t, const char *label) {
  int level;

  fprintf(stderr, "---\n%s:\n", label);

  fprintf(stderr, "maxoverlap: %.0f\n",
    (double)rdb_test_max_next_level_overlapping_bytes(t->db));

  for (level = 0; level < RDB_NUM_LEVELS; level++) {
    int num = test_files_at_level(t, level);

    if (num > 0)
      fprintf(stderr, "  level %3d : %d files\n", level, num);
  }
}

RDB_UNUSED static const char *
test_dump_sst_list(test_t *t) {
  char *value;

  ASSERT(rdb_get_property(t->db, "leveldb.sstables", &value));

  rdb_vector_push(&t->arena, value);

  return value;
}

static int
test_delete_an_sst_file(test_t *t) {
  char fname[RDB_PATH_MAX];
  rdb_filetype_t type;
  uint64_t number;
  int found = 0;
  char **names;
  int i, len;

  len = rdb_get_children(t->dbname, &names);

  ASSERT(len >= 0);

  for (i = 0; i < len; i++) {
    if (!rdb_parse_filename(&type, &number, names[i]))
      continue;

    if (type != RDB_FILE_TABLE)
      continue;

    ASSERT(rdb_table_filename(fname, sizeof(fname), t->dbname, number));
    ASSERT(rdb_remove_file(fname) == RDB_OK);

    found = 1;

    break;
  }

  rdb_free_children(names, len);

  return found;
}

/* Returns number of files renamed. */
static int
test_rename_ldb_to_sst(test_t *t) {
  char from[RDB_PATH_MAX];
  char to[RDB_PATH_MAX];
  rdb_filetype_t type;
  uint64_t number;
  int renamed = 0;
  char **names;
  int i, len;

  len = rdb_get_children(t->dbname, &names);

  ASSERT(len >= 0);

  for (i = 0; i < len; i++) {
    if (!rdb_parse_filename(&type, &number, names[i]))
      continue;

    if (type != RDB_FILE_TABLE)
      continue;

    ASSERT(rdb_table_filename(from, sizeof(from), t->dbname, number));
    ASSERT(rdb_sstable_filename(to, sizeof(to), t->dbname, number));
    ASSERT(rdb_rename_file(from, to) == RDB_OK);

    renamed++;
  }

  rdb_free_children(names, len);

  return renamed;
}

/*
 * Tests
 */

static void
test_db_empty(test_t *t) {
  do {
    ASSERT(t->db != NULL);
    ASSERT_EQ("NOT_FOUND", test_get(t, "foo"));
  } while (test_change_options(t));
}

static void
test_db_empty_key(test_t *t) {
  do {
    ASSERT(test_put(t, "", "v1") == RDB_OK);
    ASSERT_EQ("v1", test_get(t, ""));
    ASSERT(test_put(t, "", "v2") == RDB_OK);
    ASSERT_EQ("v2", test_get(t, ""));
  } while (test_change_options(t));
}

static void
test_db_empty_value(test_t *t) {
  do {
    ASSERT(test_put(t, "key", "v1") == RDB_OK);
    ASSERT_EQ("v1", test_get(t, "key"));
    ASSERT(test_put(t, "key", "") == RDB_OK);
    ASSERT_EQ("", test_get(t, "key"));
    ASSERT(test_put(t, "key", "v2") == RDB_OK);
    ASSERT_EQ("v2", test_get(t, "key"));
  } while (test_change_options(t));
}

static void
test_db_read_write(test_t *t) {
  do {
    ASSERT(test_put(t, "foo", "v1") == RDB_OK);
    ASSERT_EQ("v1", test_get(t, "foo"));
    ASSERT(test_put(t, "bar", "v2") == RDB_OK);
    ASSERT(test_put(t, "foo", "v3") == RDB_OK);
    ASSERT_EQ("v3", test_get(t, "foo"));
    ASSERT_EQ("v2", test_get(t, "bar"));
  } while (test_change_options(t));
}

static void
test_db_put_delete_get(test_t *t) {
  do {
    ASSERT(test_put(t, "foo", "v1") == RDB_OK);
    ASSERT_EQ("v1", test_get(t, "foo"));
    ASSERT(test_put(t, "foo", "v2") == RDB_OK);
    ASSERT_EQ("v2", test_get(t, "foo"));
    ASSERT(test_del(t, "foo") == RDB_OK);
    ASSERT_EQ("NOT_FOUND", test_get(t, "foo"));
  } while (test_change_options(t));
}

static void
test_db_get_from_immutable_layer(test_t *t) {
  do {
    rdb_dbopt_t options = test_current_options(t);

    options.write_buffer_size = 100000; /* Small write buffer */

    test_reopen(t, &options);

    ASSERT(test_put(t, "foo", "v1") == RDB_OK);

    ASSERT_EQ("v1", test_get(t, "foo"));

    /* Block sync calls. */
    /* rdb_atomic_store(&env->delay_data_sync, 1, rdb_order_release); */

    test_put(t, "k1", string_fill(t, 'x', 100000)); /* Fill memtable. */
    test_put(t, "k2", string_fill(t, 'y', 100000)); /* Trigger compaction. */

    ASSERT_EQ("v1", test_get(t, "foo"));

    /* Release sync calls. */
    /* rdb_atomic_store(&env->delay_data_sync, 0, rdb_order_release); */
  } while (test_change_options(t));
}

static void
test_db_get_from_versions(test_t *t) {
  do {
    ASSERT(test_put(t, "foo", "v1") == RDB_OK);
    rdb_test_compact_memtable(t->db);
    ASSERT_EQ("v1", test_get(t, "foo"));
  } while (test_change_options(t));
}

static void
test_db_get_memusage(test_t *t) {
  int mem_usage;
  char *val;

  do {
    ASSERT(test_put(t, "foo", "v1") == RDB_OK);
    ASSERT(rdb_get_property(t->db, "leveldb.approximate-memory-usage", &val));

    mem_usage = atoi(val);

    rdb_free(val);

    ASSERT(mem_usage > 0);
    ASSERT(mem_usage < 5 * 1024 * 1024);
  } while (test_change_options(t));
}

static void
test_db_get_snapshot(test_t *t) {
  do {
    /* Try with both a short key and a long key */
    int i;

    for (i = 0; i < 2; i++) {
      const char *key = (i == 0) ? "foo" : string_fill(t, 'x', 200);
      const rdb_snapshot_t *s1;

      ASSERT(test_put(t, key, "v1") == RDB_OK);

      s1 = rdb_get_snapshot(t->db);

      ASSERT(test_put(t, key, "v2") == RDB_OK);
      ASSERT_EQ("v2", test_get(t, key));
      ASSERT_EQ("v1", test_get2(t, key, s1));

      rdb_test_compact_memtable(t->db);

      ASSERT_EQ("v2", test_get(t, key));
      ASSERT_EQ("v1", test_get2(t, key, s1));

      rdb_release_snapshot(t->db, s1);
    }
  } while (test_change_options(t));
}

static void
test_db_get_identical_snapshots(test_t *t) {
  do {
    /* Try with both a short key and a long key */
    int i;

    for (i = 0; i < 2; i++) {
      const char *key = (i == 0) ? "foo" : string_fill(t, 'x', 200);
      const rdb_snapshot_t *s1;
      const rdb_snapshot_t *s2;
      const rdb_snapshot_t *s3;

      ASSERT(test_put(t, key, "v1") == RDB_OK);

      s1 = rdb_get_snapshot(t->db);
      s2 = rdb_get_snapshot(t->db);
      s3 = rdb_get_snapshot(t->db);

      ASSERT(test_put(t, key, "v2") == RDB_OK);

      ASSERT_EQ("v2", test_get(t, key));
      ASSERT_EQ("v1", test_get2(t, key, s1));
      ASSERT_EQ("v1", test_get2(t, key, s2));
      ASSERT_EQ("v1", test_get2(t, key, s3));

      rdb_release_snapshot(t->db, s1);

      rdb_test_compact_memtable(t->db);

      ASSERT_EQ("v2", test_get(t, key));
      ASSERT_EQ("v1", test_get2(t, key, s2));

      rdb_release_snapshot(t->db, s2);

      ASSERT_EQ("v1", test_get2(t, key, s3));

      rdb_release_snapshot(t->db, s3);
    }
  } while (test_change_options(t));
}

static void
test_db_iterate_over_empty_snapshot(test_t *t) {
  do {
    rdb_readopt_t options = *rdb_readopt_default;
    const rdb_snapshot_t *snapshot;
    rdb_iter_t *iter;

    snapshot = rdb_get_snapshot(t->db);

    options.snapshot = snapshot;

    ASSERT(test_put(t, "foo", "v1") == RDB_OK);
    ASSERT(test_put(t, "foo", "v2") == RDB_OK);

    iter = rdb_iterator(t->db, &options);

    rdb_iter_seek_first(iter);

    ASSERT(!rdb_iter_valid(iter));

    rdb_iter_destroy(iter);

    rdb_test_compact_memtable(t->db);

    iter = rdb_iterator(t->db, &options);

    rdb_iter_seek_first(iter);

    ASSERT(!rdb_iter_valid(iter));

    rdb_iter_destroy(iter);

    rdb_release_snapshot(t->db, snapshot);
  } while (test_change_options(t));
}

static void
test_db_get_level0_ordering(test_t *t) {
  do {
    /* Check that we process level-0 files in correct order. The code
       below generates two level-0 files where the earlier one comes
       before the later one in the level-0 file list since the earlier
       one has a smaller "smallest" key. */
    ASSERT(test_put(t, "bar", "b") == RDB_OK);
    ASSERT(test_put(t, "foo", "v1") == RDB_OK);

    rdb_test_compact_memtable(t->db);

    ASSERT(test_put(t, "foo", "v2") == RDB_OK);

    rdb_test_compact_memtable(t->db);

    ASSERT_EQ("v2", test_get(t, "foo"));
  } while (test_change_options(t));
}

static void
test_db_get_ordered_by_levels(test_t *t) {
  do {
    ASSERT(test_put(t, "foo", "v1") == RDB_OK);

    test_compact(t, "a", "z");

    ASSERT_EQ("v1", test_get(t, "foo"));
    ASSERT(test_put(t, "foo", "v2") == RDB_OK);
    ASSERT_EQ("v2", test_get(t, "foo"));

    rdb_test_compact_memtable(t->db);

    ASSERT_EQ("v2", test_get(t, "foo"));
  } while (test_change_options(t));
}

static void
test_db_get_picks_correct_file(test_t *t) {
  do {
    /* Arrange to have multiple files in a non-level-0 level. */
    ASSERT(test_put(t, "a", "va") == RDB_OK);

    test_compact(t, "a", "b");

    ASSERT(test_put(t, "x", "vx") == RDB_OK);

    test_compact(t, "x", "y");

    ASSERT(test_put(t, "f", "vf") == RDB_OK);

    test_compact(t, "f", "g");

    ASSERT_EQ("va", test_get(t, "a"));
    ASSERT_EQ("vf", test_get(t, "f"));
    ASSERT_EQ("vx", test_get(t, "x"));
  } while (test_change_options(t));
}

static void
test_db_get_encounters_empty_level(test_t *t) {
  do {
    /* Arrange for the following to happen:
     *
     *   - sstable A in level 0
     *   - nothing in level 1
     *   - sstable B in level 2
     *
     * Then do enough Get() calls to arrange for an automatic compaction
     * of sstable A. A bug would cause the compaction to be marked as
     * occurring at level 1 (instead of the correct level 0).
     */
    int i;

    /* Step 1: First place sstables in levels 0 and 2 */
    int compaction_count = 0;

    while (test_files_at_level(t, 0) == 0 || test_files_at_level(t, 2) == 0) {
      ASSERT(compaction_count <= 100);

      compaction_count++;

      test_put(t, "a", "begin");
      test_put(t, "z", "end");

      rdb_test_compact_memtable(t->db);
    }

    /* Step 2: clear level 1 if necessary. */
    rdb_test_compact_range(t->db, 1, NULL, NULL);

    ASSERT(test_files_at_level(t, 0) == 1);
    ASSERT(test_files_at_level(t, 1) == 0);
    ASSERT(test_files_at_level(t, 2) == 1);

    /* Step 3: read a bunch of times */
    for (i = 0; i < 1000; i++)
      ASSERT_EQ("NOT_FOUND", test_get(t, "missing"));

    /* Step 4: Wait for compaction to finish */
    rdb_sleep_msec(1000);

    ASSERT(test_files_at_level(t, 0) == 0);
  } while (test_change_options(t));
}

static void
test_db_iter_empty(test_t *t) {
  rdb_iter_t *iter = rdb_iterator(t->db, 0);

  rdb_iter_seek_first(iter);

  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  rdb_iter_seek_last(iter);

  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  iter_seek(iter, "foo");

  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  rdb_iter_destroy(iter);
}

static void
test_db_iter_single(test_t *t) {
  rdb_iter_t *iter;

  ASSERT(test_put(t, "a", "va") == RDB_OK);

  iter = rdb_iterator(t->db, 0);

  rdb_iter_seek_first(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");
  rdb_iter_seek_first(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  rdb_iter_seek_last(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");
  rdb_iter_seek_last(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  iter_seek(iter, "");
  ASSERT_EQ(iter_status(t, iter), "a->va");
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  iter_seek(iter, "a");
  ASSERT_EQ(iter_status(t, iter), "a->va");
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  iter_seek(iter, "b");
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  rdb_iter_destroy(iter);
}

static void
test_db_iter_multi(test_t *t) {
  rdb_iter_t *iter;

  ASSERT(test_put(t, "a", "va") == RDB_OK);
  ASSERT(test_put(t, "b", "vb") == RDB_OK);
  ASSERT(test_put(t, "c", "vc") == RDB_OK);

  iter = rdb_iterator(t->db, 0);

  rdb_iter_seek_first(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "b->vb");
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "c->vc");
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");
  rdb_iter_seek_first(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  rdb_iter_seek_last(iter);
  ASSERT_EQ(iter_status(t, iter), "c->vc");
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "b->vb");
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");
  rdb_iter_seek_last(iter);
  ASSERT_EQ(iter_status(t, iter), "c->vc");
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  iter_seek(iter, "");
  ASSERT_EQ(iter_status(t, iter), "a->va");
  iter_seek(iter, "a");
  ASSERT_EQ(iter_status(t, iter), "a->va");
  iter_seek(iter, "ax");
  ASSERT_EQ(iter_status(t, iter), "b->vb");
  iter_seek(iter, "b");
  ASSERT_EQ(iter_status(t, iter), "b->vb");
  iter_seek(iter, "z");
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  /* Switch from reverse to forward */
  rdb_iter_seek_last(iter);
  rdb_iter_prev(iter);
  rdb_iter_prev(iter);
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "b->vb");

  /* Switch from forward to reverse */
  rdb_iter_seek_first(iter);
  rdb_iter_next(iter);
  rdb_iter_next(iter);
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "b->vb");

  /* Make sure iter stays at snapshot */
  ASSERT(test_put(t, "a", "va2") == RDB_OK);
  ASSERT(test_put(t, "a2", "va3") == RDB_OK);
  ASSERT(test_put(t, "b", "vb2") == RDB_OK);
  ASSERT(test_put(t, "c", "vc2") == RDB_OK);
  ASSERT(test_del(t, "b") == RDB_OK);

  rdb_iter_seek_first(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "b->vb");
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "c->vc");
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");
  rdb_iter_seek_last(iter);
  ASSERT_EQ(iter_status(t, iter), "c->vc");
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "b->vb");
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  rdb_iter_destroy(iter);
}

static void
test_db_iter_small_and_large_mix(test_t *t) {
  rdb_iter_t *iter;

  ASSERT(test_put(t, "a", "va") == RDB_OK);
  ASSERT(test_put(t, "b", string_fill(t, 'b', 100000)) == RDB_OK);
  ASSERT(test_put(t, "c", "vc") == RDB_OK);
  ASSERT(test_put(t, "d", string_fill(t, 'd', 100000)) == RDB_OK);
  ASSERT(test_put(t, "e", string_fill(t, 'e', 100000)) == RDB_OK);

  iter = rdb_iterator(t->db, 0);

  rdb_iter_seek_first(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), string_fill2(t, "b->", 'b', 100000));
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "c->vc");
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), string_fill2(t, "d->", 'd', 100000));
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), string_fill2(t, "e->", 'e', 100000));
  rdb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  rdb_iter_seek_last(iter);
  ASSERT_EQ(iter_status(t, iter), string_fill2(t, "e->", 'e', 100000));
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), string_fill2(t, "d->", 'd', 100000));
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "c->vc");
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), string_fill2(t, "b->", 'b', 100000));
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  rdb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  rdb_iter_destroy(iter);
}

static void
test_db_iter_multi_with_delete(test_t *t) {
  do {
    rdb_iter_t *iter;

    ASSERT(test_put(t, "a", "va") == RDB_OK);
    ASSERT(test_put(t, "b", "vb") == RDB_OK);
    ASSERT(test_put(t, "c", "vc") == RDB_OK);
    ASSERT(test_del(t, "b") == RDB_OK);

    ASSERT_EQ("NOT_FOUND", test_get(t, "b"));

    iter = rdb_iterator(t->db, 0);

    iter_seek(iter, "c");
    ASSERT_EQ(iter_status(t, iter), "c->vc");

    rdb_iter_prev(iter);
    ASSERT_EQ(iter_status(t, iter), "a->va");

    rdb_iter_destroy(iter);
  } while (test_change_options(t));
}

static void
test_db_iter_multi_with_delete_and_compaction(test_t *t) {
  do {
    rdb_iter_t *iter;

    ASSERT(test_put(t, "b", "vb") == RDB_OK);
    ASSERT(test_put(t, "c", "vc") == RDB_OK);
    ASSERT(test_put(t, "a", "va") == RDB_OK);

    rdb_test_compact_memtable(t->db);

    ASSERT(test_del(t, "b") == RDB_OK);
    ASSERT_EQ("NOT_FOUND", test_get(t, "b"));

    iter = rdb_iterator(t->db, 0);

    iter_seek(iter, "c");
    ASSERT_EQ(iter_status(t, iter), "c->vc");

    rdb_iter_prev(iter);
    ASSERT_EQ(iter_status(t, iter), "a->va");

    iter_seek(iter, "b");
    ASSERT_EQ(iter_status(t, iter), "c->vc");

    rdb_iter_destroy(iter);
  } while (test_change_options(t));
}

static void
test_db_recover(test_t *t) {
  do {
    ASSERT(test_put(t, "foo", "v1") == RDB_OK);
    ASSERT(test_put(t, "baz", "v5") == RDB_OK);

    test_reopen(t, 0);

    ASSERT_EQ("v1", test_get(t, "foo"));
    ASSERT_EQ("v1", test_get(t, "foo"));
    ASSERT_EQ("v5", test_get(t, "baz"));
    ASSERT(test_put(t, "bar", "v2") == RDB_OK);
    ASSERT(test_put(t, "foo", "v3") == RDB_OK);

    test_reopen(t, 0);

    ASSERT_EQ("v3", test_get(t, "foo"));
    ASSERT(test_put(t, "foo", "v4") == RDB_OK);
    ASSERT_EQ("v4", test_get(t, "foo"));
    ASSERT_EQ("v2", test_get(t, "bar"));
    ASSERT_EQ("v5", test_get(t, "baz"));
  } while (test_change_options(t));
}

static void
test_db_recover_with_empty_log(test_t *t) {
  do {
    ASSERT(test_put(t, "foo", "v1") == RDB_OK);
    ASSERT(test_put(t, "foo", "v2") == RDB_OK);

    test_reopen(t, 0);
    test_reopen(t, 0);

    ASSERT(test_put(t, "foo", "v3") == RDB_OK);

    test_reopen(t, 0);

    ASSERT_EQ("v3", test_get(t, "foo"));
  } while (test_change_options(t));
}

/* Check that writes done during a memtable compaction are recovered
   if the database is shutdown during the memtable compaction. */
static void
test_db_recover_during_memtable_compaction(test_t *t) {
  do {
    rdb_dbopt_t options = test_current_options(t);

    options.write_buffer_size = 1000000;

    test_reopen(t, &options);

    /* Trigger a long memtable compaction and reopen the database during it */
    ASSERT(test_put(t, "foo", "v1") == RDB_OK); /* Goes to 1st log file */
    ASSERT(test_put(t, "big1", string_fill(t, 'x', 10000000)) == RDB_OK); /* Fills memtable */
    ASSERT(test_put(t, "big2", string_fill(t, 'y', 1000)) == RDB_OK); /* Triggers compaction */
    ASSERT(test_put(t, "bar", "v2") == RDB_OK); /* Goes to new log file */

    test_reopen(t, &options);

    ASSERT_EQ("v1", test_get(t, "foo"));
    ASSERT_EQ("v2", test_get(t, "bar"));
    ASSERT_EQ(string_fill(t, 'x', 10000000), test_get(t, "big1"));
    ASSERT_EQ(string_fill(t, 'y', 1000), test_get(t, "big2"));
  } while (test_change_options(t));
}

static void
test_db_minor_compactions_happen(test_t *t) {
  rdb_dbopt_t options = test_current_options(t);
  int starting_num_tables;
  int ending_num_tables;
  const int N = 500;
  int i;

  options.write_buffer_size = 10000;

  test_reopen(t, &options);

  starting_num_tables = test_total_files(t);

  for (i = 0; i < N; i++) {
    const char *key = test_key(t, i);
    const char *val = string_fill2(t, key, 'v', 1000);

    ASSERT(test_put(t, key, val) == RDB_OK);
  }

  ending_num_tables = test_total_files(t);

  ASSERT(ending_num_tables > starting_num_tables);

  for (i = 0; i < N; i++) {
    const char *key = test_key(t, i);
    const char *val = string_fill2(t, key, 'v', 1000);

    ASSERT_EQ(val, test_get(t, key));
  }

  test_reopen(t, 0);

  for (i = 0; i < N; i++) {
    const char *key = test_key(t, i);
    const char *val = string_fill2(t, key, 'v', 1000);

    ASSERT_EQ(val, test_get(t, key));
  }
}

static void
test_db_recover_with_large_log(test_t *t) {
  {
    rdb_dbopt_t options = test_current_options(t);

    test_reopen(t, &options);

    ASSERT(test_put(t, "big1", string_fill(t, '1', 200000)) == RDB_OK);
    ASSERT(test_put(t, "big2", string_fill(t, '2', 200000)) == RDB_OK);
    ASSERT(test_put(t, "small3", string_fill(t, '3', 10)) == RDB_OK);
    ASSERT(test_put(t, "small4", string_fill(t, '4', 10)) == RDB_OK);
    ASSERT(test_files_at_level(t, 0) == 0);
  }

  /* Make sure that if we re-open with a small write buffer size that
     we flush table files in the middle of a large log file. */
  {
    rdb_dbopt_t options = test_current_options(t);

    options.write_buffer_size = 100000;

    test_reopen(t, &options);

    ASSERT(test_files_at_level(t, 0) == 3);
    ASSERT_EQ(string_fill(t, '1', 200000), test_get(t, "big1"));
    ASSERT_EQ(string_fill(t, '2', 200000), test_get(t, "big2"));
    ASSERT_EQ(string_fill(t, '3', 10), test_get(t, "small3"));
    ASSERT_EQ(string_fill(t, '4', 10), test_get(t, "small4"));
    ASSERT(test_files_at_level(t, 0) > 1);
  }
}

static void
test_db_compactions_generate_multiple_files(test_t *t) {
  rdb_dbopt_t options = test_current_options(t);
  rdb_vector_t values;
  rdb_rand_t rnd;
  int i;

  rdb_vector_init(&values);
  rdb_rand_init(&rnd, 301);

  options.write_buffer_size = 100000000; /* Large write buffer */

  test_reopen(t, &options);

  /* Write 8MB (80 values, each 100K) */
  ASSERT(test_files_at_level(t, 0) == 0);

  for (i = 0; i < 80; i++) {
    const char *value = random_string(t, &rnd, 100000);

    ASSERT(test_put(t, test_key(t, i), value) == RDB_OK);

    rdb_vector_push(&values, value);
  }

  /* Reopening moves updates to level-0 */
  test_reopen(t, &options);

  rdb_test_compact_range(t->db, 0, NULL, NULL);

  ASSERT(test_files_at_level(t, 0) == 0);
  ASSERT(test_files_at_level(t, 1) > 1);

  for (i = 0; i < 80; i++)
    ASSERT_EQ(test_get(t, test_key(t, i)), values.items[i]);

  rdb_vector_clear(&values);
}

static void
test_db_repeated_writes_to_same_key(test_t *t) {
  rdb_dbopt_t options = test_current_options(t);
  const char *value;
  int i, max_files;
  rdb_rand_t rnd;

  rdb_rand_init(&rnd, 301);

  options.write_buffer_size = 100000; /* Small write buffer */

  test_reopen(t, &options);

  /* We must have at most one file per level except for level-0,
     which may have up to kL0_StopWritesTrigger files. */
  max_files = RDB_NUM_LEVELS + RDB_L0_STOP_WRITES_TRIGGER;

  value = random_string(t, &rnd, 2 * options.write_buffer_size);

  for (i = 0; i < 5 * max_files; i++) {
    test_put(t, "key", value);

    ASSERT(test_total_files(t) <= max_files);

    fprintf(stderr, "after %d: %d files\n", i + 1, test_total_files(t));
  }
}

static void
test_db_sparse_merge(test_t *t) {
  rdb_dbopt_t options = test_current_options(t);
  const char *value;
  int i;

  options.compression = RDB_NO_COMPRESSION;

  test_reopen(t, &options);

  test_fill_levels(t, "A", "Z");

  /* Suppose there is:
   *    small amount of data with prefix A
   *    large amount of data with prefix B
   *    small amount of data with prefix C
   * and that recent updates have made small changes to all three prefixes.
   * Check that we do not do a compaction that merges all of B in one shot.
   */
  value = string_fill(t, 'x', 1000);

  test_put(t, "A", "va");

  /* Write approximately 100MB of "B" values */
  for (i = 0; i < 100000; i++) {
    char key[100];
    sprintf(key, "B%010d", i);
    test_put(t, key, value);
  }

  test_put(t, "C", "vc");

  rdb_test_compact_memtable(t->db);
  rdb_test_compact_range(t->db, 0, NULL, NULL);

  /* Make sparse update */
  test_put(t, "A", "va2");
  test_put(t, "B100", "bvalue2");
  test_put(t, "C", "vc2");

  rdb_test_compact_memtable(t->db);

  /* Compactions should not cause us to create a situation where
     a file overlaps too much data at the next level. */
  ASSERT(rdb_test_max_next_level_overlapping_bytes(t->db) <= 20 * 1048576);

  rdb_test_compact_range(t->db, 0, NULL, NULL);
  ASSERT(rdb_test_max_next_level_overlapping_bytes(t->db) <= 20 * 1048576);

  rdb_test_compact_range(t->db, 1, NULL, NULL);
  ASSERT(rdb_test_max_next_level_overlapping_bytes(t->db) <= 20 * 1048576);
}

static int
check_range(uint64_t val, uint64_t low, uint64_t high) {
  int result = (val >= low) && (val <= high);

  if (!result) {
    fprintf(stderr, "Value %.0f is not in range [%.0f, %.0f]\n",
                    (double)val, (double)low, (double)high);
  }

  return result;
}

#define ASSERT_RANGE(x, y, z) ASSERT(check_range(x, y, z))

static void
test_db_approximate_sizes(test_t *t) {
  static const int N = 80;
  static const int S1 = 100000;
  static const int S2 = 105000; /* Allow some expansion from metadata */
  int i, run, compact_start;

  do {
    rdb_dbopt_t options = test_current_options(t);
    rdb_rand_t rnd;

    options.write_buffer_size = 100000000; /* Large write buffer */
    options.compression = RDB_NO_COMPRESSION;

    test_destroy_and_reopen(t, 0);

    ASSERT_RANGE(test_size(t, "", "xyz"), 0, 0);

    test_reopen(t, &options);

    ASSERT_RANGE(test_size(t, "", "xyz"), 0, 0);

    /* Write 8MB (80 values, each 100K) */
    ASSERT(test_files_at_level(t, 0) == 0);

    rdb_rand_init(&rnd, 301);

    for (i = 0; i < N; i++)
      ASSERT(test_put(t, test_key(t, i), random_string(t, &rnd, S1)) == RDB_OK);

    /* 0 because GetApproximateSizes() does not account for memtable space */
    ASSERT_RANGE(test_size(t, "", test_key(t, 50)), 0, 0);

    if (options.reuse_logs) {
      /* Recovery will reuse memtable, and GetApproximateSizes() does not
         account for memtable usage; */
      test_reopen(t, &options);
      ASSERT_RANGE(test_size(t, "", test_key(t, 50)), 0, 0);
      continue;
    }

    /* Check sizes across recovery by reopening a few times */
    for (run = 0; run < 3; run++) {
      test_reopen(t, &options);

      for (compact_start = 0; compact_start < N; compact_start += 10) {
        rdb_slice_t cstart = rdb_string(test_key(t, compact_start));
        rdb_slice_t cend = rdb_string(test_key(t, compact_start + 9));

        for (i = 0; i < N; i += 10) {
          const char *k1 = test_key(t, i);
          const char *k2 = test_key2(t, i, ".suffix");
          const char *k3 = test_key(t, i + 10);

          ASSERT_RANGE(test_size(t, "", k1), S1 * i, S2 * i);
          ASSERT_RANGE(test_size(t, "", k2), S1 * (i + 1), S2 * (i + 1));
          ASSERT_RANGE(test_size(t, k1, k3), S1 * 10, S2 * 10);
        }

        {
          const char *k1 = test_key(t, 50);
          const char *k2 = test_key2(t, 50, ".suffix");

          ASSERT_RANGE(test_size(t, "", k1), S1 * 50, S2 * 50);
          ASSERT_RANGE(test_size(t, "", k2), S1 * 50, S2 * 50);
        }

        rdb_test_compact_range(t->db, 0, &cstart, &cend);
      }

      ASSERT(test_files_at_level(t, 0) == 0);
      ASSERT(test_files_at_level(t, 1) > 0);
    }
  } while (test_change_options(t));
}

static void
test_db_approximate_sizes_mix_of_small_and_large(test_t *t) {
  do {
    rdb_dbopt_t options = test_current_options(t);
    const char *big1;
    rdb_rand_t rnd;
    int run;

    options.compression = RDB_NO_COMPRESSION;

    test_reopen(t, 0);

    rdb_rand_init(&rnd, 301);

    big1 = random_string(t, &rnd, 100000);

    ASSERT(test_put(t, test_key(t, 0), random_string(t, &rnd, 10000)) == RDB_OK);
    ASSERT(test_put(t, test_key(t, 1), random_string(t, &rnd, 10000)) == RDB_OK);
    ASSERT(test_put(t, test_key(t, 2), big1) == RDB_OK);
    ASSERT(test_put(t, test_key(t, 3), random_string(t, &rnd, 10000)) == RDB_OK);
    ASSERT(test_put(t, test_key(t, 4), big1) == RDB_OK);
    ASSERT(test_put(t, test_key(t, 5), random_string(t, &rnd, 10000)) == RDB_OK);
    ASSERT(test_put(t, test_key(t, 6), random_string(t, &rnd, 300000)) == RDB_OK);
    ASSERT(test_put(t, test_key(t, 7), random_string(t, &rnd, 10000)) == RDB_OK);

    if (options.reuse_logs) {
      /* Need to force a memtable compaction since recovery does not do so. */
      ASSERT(rdb_test_compact_memtable(t->db) == RDB_OK);
    }

    /* Check sizes across recovery by reopening a few times */
    for (run = 0; run < 3; run++) {
      test_reopen(t, &options);

      ASSERT_RANGE(test_size(t, "", test_key(t, 0)), 0, 0);
      ASSERT_RANGE(test_size(t, "", test_key(t, 1)), 10000, 11000);
      ASSERT_RANGE(test_size(t, "", test_key(t, 2)), 20000, 21000);
      ASSERT_RANGE(test_size(t, "", test_key(t, 3)), 120000, 121000);
      ASSERT_RANGE(test_size(t, "", test_key(t, 4)), 130000, 131000);
      ASSERT_RANGE(test_size(t, "", test_key(t, 5)), 230000, 231000);
      ASSERT_RANGE(test_size(t, "", test_key(t, 6)), 240000, 241000);
      ASSERT_RANGE(test_size(t, "", test_key(t, 7)), 540000, 541000);
      ASSERT_RANGE(test_size(t, "", test_key(t, 8)), 550000, 560000);

      ASSERT_RANGE(test_size(t, test_key(t, 3), test_key(t, 5)), 110000,
                                                                 111000);

      rdb_test_compact_range(t->db, 0, NULL, NULL);
    }
  } while (test_change_options(t));
}

static void
test_db_iterator_pins_ref(test_t *t) {
  rdb_iter_t *iter;
  int i;

  test_put(t, "foo", "hello");

  /* Get iterator that will yield the current contents of the DB. */
  iter = rdb_iterator(t->db, 0);

  /* Write to force compactions */
  test_put(t, "foo", "newvalue1");

  for (i = 0; i < 100; i++) {
    const char *key = test_key(t, i);
    const char *val = string_fill2(t, key, 'v', 100000);

    ASSERT(test_put(t, key, val) == RDB_OK); /* 100K values */
  }

  test_put(t, "foo", "newvalue2");

  rdb_iter_seek_first(iter);

  ASSERT(rdb_iter_valid(iter));

  ASSERT_EQ(iter_status(t, iter), "foo->hello");

  rdb_iter_next(iter);

  ASSERT(!rdb_iter_valid(iter));

  rdb_iter_destroy(iter);
}

static void
test_db_snapshot(test_t *t) {
  const rdb_snapshot_t *s1, *s2, *s3;

  do {
    test_put(t, "foo", "v1");

    s1 = rdb_get_snapshot(t->db);

    test_put(t, "foo", "v2");

    s2 = rdb_get_snapshot(t->db);

    test_put(t, "foo", "v3");

    s3 = rdb_get_snapshot(t->db);

    test_put(t, "foo", "v4");

    ASSERT_EQ("v1", test_get2(t, "foo", s1));
    ASSERT_EQ("v2", test_get2(t, "foo", s2));
    ASSERT_EQ("v3", test_get2(t, "foo", s3));
    ASSERT_EQ("v4", test_get(t, "foo"));

    rdb_release_snapshot(t->db, s3);

    ASSERT_EQ("v1", test_get2(t, "foo", s1));
    ASSERT_EQ("v2", test_get2(t, "foo", s2));
    ASSERT_EQ("v4", test_get(t, "foo"));

    rdb_release_snapshot(t->db, s1);

    ASSERT_EQ("v2", test_get2(t, "foo", s2));
    ASSERT_EQ("v4", test_get(t, "foo"));

    rdb_release_snapshot(t->db, s2);

    ASSERT_EQ("v4", test_get(t, "foo"));
  } while (test_change_options(t));
}

static void
test_db_hidden_values_are_removed(test_t *t) {
  do {
    const rdb_snapshot_t *snapshot;
    rdb_slice_t x = rdb_string("x");
    const char *big;
    rdb_rand_t rnd;
    char *expect;

    rdb_rand_init(&rnd, 301);

    test_fill_levels(t, "a", "z");

    big = random_string(t, &rnd, 50000);

    test_put(t, "foo", big);
    test_put(t, "pastfoo", "v");

    snapshot = rdb_get_snapshot(t->db);

    test_put(t, "foo", "tiny");
    test_put(t, "pastfoo2", "v2"); /* Advance sequence number one more */

    ASSERT(rdb_test_compact_memtable(t->db) == RDB_OK);
    ASSERT(test_files_at_level(t, 0) > 0);

    ASSERT_EQ(big, test_get2(t, "foo", snapshot));
    ASSERT_RANGE(test_size(t, "", "pastfoo"), 50000, 60000);

    rdb_release_snapshot(t->db, snapshot);

    expect = rdb_malloc(50100);

    sprintf(expect, "[ tiny, %s ]", big);

    ASSERT_EQ(test_all_entries(t, "foo"), expect);

    rdb_free(expect);

    rdb_test_compact_range(t->db, 0, NULL, &x);

    ASSERT_EQ(test_all_entries(t, "foo"), "[ tiny ]");
    ASSERT(test_files_at_level(t, 0) == 0);
    ASSERT(test_files_at_level(t, 1) >= 1);

    rdb_test_compact_range(t->db, 1, NULL, &x);

    ASSERT_EQ(test_all_entries(t, "foo"), "[ tiny ]");

    ASSERT_RANGE(test_size(t, "", "pastfoo"), 0, 1000);
  } while (test_change_options(t));
}

static void
test_db_deletion_markers_1(test_t *t) {
  const int last = RDB_MAX_MEM_COMPACT_LEVEL;
  rdb_slice_t z = rdb_string("z");

  test_put(t, "foo", "v1");

  ASSERT(rdb_test_compact_memtable(t->db) == RDB_OK);
  ASSERT(test_files_at_level(t, last) == 1); /* foo => v1 is now in last level */

  /* Place a table at level last-1 to prevent merging with preceding mutation */
  test_put(t, "a", "begin");
  test_put(t, "z", "end");

  rdb_test_compact_memtable(t->db);

  ASSERT(test_files_at_level(t, last) == 1);
  ASSERT(test_files_at_level(t, last - 1) == 1);

  test_del(t, "foo");
  test_put(t, "foo", "v2");

  ASSERT_EQ(test_all_entries(t, "foo"), "[ v2, DEL, v1 ]");
  ASSERT(rdb_test_compact_memtable(t->db) == RDB_OK); /* Moves to level last-2 */
  ASSERT_EQ(test_all_entries(t, "foo"), "[ v2, DEL, v1 ]");

  rdb_test_compact_range(t->db, last - 2, NULL, &z);

  /* DEL eliminated, but v1 remains because we aren't compacting
     that level (DEL can be eliminated because v2 hides v1). */
  ASSERT_EQ(test_all_entries(t, "foo"), "[ v2, v1 ]");
  rdb_test_compact_range(t->db, last - 1, NULL, NULL);

  /* Merging last-1 w/ last, so we are the base level
     for "foo", so DEL is removed. (as is v1). */
  ASSERT_EQ(test_all_entries(t, "foo"), "[ v2 ]");
}

static void
test_db_deletion_markers_2(test_t *t) {
  const int last = RDB_MAX_MEM_COMPACT_LEVEL;

  test_put(t, "foo", "v1");

  ASSERT(rdb_test_compact_memtable(t->db) == RDB_OK);
  ASSERT(test_files_at_level(t, last) == 1); /* foo => v1 is now in last level */

  /* Place a table at level last-1 to prevent merging with preceding mutation */
  test_put(t, "a", "begin");
  test_put(t, "z", "end");

  rdb_test_compact_memtable(t->db);

  ASSERT(test_files_at_level(t, last) == 1);
  ASSERT(test_files_at_level(t, last - 1) == 1);

  test_del(t, "foo");

  ASSERT_EQ(test_all_entries(t, "foo"), "[ DEL, v1 ]");
  ASSERT(rdb_test_compact_memtable(t->db) == RDB_OK); /* Moves to level last == RDB_OK); */
  ASSERT_EQ(test_all_entries(t, "foo"), "[ DEL, v1 ]");

  rdb_test_compact_range(t->db, last - 2, NULL, NULL);

  /* DEL kept: "last" file overlaps */
  ASSERT_EQ(test_all_entries(t, "foo"), "[ DEL, v1 ]");
  rdb_test_compact_range(t->db, last - 1, NULL, NULL);

  /* Merging last-1 w/ last, so we are the base level
     for "foo", so DEL is removed. (as is v1). */
  ASSERT_EQ(test_all_entries(t, "foo"), "[ ]");
}

static void
test_db_overlap_in_level0(test_t *t) {
  do {
    ASSERT(RDB_MAX_MEM_COMPACT_LEVEL == 2);

    /* Fill levels 1 and 2 to disable the pushing
       of new memtables to levels > 0. */
    ASSERT(test_put(t, "100", "v100") == RDB_OK);
    ASSERT(test_put(t, "999", "v999") == RDB_OK);

    rdb_test_compact_memtable(t->db);

    ASSERT(test_del(t, "100") == RDB_OK);
    ASSERT(test_del(t, "999") == RDB_OK);

    rdb_test_compact_memtable(t->db);

    ASSERT_EQ("0,1,1", test_files_per_level(t));

    /* Make files spanning the following ranges in level-0:
     *
     *   files[0]  200 .. 900
     *   files[1]  300 .. 500
     *
     * Note that files are sorted by smallest key.
     */
    ASSERT(test_put(t, "300", "v300") == RDB_OK);
    ASSERT(test_put(t, "500", "v500") == RDB_OK);

    rdb_test_compact_memtable(t->db);

    ASSERT(test_put(t, "200", "v200") == RDB_OK);
    ASSERT(test_put(t, "600", "v600") == RDB_OK);
    ASSERT(test_put(t, "900", "v900") == RDB_OK);

    rdb_test_compact_memtable(t->db);

    ASSERT_EQ("2,1,1", test_files_per_level(t));

    /* Compact away the placeholder files we created initially */
    rdb_test_compact_range(t->db, 1, NULL, NULL);
    rdb_test_compact_range(t->db, 2, NULL, NULL);

    ASSERT_EQ("2", test_files_per_level(t));

    /* Do a memtable compaction. Before bug-fix, the compaction
       would not detect the overlap with level-0 files and would
       incorrectly place the deletion in a deeper level. */
    ASSERT(test_del(t, "600") == RDB_OK);

    rdb_test_compact_memtable(t->db);

    ASSERT_EQ("3", test_files_per_level(t));
    ASSERT_EQ("NOT_FOUND", test_get(t, "600"));
  } while (test_change_options(t));
}

static void
test_db_l0_compaction_bug_issue44_a(test_t *t) {
  test_reopen(t, 0);

  ASSERT(test_put(t, "b", "v") == RDB_OK);

  test_reopen(t, 0);

  ASSERT(test_del(t, "b") == RDB_OK);
  ASSERT(test_del(t, "a") == RDB_OK);

  test_reopen(t, 0);

  ASSERT(test_del(t, "a") == RDB_OK);

  test_reopen(t, 0);

  ASSERT(test_put(t, "a", "v") == RDB_OK);

  test_reopen(t, 0);
  test_reopen(t, 0);

  ASSERT_EQ("(a->v)", test_contents(t));

  rdb_sleep_msec(1000); /* Wait for compaction to finish */

  ASSERT_EQ("(a->v)", test_contents(t));
}

static void
test_db_l0_compaction_bug_issue44_b(test_t *t) {
  test_reopen(t, 0);
  test_put(t, "", "");
  test_reopen(t, 0);
  test_del(t, "e");
  test_put(t, "", "");
  test_reopen(t, 0);
  test_put(t, "c", "cv");
  test_reopen(t, 0);
  test_put(t, "", "");
  test_reopen(t, 0);
  test_put(t, "", "");

  rdb_sleep_msec(1000); /* Wait for compaction to finish */

  test_reopen(t, 0);
  test_put(t, "d", "dv");
  test_reopen(t, 0);
  test_put(t, "", "");
  test_reopen(t, 0);
  test_del(t, "d");
  test_del(t, "b");
  test_reopen(t, 0);

  ASSERT_EQ("(->)(c->cv)", test_contents(t));

  rdb_sleep_msec(1000); /* Wait for compaction to finish */

  ASSERT_EQ("(->)(c->cv)", test_contents(t));
}

static void
test_db_fflush_issue474(test_t *t) {
  static const int num = 100000;
  rdb_rand_t rnd;
  int i;

  rdb_rand_init(&rnd, rdb_random_seed());

  for (i = 0; i < num; i++) {
    const char *key = random_key(t, &rnd);
    const char *val = random_string(t, &rnd, 100);

    fflush(NULL);

    ASSERT(test_put(t, key, val) == RDB_OK);
  }
}

static void
test_db_comparator_check(test_t *t) {
  rdb_comparator_t comparator = *rdb_bytewise_comparator;
  rdb_dbopt_t options = test_current_options(t);

  comparator.name = "leveldb.NewComparator";

  options.comparator = &comparator;

  ASSERT(test_try_reopen(t, &options) != RDB_OK);
}

static int
test_to_number(const rdb_slice_t *x) {
  /* Check that there are no extra characters. */
  char xp[100];
  char ignored;
  int val;

  ASSERT(x->size < sizeof(xp));
  ASSERT(x->size >= 2 && x->data[0] == '[' && x->data[x->size - 1] == ']');

  memcpy(xp, x->data, x->size);

  xp[x->size] = '\0';

  ASSERT(sscanf(xp, "[%i]%c", &val, &ignored) == 1);

  return val;
}

static int
slice_compare(const rdb_comparator_t *comparator,
              const rdb_slice_t *x,
              const rdb_slice_t *y) {
  (void)comparator;
  return test_to_number(x) - test_to_number(y);
}

static void
shortest_separator(const rdb_comparator_t *comparator,
                   rdb_buffer_t *start,
                   const rdb_slice_t *limit) {
  (void)comparator;
  test_to_number(start); /* Check format */
  test_to_number(limit); /* Check format */
}

static void
short_successor(const rdb_comparator_t *comparator, rdb_buffer_t *key) {
  (void)comparator;
  test_to_number(key); /* Check format */
}

static void
test_db_custom_comparator(test_t *t) {
  static const rdb_comparator_t comparator = {
    /* .name = */ "test.NumberComparator",
    /* .compare = */ slice_compare,
    /* .shortest_separator = */ shortest_separator,
    /* .short_successor = */ short_successor,
    /* .user_comparator = */ NULL,
    /* .state = */ NULL
  };

  rdb_dbopt_t options = test_current_options(t);
  int i, run;

  options.create_if_missing = 1;
  options.comparator = &comparator;
  options.filter_policy = NULL;     /* Cannot use bloom filters */
  options.write_buffer_size = 1000; /* Compact more often */

  test_destroy_and_reopen(t, &options);

  ASSERT(test_put(t, "[10]", "ten") == RDB_OK);
  ASSERT(test_put(t, "[0x14]", "twenty") == RDB_OK);

  for (i = 0; i < 2; i++) {
    ASSERT_EQ("ten", test_get(t, "[10]"));
    ASSERT_EQ("ten", test_get(t, "[0xa]"));
    ASSERT_EQ("twenty", test_get(t, "[20]"));
    ASSERT_EQ("twenty", test_get(t, "[0x14]"));
    ASSERT_EQ("NOT_FOUND", test_get(t, "[15]"));
    ASSERT_EQ("NOT_FOUND", test_get(t, "[0xf]"));

    test_compact(t, "[0]", "[9999]");
  }

  for (run = 0; run < 2; run++) {
    for (i = 0; i < 1000; i++) {
      char buf[100];
      sprintf(buf, "[%d]", i * 10);
      ASSERT(test_put(t, buf, buf) == RDB_OK);
    }

    test_compact(t, "[0]", "[1000000]");
  }
}

static void
test_db_manual_compaction(test_t *t) {
  ASSERT(RDB_MAX_MEM_COMPACT_LEVEL == 2);

  test_make_tables(t, 3, "p", "q");
  ASSERT_EQ("1,1,1", test_files_per_level(t));

  /* Compaction range falls before files */
  test_compact(t, "", "c");
  ASSERT_EQ("1,1,1", test_files_per_level(t));

  /* Compaction range falls after files */
  test_compact(t, "r", "z");
  ASSERT_EQ("1,1,1", test_files_per_level(t));

  /* Compaction range overlaps files */
  test_compact(t, "p1", "p9");
  ASSERT_EQ("0,0,1", test_files_per_level(t));

  /* Populate a different range */
  test_make_tables(t, 3, "c", "e");
  ASSERT_EQ("1,1,2", test_files_per_level(t));

  /* Compact just the new range */
  test_compact(t, "b", "f");
  ASSERT_EQ("0,0,2", test_files_per_level(t));

  /* Compact all */
  test_make_tables(t, 1, "a", "z");
  ASSERT_EQ("0,1,2", test_files_per_level(t));

  rdb_compact_range(t->db, NULL, NULL);
  ASSERT_EQ("0,0,1", test_files_per_level(t));
}

static void
test_db_open_options(test_t *t) {
  rdb_dbopt_t opts = *rdb_dbopt_default;
  char dbname[RDB_PATH_MAX];
  rdb_t *db = NULL;

  (void)t;

  ASSERT(rdb_test_filename(dbname, sizeof(dbname), "db_options_test"));

  rdb_destroy_db(dbname, 0);

  /* Does not exist, and create_if_missing == 0: error */
  opts.create_if_missing = 0;

  ASSERT(rdb_open(dbname, &opts, &db) == RDB_INVALID);
  ASSERT(db == NULL);

  /* Does not exist, and create_if_missing == 1: OK */
  opts.create_if_missing = 1;

  ASSERT(rdb_open(dbname, &opts, &db) == RDB_OK);
  ASSERT(db != NULL);

  rdb_close(db);
  db = NULL;

  /* Does exist, and error_if_exists == 1: error */
  opts.create_if_missing = 0;
  opts.error_if_exists = 1;

  ASSERT(rdb_open(dbname, &opts, &db) == RDB_INVALID);
  ASSERT(db == NULL);

  /* Does exist, and error_if_exists == 0: OK */
  opts.create_if_missing = 1;
  opts.error_if_exists = 0;

  ASSERT(rdb_open(dbname, &opts, &db) == RDB_OK);
  ASSERT(db != NULL);

  rdb_close(db);

  rdb_destroy_db(dbname, 0);
}

static void
test_db_destroy_empty_dir(test_t *t) {
  rdb_dbopt_t opts = *rdb_dbopt_default;
  char dbname[RDB_PATH_MAX];
  char **names;
  int len;

  (void)t;

  ASSERT(rdb_test_filename(dbname, sizeof(dbname), "db_empty_dir"));

  rdb_remove_dir(dbname);

  ASSERT(!rdb_file_exists(dbname));
  ASSERT(rdb_create_dir(dbname) == RDB_OK);
#ifndef RDB_MEMENV
  ASSERT(rdb_file_exists(dbname));
#endif
  ASSERT((len = rdb_get_children(dbname, &names)) >= 0);
  ASSERT(0 == len);
  ASSERT(rdb_destroy_db(dbname, &opts) == RDB_OK);
  ASSERT(!rdb_file_exists(dbname));

  rdb_free_children(names, len);
}

static void
test_db_destroy_open_db(test_t *t) {
  rdb_dbopt_t opts = *rdb_dbopt_default;
  char dbname[RDB_PATH_MAX];
  rdb_t *db = NULL;

  (void)t;

  ASSERT(rdb_test_filename(dbname, sizeof(dbname), "open_db_dir"));

  /* rdb_remove_dir(dbname); */
  rdb_destroy_db(dbname, 0);

  ASSERT(!rdb_file_exists(dbname));

  opts.create_if_missing = 1;

  ASSERT(rdb_open(dbname, &opts, &db) == RDB_OK);
  ASSERT(db != NULL);

  /* Must fail to destroy an open db. */
#ifndef RDB_MEMENV
  ASSERT(rdb_file_exists(dbname));
#endif
  ASSERT(rdb_destroy_db(dbname, 0) != RDB_OK);
#ifndef RDB_MEMENV
  ASSERT(rdb_file_exists(dbname));
#endif

  rdb_close(db);

  /* Should succeed destroying a closed db. */
  ASSERT(rdb_destroy_db(dbname, 0) == RDB_OK);
  ASSERT(!rdb_file_exists(dbname));
}

static void
test_db_locking(test_t *t) {
  rdb_dbopt_t opt = test_current_options(t);
  rdb_t *db = NULL;

  ASSERT(rdb_open(t->dbname, &opt, &db) != RDB_OK);
}

#if 0
/* Check that number of files does not grow when we are out of space */
static void
test_db_no_space(test_t *t) {
  rdb_dbopt_t options = test_current_options(t);
  int i, level, num_files;

  test_reopen(t, &options);

  ASSERT(test_put(t, "foo", "v1") == RDB_OK);
  ASSERT_EQ("v1", test_get(t, "foo"));

  test_compact(t, "a", "z");

  num_files = test_count_files(t);

  /* Force out-of-space errors. */
  rdb_atomic_store(&env->no_space, 1, rdb_order_release);

  for (i = 0; i < 10; i++) {
    for (level = 0; level < RDB_NUM_LEVELS - 1; level++)
      rdb_test_compact_range(t->db, level, NULL, NULL);
  }

  rdb_atomic_store(&env->no_space, 0, rdb_order_release);

  ASSERT(test_count_files(t) < num_files + 3);
}

static void
test_db_non_writable_filesystem(test_t *t) {
  rdb_dbopt_t options = test_current_options(t);
  const char *big;
  int i, errors;

  options.write_buffer_size = 1000;

  test_reopen(t, &options);

  ASSERT(test_put(t, "foo", "v1") == RDB_OK);

  /* Force errors for new files. */
  rdb_atomic_store(&env->non_writable, 1, rdb_order_release);

  big = string_fill(t, 'x', 100000);
  errors = 0;

  for (i = 0; i < 20; i++) {
    fprintf(stderr, "iter %d; errors %d\n", i, errors);

    if (test_put(t, "foo", big) != RDB_OK) {
      errors++;
      rdb_sleep_msec(100);
    }
  }

  ASSERT(errors > 0);

  rdb_atomic_store(&env->non_writable, 0, rdb_order_release);
}

static void
test_db_write_sync_error(test_t *t) {
  /* Check that log sync errors cause the DB to disallow future writes. */
  rdb_dbopt_t options = test_current_options(t);
  rdb_writeopt_t w = *rdb_writeopt_default;
  rdb_slice_t k1 = rdb_string("k1");
  rdb_slice_t k2 = rdb_string("k2");
  rdb_slice_t k3 = rdb_string("k3");
  rdb_slice_t v1 = rdb_string("v1");
  rdb_slice_t v2 = rdb_string("v2");
  rdb_slice_t v3 = rdb_string("v3");

  /* (a) Cause log sync calls to fail */
  test_reopen(t, &options);
  rdb_atomic_store(&env->data_sync_error, 1, rdb_order_release);

  /* (b) Normal write should succeed */
  w.sync = 0;

  ASSERT(rdb_put(t->db, &k1, &v1, &w) == RDB_OK);
  ASSERT_EQ("v1", test_get(t, "k1"));

  /* (c) Do a sync write; should fail */
  w.sync = 1;

  ASSERT(rdb_put(t->db, &k2, &v2, &w) != RDB_OK);
  ASSERT_EQ("v1", test_get(t, "k1"));
  ASSERT_EQ("NOT_FOUND", test_get(t, "k2"));

  /* (d) make sync behave normally */
  rdb_atomic_store(&env->data_sync_error, 0, rdb_order_release);

  /* (e) Do a non-sync write; should fail */
  w.sync = 0;

  ASSERT(rdb_put(t->db, &k3, &v3, &w) != RDB_OK);
  ASSERT_EQ("v1", test_get(t, "k1"));
  ASSERT_EQ("NOT_FOUND", test_get(t, "k2"));
  ASSERT_EQ("NOT_FOUND", test_get(t, "k3"));
}

static void
test_db_manifest_write_error(test_t *t) {
  /* Test for the following problem:
   *
   *   (a) Compaction produces file F
   *   (b) Log record containing F is written to MANIFEST file, but Sync() fails
   *   (c) GC deletes F
   *   (d) After reopening DB, reads fail since deleted F is named in log record
   */
  const int last = RDB_MAX_MEM_COMPACT_LEVEL;
  int iter;

  /* We iterate twice. In the second iteration, everything is the
     same except the log record never makes it to the MANIFEST file. */
  for (iter = 0; iter < 2; iter++) {
    rdb_atomic(int) *error_type = (iter == 0) ? &env->manifest_sync_error
                                              : &env->manifest_write_error;

    /* Insert foo=>bar mapping */
    rdb_dbopt_t options = test_current_options(t);

    options.create_if_missing = 1;
    options.error_if_exists = 0;

    test_destroy_and_reopen(t, &options);

    ASSERT(test_put(t, "foo", "bar") == RDB_OK);
    ASSERT_EQ("bar", test_get(t, "foo"));

    /* Memtable compaction (will succeed) */
    rdb_test_compact_memtable(t->db);

    ASSERT_EQ("bar", test_get(t, "foo"));
    ASSERT(test_files_at_level(t, last) == 1); /* foo=>bar is now in last level */

    /* Merging compaction (will fail) */
    rdb_atomic_store(error_type, 1, rdb_order_release);
    rdb_test_compact_range(t->db, last, NULL, NULL); /* Should fail */

    ASSERT_EQ("bar", test_get(t, "foo"));

    /* Recovery: should not lose data */
    rdb_atomic_store(error_type, 0, rdb_order_release);
    test_reopen(t, &options);

    ASSERT_EQ("bar", test_get(t, "foo"));
  }
}
#endif

static void
test_db_missing_sst_file(test_t *t) {
  rdb_dbopt_t options;

  ASSERT(test_put(t, "foo", "bar") == RDB_OK);
  ASSERT_EQ("bar", test_get(t, "foo"));

  /* Dump the memtable to disk. */
  rdb_test_compact_memtable(t->db);

  ASSERT_EQ("bar", test_get(t, "foo"));

  test_close(t);

  ASSERT(test_delete_an_sst_file(t));

  options = test_current_options(t);
  options.paranoid_checks = 1;

  ASSERT(test_try_reopen(t, &options) != RDB_OK);
}

static void
test_db_still_read_sst(test_t *t) {
  rdb_dbopt_t options;

  ASSERT(test_put(t, "foo", "bar") == RDB_OK);
  ASSERT_EQ("bar", test_get(t, "foo"));

  /* Dump the memtable to disk. */
  rdb_test_compact_memtable(t->db);

  ASSERT_EQ("bar", test_get(t, "foo"));

  test_close(t);

  ASSERT(test_rename_ldb_to_sst(t) > 0);

  options = test_current_options(t);
  options.paranoid_checks = 1;

  ASSERT(test_try_reopen(t, &options) == RDB_OK);

  ASSERT_EQ("bar", test_get(t, "foo"));
}

static void
test_db_files_deleted_after_compaction(test_t *t) {
  int i, num_files;

  ASSERT(test_put(t, "foo", "v2") == RDB_OK);

  test_compact(t, "a", "z");

  num_files = test_count_files(t);

  for (i = 0; i < 10; i++) {
    ASSERT(test_put(t, "foo", "v2") == RDB_OK);
    test_compact(t, "a", "z");
  }

  ASSERT(test_count_files(t) == num_files);
}

static void
test_db_bloom_filter(test_t *t) {
  rdb_dbopt_t options = test_current_options(t);
  const int N = 10000;
  int i, reads;

  /* env->count_random_reads = 1; */

  options.block_cache = rdb_lru_create(0); /* Prevent cache hits */
  options.filter_policy = rdb_bloom_create(10);

  test_reopen(t, &options);

  /* Populate multiple layers */
  for (i = 0; i < N; i++)
    ASSERT(test_put(t, test_key(t, i), test_key(t, i)) == RDB_OK);

  test_compact(t, "a", "z");

  for (i = 0; i < N; i += 100)
    ASSERT(test_put(t, test_key(t, i), test_key(t, i)) == RDB_OK);

  rdb_test_compact_memtable(t->db);

  /* Prevent auto compactions triggered by seeks */
  /* rdb_atomic_store(&env->delay_data_sync, 1, rdb_order_release); */

  /* Lookup present keys. Should rarely read from small sstable. */
  /* atom_reset(&env->random_read_counter); */

  for (i = 0; i < N; i++)
    ASSERT_EQ(test_key(t, i), test_get(t, test_key(t, i)));

  /* reads = atom_read(&env->random_read_counter); */
  reads = N;

  fprintf(stderr, "%d present => %d reads\n", N, reads);

  ASSERT(reads >= N);
  ASSERT(reads <= N + 2 * N / 100);

  /* Lookup present keys. Should rarely read from either sstable. */
  /* atom_reset(&env->random_read_counter); */

  for (i = 0; i < N; i++)
    ASSERT_EQ("NOT_FOUND", test_get(t, test_key2(t, i, ".missing")));

  /* reads = atom_read(&env->random_read_counter); */
  reads = 3 * N / 100;

  fprintf(stderr, "%d missing => %d reads\n", N, reads);

  ASSERT(reads <= 3 * N / 100);

  /* rdb_atomic_store(&env->delay_data_sync, 0, rdb_order_release); */

  test_close(t);

  rdb_lru_destroy(options.block_cache);
  rdb_bloom_destroy((rdb_bloom_t *)options.filter_policy);
}

/*
 * Multi-threaded Testing
 */

#define NUM_THREADS 4
#define TEST_SECONDS 10
#define NUM_KEYS 1000

typedef struct mt_state {
  test_t *test;
  rdb_atomic(int) stop;
  rdb_atomic(int) counter[NUM_THREADS];
  rdb_atomic(int) thread_done[NUM_THREADS];
} mt_state_t;

typedef struct mt_thread {
  mt_state_t *state;
  int id;
} mt_thread_t;

static void
mt_thread_body(void *arg) {
  mt_thread_t *ctx = (mt_thread_t *)arg;
  mt_state_t *state = ctx->state;
  rdb_t *db = state->test->db;
  rdb_slice_t key, val;
  int id = ctx->id;
  int counter = 0;
  char vbuf[1500];
  rdb_rand_t rnd;
  char kbuf[20];

  rdb_rand_init(&rnd, 1000 + id);

  fprintf(stderr, "... starting thread %d\n", id);

  while (!rdb_atomic_load(&state->stop, rdb_order_acquire)) {
    int num = rdb_rand_uniform(&rnd, NUM_KEYS);

    sprintf(kbuf, "%016d", num);

    key = rdb_string(kbuf);

    rdb_atomic_store(&state->counter[id], counter, rdb_order_release);

    if (rdb_rand_one_in(&rnd, 2)) {
      /* Write values of the form <key, my id, counter>. */
      /* We add some padding for force compactions. */
      sprintf(vbuf, "%d.%d.%-1000d", num, id, counter);

      val = rdb_string(vbuf);

      ASSERT(rdb_put(db, &key, &val, 0) == RDB_OK);
    } else {
      /* Read a value and verify that it matches the pattern written above. */
      int rc = rdb_get(db, &key, &val, 0);
      int n, w, c;

      if (rc == RDB_NOTFOUND) {
        /* Key has not yet been written */
      } else {
        ASSERT(rc == RDB_OK);
        ASSERT(val.size < sizeof(vbuf));

        memcpy(vbuf, val.data, val.size);

        vbuf[val.size] = '\0';

        rdb_free(val.data);

        /* Check that the writer thread counter
           is >= the counter in the value */
        ASSERT(3 == sscanf(vbuf, "%d.%d.%d", &n, &w, &c));
        ASSERT(n == num);
        ASSERT(w >= 0);
        ASSERT(w < NUM_THREADS);
        ASSERT(c <= rdb_atomic_load(&state->counter[w], rdb_order_acquire));
      }
    }

    counter++;
  }

  rdb_atomic_store(&state->thread_done[id], 1, rdb_order_release);

  fprintf(stderr, "... stopping thread %d after %d ops\n", id, counter);
}

static void
test_db_multi_threaded(test_t *t) {
  do {
    mt_thread_t contexts[NUM_THREADS];
    mt_state_t state;
    int id;

    /* Initialize state */
    state.test = t;

    rdb_atomic_store(&state.stop, 0, rdb_order_release);

    for (id = 0; id < NUM_THREADS; id++) {
      rdb_atomic_store(&state.counter[id], 0, rdb_order_release);
      rdb_atomic_store(&state.thread_done[id], 0, rdb_order_release);
    }

    /* Start threads */
    for (id = 0; id < NUM_THREADS; id++) {
      rdb_thread_t thread;

      contexts[id].state = &state;
      contexts[id].id = id;

      rdb_thread_create(&thread, mt_thread_body, &contexts[id]);
      rdb_thread_detach(&thread);
    }

    /* Let them run for a while */
    rdb_sleep_msec(TEST_SECONDS * 1000);

    /* Stop the threads and wait for them to finish */
    rdb_atomic_store(&state.stop, 1, rdb_order_release);

    for (id = 0; id < NUM_THREADS; id++) {
      while (!rdb_atomic_load(&state.thread_done[id], rdb_order_acquire))
        rdb_sleep_msec(100);
    }
  } while (test_change_options(t));
}

/*
 * Randomized Testing
 */

static int
map_compare(rb_val_t x, rb_val_t y, void *arg) {
  (void)arg;
  return strcmp(x.p, y.p);
}

static void
map_put(rb_map_t *map, const char *k, const char *v) {
  rb_val_t key, val;
  rb_node_t *node;

  key.p = (void *)k;
  val.p = (void *)v;

  node = rb_tree_put(map, key, val);

  if (node != NULL)
    node->value.p = (void *)v;
}

static void
map_del(rb_map_t *map, const char *k) {
  rb_node_t *node;
  rb_val_t key;

  key.p = (void *)k;

  node = rb_tree_del(map, key);

  if (node != NULL)
    rb_node_destroy(node);
}

static void
batch_put(rdb_batch_t *b, const char *k, const char *v) {
  rdb_slice_t key = rdb_string(k);
  rdb_slice_t val = rdb_string(v);

  rdb_batch_put(b, &key, &val);
}

static void
batch_del(rdb_batch_t *b, const char *k) {
  rdb_slice_t key = rdb_string(k);

  rdb_batch_del(b, &key);
}

static void
check_get(rdb_t *db, const rdb_snapshot_t *db_snap,
          rb_map_t *map, rb_map_t *map_snap) {
  rdb_readopt_t opt = *rdb_readopt_default;
  rb_iter_t it;

  opt.snapshot = db_snap;

  if (map_snap == NULL)
    map_snap = map;

  it = rb_tree_iterator(map_snap);

  rb_iter_seek_first(&it);

  while (rb_iter_valid(&it)) {
    const char *k = rb_iter_key(&it).p;
    const char *v = rb_iter_value(&it).p;
    rdb_slice_t key = rdb_string(k);
    rdb_slice_t val;

    ASSERT(rdb_get(db, &key, &val, &opt) == RDB_OK);
    ASSERT(val.size == strlen(v));
    ASSERT(memcmp(val.data, v, val.size) == 0);

    rdb_free(val.data);

    rb_iter_next(&it);
  }
}

static int
iter_equal(rdb_iter_t *iter, rb_iter_t *it) {
  rdb_slice_t k1, v1;
  rb_val_t k2, v2;

  if (!rdb_iter_valid(iter))
    return !rb_iter_valid(it);

  if (!rb_iter_valid(it))
    return !rdb_iter_valid(iter);

  k1 = rdb_iter_key(iter);
  v1 = rdb_iter_value(iter);

  k2 = rb_iter_key(it);
  v2 = rb_iter_value(it);

  if (k1.size != strlen(k2.p))
    return 0;

  if (v1.size != strlen(v2.p))
    return 0;

  return memcmp(k1.data, k2.p, k1.size) == 0
      && memcmp(v1.data, v2.p, v1.size) == 0;
}

static void
check_iter(rdb_t *db, const rdb_snapshot_t *db_snap,
           rb_map_t *map, rb_map_t *map_snap) {
  rdb_readopt_t opt = *rdb_readopt_default;
  rdb_vector_t keys;
  rdb_iter_t *iter;
  int count = 0;
  rb_iter_t it;
  size_t i;

  rdb_vector_init(&keys);

  opt.snapshot = db_snap;

  if (map_snap == NULL)
    map_snap = map;

  iter = rdb_iterator(db, &opt);
  it = rb_tree_iterator(map_snap);

  ASSERT(!rdb_iter_valid(iter));

  rdb_iter_seek_first(iter);
  rb_iter_seek_first(&it);

  while (rb_iter_valid(&it)) {
    ASSERT(rdb_iter_valid(iter));
    ASSERT(iter_equal(iter, &it));

    if ((++count % 10) == 0)
      rdb_vector_push(&keys, rb_iter_key(&it).p);

    rdb_iter_next(iter);
    rb_iter_next(&it);
  }

  ASSERT(!rdb_iter_valid(iter));

  for (i = 0; i < keys.length; i++) {
    rdb_slice_t key = rdb_string(keys.items[i]);
    rb_val_t k;

    k.p = key.data;

    rdb_iter_seek(iter, &key);
    rb_iter_seek(&it, k);

    ASSERT(iter_equal(iter, &it));
  }

  rdb_vector_clear(&keys);
  rdb_iter_destroy(iter);
}

static void
test_db_randomized(test_t *t) {
  const int N = 10000;
  rdb_rand_t rnd;

  rdb_rand_init(&rnd, rdb_random_seed());

  do {
    const rdb_snapshot_t *db_snap = NULL;
    rb_map_t *map_snap = NULL;
    int i, p, step, num;
    rb_map_t map, tmp;
    const char *k, *v;
    rdb_batch_t b;

    rb_map_init(&map, map_compare, NULL);
    rb_map_init(&tmp, map_compare, NULL);

    for (step = 0; step < N; step++) {
      if (step % 100 == 0)
        fprintf(stderr, "Step %d of %d\n", step, N);

      p = rdb_rand_uniform(&rnd, 100);

      if (p < 45) { /* Put */
        k = random_key(t, &rnd);
        v = random_string(t, &rnd, rdb_rand_one_in(&rnd, 20)
                                 ? 100 + rdb_rand_uniform(&rnd, 100)
                                 : rdb_rand_uniform(&rnd, 8));

        map_put(&map, k, v);

        ASSERT(test_put(t, k, v) == RDB_OK);
        ASSERT(test_has(t, k));
      } else if (p < 90) { /* Delete */
        k = random_key(t, &rnd);

        map_del(&map, k);

        ASSERT(test_del(t, k) == RDB_OK);
        ASSERT(!test_has(t, k));
      } else { /* Multi-element batch */
        rdb_batch_init(&b);

        num = rdb_rand_uniform(&rnd, 8);

        for (i = 0; i < num; i++) {
          if (i == 0 || !rdb_rand_one_in(&rnd, 10)) {
            k = random_key(t, &rnd);
          } else {
            /* Periodically re-use the same key from the previous iter, so
               we have multiple entries in the write batch for the same key */
          }

          if (rdb_rand_one_in(&rnd, 2)) {
            v = random_string(t, &rnd, rdb_rand_uniform(&rnd, 10));
            batch_put(&b, k, v);
            map_put(&map, k, v);
          } else {
            batch_del(&b, k);
            map_del(&map, k);
          }
        }

        ASSERT(rdb_write(t->db, &b, 0) == RDB_OK);

        rdb_batch_clear(&b);
      }

      if ((step % 100) == 0) {
        check_get(t->db, NULL, &map, NULL);
        check_get(t->db, db_snap, &map, map_snap);

        check_iter(t->db, NULL, &map, NULL);
        check_iter(t->db, db_snap, &map, map_snap);

        /* Save a snapshot from each DB this time that we'll use next
           time we compare things, to make sure the current state is
           preserved with the snapshot */
        if (map_snap != NULL)
          rb_map_clear(map_snap, NULL);

        if (db_snap != NULL)
          rdb_release_snapshot(t->db, db_snap);

        test_reopen(t, 0);

        check_get(t->db, NULL, &map, NULL);
        check_iter(t->db, NULL, &map, NULL);

        rb_map_copy(&tmp, &map, NULL);

        map_snap = &tmp;
        db_snap = rdb_get_snapshot(t->db);
      }
    }

    if (map_snap != NULL)
      rb_map_clear(map_snap, NULL);

    if (db_snap != NULL)
      rdb_release_snapshot(t->db, db_snap);

    rb_map_clear(&map, NULL);
    rb_map_clear(&tmp, NULL);
  } while (test_change_options(t));
}

/*
 * Execute
 */

RDB_EXTERN int
rdb_test_db(void);

int
rdb_test_db(void) {
  static void (*tests[])(test_t *) = {
    test_db_empty,
    test_db_empty_key,
    test_db_empty_value,
    test_db_read_write,
    test_db_put_delete_get,
    test_db_get_from_immutable_layer,
    test_db_get_from_versions,
    test_db_get_memusage,
    test_db_get_snapshot,
    test_db_get_identical_snapshots,
    test_db_iterate_over_empty_snapshot,
    test_db_get_level0_ordering,
    test_db_get_ordered_by_levels,
    test_db_get_picks_correct_file,
    test_db_get_encounters_empty_level,
    test_db_iter_empty,
    test_db_iter_single,
    test_db_iter_multi,
    test_db_iter_small_and_large_mix,
    test_db_iter_multi_with_delete,
    test_db_iter_multi_with_delete_and_compaction,
    test_db_recover,
    test_db_recover_with_empty_log,
    test_db_recover_during_memtable_compaction,
    test_db_minor_compactions_happen,
    test_db_recover_with_large_log,
    test_db_compactions_generate_multiple_files,
    test_db_repeated_writes_to_same_key,
    test_db_sparse_merge,
    test_db_approximate_sizes,
    test_db_approximate_sizes_mix_of_small_and_large,
    test_db_iterator_pins_ref,
    test_db_snapshot,
    test_db_hidden_values_are_removed,
    test_db_deletion_markers_1,
    test_db_deletion_markers_2,
    test_db_overlap_in_level0,
    test_db_l0_compaction_bug_issue44_a,
    test_db_l0_compaction_bug_issue44_b,
    test_db_fflush_issue474,
    test_db_comparator_check,
    test_db_custom_comparator,
    test_db_manual_compaction,
    test_db_open_options,
    test_db_destroy_empty_dir,
    test_db_destroy_open_db,
    test_db_locking,
#if 0
    test_db_no_space,
    test_db_non_writable_filesystem,
    test_db_write_sync_error,
    test_db_manifest_write_error,
#endif
    test_db_missing_sst_file,
    test_db_still_read_sst,
    test_db_files_deleted_after_compaction,
    test_db_bloom_filter,
    test_db_multi_threaded,
    test_db_randomized
  };

  size_t i;

  for (i = 0; i < lengthof(tests); i++) {
    test_t t;

    test_init(&t);

    tests[i](&t);

    test_clear(&t);
  }

  return 0;
}
