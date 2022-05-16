/*!
 * t-db.c - db test for lcdb
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

#include "util/atomic.h"
#include "util/bloom.h"
#include "util/buffer.h"
#include "util/cache.h"
#include "util/comparator.h"
#include "util/env.h"
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
ldb_sleep_msec(int64_t ms) {
  ldb_sleep_usec(ms * 1000);
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
  char dbname[LDB_PATH_MAX];
  ldb_dbopt_t last_options;
  int config;
  ldb_bloom_t *policy;
  ldb_t *db;
  ldb_vector_t arena;
} test_t;

static void
test_reopen(test_t *t, const ldb_dbopt_t *options);

static void
test_destroy_and_reopen(test_t *t, const ldb_dbopt_t *options);

static void
test_init(test_t *t) {
  ASSERT(ldb_test_filename(t->dbname, sizeof(t->dbname), "db_test"));

  t->last_options = *ldb_dbopt_default;
  t->last_options.comparator = ldb_bytewise_comparator;

  t->config = CONFIG_DEFAULT;
  t->policy = ldb_bloom_create(10);
  t->db = NULL;

  ldb_vector_init(&t->arena);

  ldb_destroy(t->dbname, 0);

  test_reopen(t, 0);
}

static void
test_clear(test_t *t) {
  size_t i;

  if (t->db != NULL)
    ldb_close(t->db);

  ldb_destroy(t->dbname, 0);
  ldb_bloom_destroy(t->policy);

  for (i = 0; i < t->arena.length; i++)
    ldb_free(t->arena.items[i]);

  ldb_vector_clear(&t->arena);
}

static void
test_reset(test_t *t) {
  size_t i;

  for (i = 0; i < t->arena.length; i++)
    ldb_free(t->arena.items[i]);

  ldb_vector_reset(&t->arena);
}

static const char *
test_key(test_t *t, int i) {
  char *zp = ldb_malloc(15);

  sprintf(zp, "key%06d", i);

  ldb_vector_push(&t->arena, zp);

  return zp;
}

static const char *
test_key2(test_t *t, int i, const char *suffix) {
  char *zp = ldb_malloc(15 + 8);

  ASSERT(strlen(suffix) <= 8);

  sprintf(zp, "key%06d%s", i, suffix);

  ldb_vector_push(&t->arena, zp);

  return zp;
}

static const char *
random_key(test_t *t, ldb_rand_t *rnd) {
  int len = (ldb_rand_one_in(rnd, 3)
    ? 1 /* Short sometimes to encourage collisions. */
    : (ldb_rand_one_in(rnd, 100)
      ? ldb_rand_skewed(rnd, 10)
      : ldb_rand_uniform(rnd, 10)));

  ldb_buffer_t z;

  ldb_buffer_init(&z);
  ldb_random_key(&z, rnd, len);

  ldb_vector_push(&t->arena, z.data);

  return (char *)z.data;
}

static const char *
random_string(test_t *t, ldb_rand_t *rnd, size_t len) {
  ldb_buffer_t z;

  ldb_buffer_init(&z);
  ldb_random_string(&z, rnd, len);

  ldb_vector_push(&t->arena, z.data);

  return (char *)z.data;
}

static const char *
string_fill(test_t *t, int ch, size_t len) {
  char *zp = ldb_malloc(len + 1);

  memset(zp, ch, len);

  zp[len] = '\0';

  ldb_vector_push(&t->arena, zp);

  return zp;
}

static const char *
string_fill2(test_t *t, const char *prefix, int ch, size_t len) {
  size_t plen = strlen(prefix);
  char *buf = ldb_malloc(plen + len + 1);
  char *zp = buf;

  memcpy(zp, prefix, plen);
  zp += plen;

  memset(zp, ch, len);
  zp += len;

  *zp = '\0';

  ldb_vector_push(&t->arena, buf);

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
static ldb_dbopt_t
test_current_options(test_t *t) {
  ldb_dbopt_t options = *ldb_dbopt_default;

  options.comparator = ldb_bytewise_comparator;
  options.reuse_logs = 0;

  switch (t->config) {
    case CONFIG_REUSE:
      options.reuse_logs = 1;
      break;
    case CONFIG_FILTER:
      options.filter_policy = t->policy;
      break;
    case CONFIG_UNCOMPRESSED:
      options.compression = LDB_NO_COMPRESSION;
      break;
    default:
      break;
  }

  return options;
}

static int
test_try_reopen(test_t *t, const ldb_dbopt_t *options) {
  ldb_dbopt_t opts;

  if (t->db != NULL)
    ldb_close(t->db);

  t->db = NULL;

  if (options != NULL) {
    opts = *options;
  } else {
    opts = test_current_options(t);
    opts.create_if_missing = 1;
  }

  t->last_options = opts;

  if (t->last_options.comparator == NULL)
    t->last_options.comparator = ldb_bytewise_comparator;

  return ldb_open(t->dbname, &opts, &t->db);
}

static void
test_reopen(test_t *t, const ldb_dbopt_t *options) {
  ASSERT(test_try_reopen(t, options) == LDB_OK);
}

static void
test_destroy_and_reopen(test_t *t, const ldb_dbopt_t *options) {
  if (t->db != NULL)
    ldb_close(t->db);

  t->db = NULL;

  ldb_destroy(t->dbname, 0);

  ASSERT(test_try_reopen(t, options) == LDB_OK);
}

static void
test_close(test_t *t) {
  if (t->db != NULL)
    ldb_close(t->db);

  t->db = NULL;
}

static int
test_put(test_t *t, const char *k, const char *v) {
  ldb_slice_t key = ldb_string(k);
  ldb_slice_t val = ldb_string(v);

  return ldb_put(t->db, &key, &val, 0);
}

static int
test_del(test_t *t, const char *k) {
  ldb_slice_t key = ldb_string(k);

  return ldb_del(t->db, &key, 0);
}

static const char *
test_get2(test_t *t, const char *k, const ldb_snapshot_t *snap) {
  ldb_readopt_t opt = *ldb_readopt_default;
  ldb_slice_t key = ldb_string(k);
  ldb_slice_t val;
  char *zp;
  int rc;

  opt.snapshot = snap;

  rc = ldb_get(t->db, &key, &val, &opt);

  if (rc == LDB_NOTFOUND)
    return "NOT_FOUND";

  if (rc != LDB_OK)
    return ldb_strerror(rc);

  zp = ldb_malloc(val.size + 1);

  memcpy(zp, val.data, val.size);

  zp[val.size] = '\0';

  ldb_free(val.data);

  ldb_vector_push(&t->arena, zp);

  return zp;
}

static const char *
test_get(test_t *t, const char *k) {
  return test_get2(t, k, NULL);
}

static int
test_has(test_t *t, const char *k) {
  ldb_slice_t key = ldb_string(k);
  int rc;

  rc = ldb_has(t->db, &key, 0);

  if (rc == LDB_NOTFOUND)
    return 0;

  ASSERT(rc == LDB_OK);

  return 1;
}

static const char *
iter_status(test_t *t, ldb_iter_t *iter) {
  ldb_slice_t key, val;
  ldb_buffer_t z;

  if (!ldb_iter_valid(iter))
    return "(invalid)";

  key = ldb_iter_key(iter);
  val = ldb_iter_value(iter);

  ldb_buffer_init(&z);
  ldb_buffer_concat(&z, &key);
  ldb_buffer_string(&z, "->");
  ldb_buffer_concat(&z, &val);
  ldb_buffer_push(&z, 0);

  ldb_vector_push(&t->arena, z.data);

  return (char *)z.data;
}

static void
iter_seek(ldb_iter_t *iter, const char *k) {
  ldb_slice_t key = ldb_string(k);
  ldb_iter_seek(iter, &key);
}

/* Return a string that contains all key,value pairs in order,
   formatted like "(k1->v1)(k2->v2)". */
static const char *
test_contents(test_t *t) {
  ldb_vector_t forward;
  ldb_iter_t *iter;
  ldb_buffer_t z;
  size_t matched = 0;

  ldb_buffer_init(&z);
  ldb_vector_init(&forward);

  iter = ldb_iterator(t->db, 0);

  for (ldb_iter_first(iter); ldb_iter_valid(iter); ldb_iter_next(iter)) {
    const char *s = iter_status(t, iter);

    ldb_buffer_push(&z, '(');
    ldb_buffer_string(&z, s);
    ldb_buffer_push(&z, ')');

    ldb_vector_push(&forward, s);
  }

  ldb_buffer_push(&z, 0);

  /* Check reverse iteration results are the reverse of forward results. */
  for (ldb_iter_last(iter); ldb_iter_valid(iter); ldb_iter_prev(iter)) {
    size_t index = forward.length - matched - 1;

    ASSERT(matched < forward.length);
    ASSERT_EQ(iter_status(t, iter), forward.items[index]);

    matched++;
  }

  ASSERT(matched == forward.length);

  ldb_iter_destroy(iter);

  ldb_vector_clear(&forward);

  ldb_vector_push(&t->arena, z.data);

  return (char *)z.data;
}

static const char *
test_all_entries(test_t *t, const char *user_key) {
  ldb_slice_t ukey = ldb_string(user_key);
  ldb_iter_t *iter;
  ldb_ikey_t ikey;
  ldb_buffer_t z;
  int first = 1;
  int rc;

  ldb_buffer_init(&z);
  ldb_ikey_init(&ikey);

  ldb_ikey_set(&ikey, &ukey, LDB_MAX_SEQUENCE, LDB_TYPE_VALUE);

  iter = ldb_test_internal_iterator(t->db);

  ldb_iter_seek(iter, &ikey);

  rc = ldb_iter_status(iter);

  if (rc != LDB_OK) {
    ldb_buffer_string(&z, ldb_strerror(rc));
  } else {
    ldb_buffer_string(&z, "[ ");

    while (ldb_iter_valid(iter)) {
      ldb_slice_t key = ldb_iter_key(iter);
      ldb_slice_t val;
      ldb_pkey_t pkey;

      if (!ldb_pkey_import(&pkey, &key)) {
        ldb_buffer_string(&z, "CORRUPTED");
      } else {
        const ldb_comparator_t *cmp = t->last_options.comparator;

        if (ldb_compare(cmp, &pkey.user_key, &ukey) != 0)
          break;

        if (!first)
          ldb_buffer_string(&z, ", ");

        first = 0;

        switch (pkey.type) {
          case LDB_TYPE_VALUE:
            val = ldb_iter_value(iter);
            ldb_buffer_concat(&z, &val);
            break;
          case LDB_TYPE_DELETION:
            ldb_buffer_string(&z, "DEL");
            break;
        }
      }

      ldb_iter_next(iter);
    }

    if (!first)
      ldb_buffer_string(&z, " ");

    ldb_buffer_string(&z, "]");
  }

  ldb_buffer_push(&z, 0);

  ldb_iter_destroy(iter);
  ldb_ikey_clear(&ikey);

  ldb_vector_push(&t->arena, z.data);

  return (char *)z.data;
}

static int
test_files_at_level(test_t *t, int level) {
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
test_total_files(test_t *t) {
  int result = 0;
  int level;

  for (level = 0; level < LDB_NUM_LEVELS; level++)
    result += test_files_at_level(t, level);

  return result;
}

/* Return spread of files per level. */
static const char *
test_files_per_level(test_t *t) {
  char *buf = ldb_malloc(256);
  char *last = buf;
  char *zp = buf;
  int level;

  for (level = 0; level < LDB_NUM_LEVELS; level++) {
    int f = test_files_at_level(t, level);

    zp += sprintf(zp, "%s%d", (level ? "," : ""), f);

    if (f > 0)
      last = zp;
  }

  last[0] = '\0';

  ldb_vector_push(&t->arena, buf);

  return buf;
}

static int
test_count_files(test_t *t) {
  char **files;
  int len;

  len = ldb_get_children(t->dbname, &files);

  if (len >= 0)
    ldb_free_children(files, len);

  if (len < 0)
    len = 0;

  return len;
}

static uint64_t
test_size(test_t *t, const char *start, const char *limit) {
  ldb_range_t r;
  uint64_t size;

  r.start = ldb_string(start);
  r.limit = ldb_string(limit);

  ldb_approximate_sizes(t->db, &r, 1, &size);

  return size;
}

static void
test_compact(test_t *t, const char *start, const char *limit) {
  ldb_slice_t s = ldb_string(start);
  ldb_slice_t l = ldb_string(limit);

  ldb_compact(t->db, &s, &l);
}

/* Do n memtable compactions, each of which produces an sstable
   covering the range [small_key,large_key]. */
static void
test_make_tables(test_t *t, int n, const char *small, const char *large) {
  int i;

  for (i = 0; i < n; i++) {
    test_put(t, small, "begin");
    test_put(t, large, "end");

    ldb_test_compact_memtable(t->db);
  }
}

/* Prevent pushing of new sstables into deeper levels by adding
   tables that cover a specified range to all levels. */
static void
test_fill_levels(test_t *t, const char *small, const char *large) {
  test_make_tables(t, LDB_NUM_LEVELS, small, large);
}

LDB_UNUSED static void
test_dump_file_counts(test_t *t, const char *label) {
  int level;

  fprintf(stderr, "---\n%s:\n", label);

  fprintf(stderr, "maxoverlap: %.0f\n",
    (double)ldb_test_max_next_level_overlapping_bytes(t->db));

  for (level = 0; level < LDB_NUM_LEVELS; level++) {
    int num = test_files_at_level(t, level);

    if (num > 0)
      fprintf(stderr, "  level %3d : %d files\n", level, num);
  }
}

LDB_UNUSED static const char *
test_dump_sst_list(test_t *t) {
  char *value;

  ASSERT(ldb_property(t->db, "leveldb.sstables", &value));

  ldb_vector_push(&t->arena, value);

  return value;
}

static int
test_delete_an_sst_file(test_t *t) {
  char fname[LDB_PATH_MAX];
  ldb_filetype_t type;
  uint64_t number;
  int found = 0;
  char **names;
  int i, len;

  len = ldb_get_children(t->dbname, &names);

  ASSERT(len >= 0);

  for (i = 0; i < len; i++) {
    if (!ldb_parse_filename(&type, &number, names[i]))
      continue;

    if (type != LDB_FILE_TABLE)
      continue;

    ASSERT(ldb_table_filename(fname, sizeof(fname), t->dbname, number));
    ASSERT(ldb_remove_file(fname) == LDB_OK);

    found = 1;

    break;
  }

  ldb_free_children(names, len);

  return found;
}

/* Returns number of files renamed. */
static int
test_rename_ldb_to_sst(test_t *t) {
  char from[LDB_PATH_MAX];
  char to[LDB_PATH_MAX];
  ldb_filetype_t type;
  uint64_t number;
  int renamed = 0;
  char **names;
  int i, len;

  len = ldb_get_children(t->dbname, &names);

  ASSERT(len >= 0);

  for (i = 0; i < len; i++) {
    if (!ldb_parse_filename(&type, &number, names[i]))
      continue;

    if (type != LDB_FILE_TABLE)
      continue;

    ASSERT(ldb_table_filename(from, sizeof(from), t->dbname, number));
    ASSERT(ldb_sstable_filename(to, sizeof(to), t->dbname, number));
    ASSERT(ldb_rename_file(from, to) == LDB_OK);

    renamed++;
  }

  ldb_free_children(names, len);

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
    ASSERT(test_put(t, "", "v1") == LDB_OK);
    ASSERT_EQ("v1", test_get(t, ""));
    ASSERT(test_put(t, "", "v2") == LDB_OK);
    ASSERT_EQ("v2", test_get(t, ""));
  } while (test_change_options(t));
}

static void
test_db_empty_value(test_t *t) {
  do {
    ASSERT(test_put(t, "key", "v1") == LDB_OK);
    ASSERT_EQ("v1", test_get(t, "key"));
    ASSERT(test_put(t, "key", "") == LDB_OK);
    ASSERT_EQ("", test_get(t, "key"));
    ASSERT(test_put(t, "key", "v2") == LDB_OK);
    ASSERT_EQ("v2", test_get(t, "key"));
  } while (test_change_options(t));
}

static void
test_db_read_write(test_t *t) {
  do {
    ASSERT(test_put(t, "foo", "v1") == LDB_OK);
    ASSERT_EQ("v1", test_get(t, "foo"));
    ASSERT(test_put(t, "bar", "v2") == LDB_OK);
    ASSERT(test_put(t, "foo", "v3") == LDB_OK);
    ASSERT_EQ("v3", test_get(t, "foo"));
    ASSERT_EQ("v2", test_get(t, "bar"));
  } while (test_change_options(t));
}

static void
test_db_put_delete_get(test_t *t) {
  do {
    ASSERT(test_put(t, "foo", "v1") == LDB_OK);
    ASSERT_EQ("v1", test_get(t, "foo"));
    ASSERT(test_put(t, "foo", "v2") == LDB_OK);
    ASSERT_EQ("v2", test_get(t, "foo"));
    ASSERT(test_del(t, "foo") == LDB_OK);
    ASSERT_EQ("NOT_FOUND", test_get(t, "foo"));
  } while (test_change_options(t));
}

static void
test_db_get_from_immutable_layer(test_t *t) {
  do {
    ldb_dbopt_t options = test_current_options(t);

    options.write_buffer_size = 100000; /* Small write buffer */

    test_reopen(t, &options);

    ASSERT(test_put(t, "foo", "v1") == LDB_OK);

    ASSERT_EQ("v1", test_get(t, "foo"));

    /* Block sync calls. */
    /* ldb_atomic_store(&env->delay_data_sync, 1, ldb_order_release); */

    test_put(t, "k1", string_fill(t, 'x', 100000)); /* Fill memtable. */
    test_put(t, "k2", string_fill(t, 'y', 100000)); /* Trigger compaction. */

    ASSERT_EQ("v1", test_get(t, "foo"));

    /* Release sync calls. */
    /* ldb_atomic_store(&env->delay_data_sync, 0, ldb_order_release); */
  } while (test_change_options(t));
}

static void
test_db_get_from_versions(test_t *t) {
  do {
    ASSERT(test_put(t, "foo", "v1") == LDB_OK);
    ldb_test_compact_memtable(t->db);
    ASSERT_EQ("v1", test_get(t, "foo"));
  } while (test_change_options(t));
}

static void
test_db_get_memusage(test_t *t) {
  int mem_usage;
  char *val;

  do {
    ASSERT(test_put(t, "foo", "v1") == LDB_OK);
    ASSERT(ldb_property(t->db, "leveldb.approximate-memory-usage", &val));

    mem_usage = atoi(val);

    ldb_free(val);

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
      const ldb_snapshot_t *s1;

      ASSERT(test_put(t, key, "v1") == LDB_OK);

      s1 = ldb_snapshot(t->db);

      ASSERT(test_put(t, key, "v2") == LDB_OK);
      ASSERT_EQ("v2", test_get(t, key));
      ASSERT_EQ("v1", test_get2(t, key, s1));

      ldb_test_compact_memtable(t->db);

      ASSERT_EQ("v2", test_get(t, key));
      ASSERT_EQ("v1", test_get2(t, key, s1));

      ldb_release(t->db, s1);
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
      const ldb_snapshot_t *s1;
      const ldb_snapshot_t *s2;
      const ldb_snapshot_t *s3;

      ASSERT(test_put(t, key, "v1") == LDB_OK);

      s1 = ldb_snapshot(t->db);
      s2 = ldb_snapshot(t->db);
      s3 = ldb_snapshot(t->db);

      ASSERT(test_put(t, key, "v2") == LDB_OK);

      ASSERT_EQ("v2", test_get(t, key));
      ASSERT_EQ("v1", test_get2(t, key, s1));
      ASSERT_EQ("v1", test_get2(t, key, s2));
      ASSERT_EQ("v1", test_get2(t, key, s3));

      ldb_release(t->db, s1);

      ldb_test_compact_memtable(t->db);

      ASSERT_EQ("v2", test_get(t, key));
      ASSERT_EQ("v1", test_get2(t, key, s2));

      ldb_release(t->db, s2);

      ASSERT_EQ("v1", test_get2(t, key, s3));

      ldb_release(t->db, s3);
    }
  } while (test_change_options(t));
}

static void
test_db_iterate_over_empty_snapshot(test_t *t) {
  do {
    ldb_readopt_t options = *ldb_readopt_default;
    const ldb_snapshot_t *snapshot;
    ldb_iter_t *iter;

    snapshot = ldb_snapshot(t->db);

    options.snapshot = snapshot;

    ASSERT(test_put(t, "foo", "v1") == LDB_OK);
    ASSERT(test_put(t, "foo", "v2") == LDB_OK);

    iter = ldb_iterator(t->db, &options);

    ldb_iter_first(iter);

    ASSERT(!ldb_iter_valid(iter));

    ldb_iter_destroy(iter);

    ldb_test_compact_memtable(t->db);

    iter = ldb_iterator(t->db, &options);

    ldb_iter_first(iter);

    ASSERT(!ldb_iter_valid(iter));

    ldb_iter_destroy(iter);

    ldb_release(t->db, snapshot);
  } while (test_change_options(t));
}

static void
test_db_get_level0_ordering(test_t *t) {
  do {
    /* Check that we process level-0 files in correct order. The code
       below generates two level-0 files where the earlier one comes
       before the later one in the level-0 file list since the earlier
       one has a smaller "smallest" key. */
    ASSERT(test_put(t, "bar", "b") == LDB_OK);
    ASSERT(test_put(t, "foo", "v1") == LDB_OK);

    ldb_test_compact_memtable(t->db);

    ASSERT(test_put(t, "foo", "v2") == LDB_OK);

    ldb_test_compact_memtable(t->db);

    ASSERT_EQ("v2", test_get(t, "foo"));
  } while (test_change_options(t));
}

static void
test_db_get_ordered_by_levels(test_t *t) {
  do {
    ASSERT(test_put(t, "foo", "v1") == LDB_OK);

    test_compact(t, "a", "z");

    ASSERT_EQ("v1", test_get(t, "foo"));
    ASSERT(test_put(t, "foo", "v2") == LDB_OK);
    ASSERT_EQ("v2", test_get(t, "foo"));

    ldb_test_compact_memtable(t->db);

    ASSERT_EQ("v2", test_get(t, "foo"));
  } while (test_change_options(t));
}

static void
test_db_get_picks_correct_file(test_t *t) {
  do {
    /* Arrange to have multiple files in a non-level-0 level. */
    ASSERT(test_put(t, "a", "va") == LDB_OK);

    test_compact(t, "a", "b");

    ASSERT(test_put(t, "x", "vx") == LDB_OK);

    test_compact(t, "x", "y");

    ASSERT(test_put(t, "f", "vf") == LDB_OK);

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

      ldb_test_compact_memtable(t->db);
    }

    /* Step 2: clear level 1 if necessary. */
    ldb_test_compact_range(t->db, 1, NULL, NULL);

    ASSERT(test_files_at_level(t, 0) == 1);
    ASSERT(test_files_at_level(t, 1) == 0);
    ASSERT(test_files_at_level(t, 2) == 1);

    /* Step 3: read a bunch of times */
    for (i = 0; i < 1000; i++)
      ASSERT_EQ("NOT_FOUND", test_get(t, "missing"));

    /* Step 4: Wait for compaction to finish */
    ldb_sleep_msec(1000);

    ASSERT(test_files_at_level(t, 0) == 0);
  } while (test_change_options(t));
}

static void
test_db_iter_empty(test_t *t) {
  ldb_iter_t *iter = ldb_iterator(t->db, 0);

  ldb_iter_first(iter);

  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  ldb_iter_last(iter);

  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  iter_seek(iter, "foo");

  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  ldb_iter_destroy(iter);
}

static void
test_db_iter_single(test_t *t) {
  ldb_iter_t *iter;

  ASSERT(test_put(t, "a", "va") == LDB_OK);

  iter = ldb_iterator(t->db, 0);

  ldb_iter_first(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");
  ldb_iter_first(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  ldb_iter_last(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");
  ldb_iter_last(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  iter_seek(iter, "");
  ASSERT_EQ(iter_status(t, iter), "a->va");
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  iter_seek(iter, "a");
  ASSERT_EQ(iter_status(t, iter), "a->va");
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  iter_seek(iter, "b");
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  ldb_iter_destroy(iter);
}

static void
test_db_iter_multi(test_t *t) {
  ldb_iter_t *iter;

  ASSERT(test_put(t, "a", "va") == LDB_OK);
  ASSERT(test_put(t, "b", "vb") == LDB_OK);
  ASSERT(test_put(t, "c", "vc") == LDB_OK);

  iter = ldb_iterator(t->db, 0);

  ldb_iter_first(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "b->vb");
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "c->vc");
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");
  ldb_iter_first(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  ldb_iter_last(iter);
  ASSERT_EQ(iter_status(t, iter), "c->vc");
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "b->vb");
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");
  ldb_iter_last(iter);
  ASSERT_EQ(iter_status(t, iter), "c->vc");
  ldb_iter_next(iter);
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
  ldb_iter_last(iter);
  ldb_iter_prev(iter);
  ldb_iter_prev(iter);
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "b->vb");

  /* Switch from forward to reverse */
  ldb_iter_first(iter);
  ldb_iter_next(iter);
  ldb_iter_next(iter);
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "b->vb");

  /* Make sure iter stays at snapshot */
  ASSERT(test_put(t, "a", "va2") == LDB_OK);
  ASSERT(test_put(t, "a2", "va3") == LDB_OK);
  ASSERT(test_put(t, "b", "vb2") == LDB_OK);
  ASSERT(test_put(t, "c", "vc2") == LDB_OK);
  ASSERT(test_del(t, "b") == LDB_OK);

  ldb_iter_first(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "b->vb");
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "c->vc");
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");
  ldb_iter_last(iter);
  ASSERT_EQ(iter_status(t, iter), "c->vc");
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "b->vb");
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  ldb_iter_destroy(iter);
}

static void
test_db_iter_small_and_large_mix(test_t *t) {
  ldb_iter_t *iter;

  ASSERT(test_put(t, "a", "va") == LDB_OK);
  ASSERT(test_put(t, "b", string_fill(t, 'b', 100000)) == LDB_OK);
  ASSERT(test_put(t, "c", "vc") == LDB_OK);
  ASSERT(test_put(t, "d", string_fill(t, 'd', 100000)) == LDB_OK);
  ASSERT(test_put(t, "e", string_fill(t, 'e', 100000)) == LDB_OK);

  iter = ldb_iterator(t->db, 0);

  ldb_iter_first(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), string_fill2(t, "b->", 'b', 100000));
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "c->vc");
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), string_fill2(t, "d->", 'd', 100000));
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), string_fill2(t, "e->", 'e', 100000));
  ldb_iter_next(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  ldb_iter_last(iter);
  ASSERT_EQ(iter_status(t, iter), string_fill2(t, "e->", 'e', 100000));
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), string_fill2(t, "d->", 'd', 100000));
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "c->vc");
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), string_fill2(t, "b->", 'b', 100000));
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "a->va");
  ldb_iter_prev(iter);
  ASSERT_EQ(iter_status(t, iter), "(invalid)");

  ldb_iter_destroy(iter);
}

static void
test_db_iter_multi_with_delete(test_t *t) {
  do {
    ldb_iter_t *iter;

    ASSERT(test_put(t, "a", "va") == LDB_OK);
    ASSERT(test_put(t, "b", "vb") == LDB_OK);
    ASSERT(test_put(t, "c", "vc") == LDB_OK);
    ASSERT(test_del(t, "b") == LDB_OK);

    ASSERT_EQ("NOT_FOUND", test_get(t, "b"));

    iter = ldb_iterator(t->db, 0);

    iter_seek(iter, "c");
    ASSERT_EQ(iter_status(t, iter), "c->vc");

    ldb_iter_prev(iter);
    ASSERT_EQ(iter_status(t, iter), "a->va");

    ldb_iter_destroy(iter);
  } while (test_change_options(t));
}

static void
test_db_iter_multi_with_delete_and_compaction(test_t *t) {
  do {
    ldb_iter_t *iter;

    ASSERT(test_put(t, "b", "vb") == LDB_OK);
    ASSERT(test_put(t, "c", "vc") == LDB_OK);
    ASSERT(test_put(t, "a", "va") == LDB_OK);

    ldb_test_compact_memtable(t->db);

    ASSERT(test_del(t, "b") == LDB_OK);
    ASSERT_EQ("NOT_FOUND", test_get(t, "b"));

    iter = ldb_iterator(t->db, 0);

    iter_seek(iter, "c");
    ASSERT_EQ(iter_status(t, iter), "c->vc");

    ldb_iter_prev(iter);
    ASSERT_EQ(iter_status(t, iter), "a->va");

    iter_seek(iter, "b");
    ASSERT_EQ(iter_status(t, iter), "c->vc");

    ldb_iter_destroy(iter);
  } while (test_change_options(t));
}

static void
test_db_recover(test_t *t) {
  do {
    ASSERT(test_put(t, "foo", "v1") == LDB_OK);
    ASSERT(test_put(t, "baz", "v5") == LDB_OK);

    test_reopen(t, 0);

    ASSERT_EQ("v1", test_get(t, "foo"));
    ASSERT_EQ("v1", test_get(t, "foo"));
    ASSERT_EQ("v5", test_get(t, "baz"));
    ASSERT(test_put(t, "bar", "v2") == LDB_OK);
    ASSERT(test_put(t, "foo", "v3") == LDB_OK);

    test_reopen(t, 0);

    ASSERT_EQ("v3", test_get(t, "foo"));
    ASSERT(test_put(t, "foo", "v4") == LDB_OK);
    ASSERT_EQ("v4", test_get(t, "foo"));
    ASSERT_EQ("v2", test_get(t, "bar"));
    ASSERT_EQ("v5", test_get(t, "baz"));
  } while (test_change_options(t));
}

static void
test_db_recover_with_empty_log(test_t *t) {
  do {
    ASSERT(test_put(t, "foo", "v1") == LDB_OK);
    ASSERT(test_put(t, "foo", "v2") == LDB_OK);

    test_reopen(t, 0);
    test_reopen(t, 0);

    ASSERT(test_put(t, "foo", "v3") == LDB_OK);

    test_reopen(t, 0);

    ASSERT_EQ("v3", test_get(t, "foo"));
  } while (test_change_options(t));
}

/* Check that writes done during a memtable compaction are recovered
   if the database is shutdown during the memtable compaction. */
static void
test_db_recover_during_memtable_compaction(test_t *t) {
  do {
    ldb_dbopt_t options = test_current_options(t);

    options.write_buffer_size = 1000000;

    test_reopen(t, &options);

    /* Trigger a long memtable compaction and reopen the database during it */
    ASSERT(test_put(t, "foo", "v1") == LDB_OK); /* Goes to 1st log file */
    ASSERT(test_put(t, "big1", string_fill(t, 'x', 10000000)) == LDB_OK); /* Fills memtable */
    ASSERT(test_put(t, "big2", string_fill(t, 'y', 1000)) == LDB_OK); /* Triggers compaction */
    ASSERT(test_put(t, "bar", "v2") == LDB_OK); /* Goes to new log file */

    test_reopen(t, &options);

    ASSERT_EQ("v1", test_get(t, "foo"));
    ASSERT_EQ("v2", test_get(t, "bar"));
    ASSERT_EQ(string_fill(t, 'x', 10000000), test_get(t, "big1"));
    ASSERT_EQ(string_fill(t, 'y', 1000), test_get(t, "big2"));
  } while (test_change_options(t));
}

static void
test_db_minor_compactions_happen(test_t *t) {
  ldb_dbopt_t options = test_current_options(t);
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

    ASSERT(test_put(t, key, val) == LDB_OK);
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
    ldb_dbopt_t options = test_current_options(t);

    test_reopen(t, &options);

    ASSERT(test_put(t, "big1", string_fill(t, '1', 200000)) == LDB_OK);
    ASSERT(test_put(t, "big2", string_fill(t, '2', 200000)) == LDB_OK);
    ASSERT(test_put(t, "small3", string_fill(t, '3', 10)) == LDB_OK);
    ASSERT(test_put(t, "small4", string_fill(t, '4', 10)) == LDB_OK);
    ASSERT(test_files_at_level(t, 0) == 0);
  }

  /* Make sure that if we re-open with a small write buffer size that
     we flush table files in the middle of a large log file. */
  {
    ldb_dbopt_t options = test_current_options(t);

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
  ldb_dbopt_t options = test_current_options(t);
  ldb_vector_t values;
  ldb_rand_t rnd;
  int i;

  ldb_vector_init(&values);
  ldb_rand_init(&rnd, 301);

  options.write_buffer_size = 100000000; /* Large write buffer */

  test_reopen(t, &options);

  /* Write 8MB (80 values, each 100K) */
  ASSERT(test_files_at_level(t, 0) == 0);

  for (i = 0; i < 80; i++) {
    const char *value = random_string(t, &rnd, 100000);

    ASSERT(test_put(t, test_key(t, i), value) == LDB_OK);

    ldb_vector_push(&values, value);
  }

  /* Reopening moves updates to level-0 */
  test_reopen(t, &options);

  ldb_test_compact_range(t->db, 0, NULL, NULL);

  ASSERT(test_files_at_level(t, 0) == 0);
  ASSERT(test_files_at_level(t, 1) > 1);

  for (i = 0; i < 80; i++)
    ASSERT_EQ(test_get(t, test_key(t, i)), values.items[i]);

  ldb_vector_clear(&values);
}

static void
test_db_repeated_writes_to_same_key(test_t *t) {
  ldb_dbopt_t options = test_current_options(t);
  const char *value;
  int i, max_files;
  ldb_rand_t rnd;

  ldb_rand_init(&rnd, 301);

  options.write_buffer_size = 100000; /* Small write buffer */

  test_reopen(t, &options);

  /* We must have at most one file per level except for level-0,
     which may have up to kL0_StopWritesTrigger files. */
  max_files = LDB_NUM_LEVELS + LDB_L0_STOP_WRITES_TRIGGER;

  value = random_string(t, &rnd, 2 * options.write_buffer_size);

  for (i = 0; i < 5 * max_files; i++) {
    test_put(t, "key", value);

    ASSERT(test_total_files(t) <= max_files);

    fprintf(stderr, "after %d: %d files\n", i + 1, test_total_files(t));
  }
}

static void
test_db_sparse_merge(test_t *t) {
  ldb_dbopt_t options = test_current_options(t);
  const char *value;
  int i;

  options.compression = LDB_NO_COMPRESSION;

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

  ldb_test_compact_memtable(t->db);
  ldb_test_compact_range(t->db, 0, NULL, NULL);

  /* Make sparse update */
  test_put(t, "A", "va2");
  test_put(t, "B100", "bvalue2");
  test_put(t, "C", "vc2");

  ldb_test_compact_memtable(t->db);

  /* Compactions should not cause us to create a situation where
     a file overlaps too much data at the next level. */
  ASSERT(ldb_test_max_next_level_overlapping_bytes(t->db) <= 20 * 1048576);

  ldb_test_compact_range(t->db, 0, NULL, NULL);
  ASSERT(ldb_test_max_next_level_overlapping_bytes(t->db) <= 20 * 1048576);

  ldb_test_compact_range(t->db, 1, NULL, NULL);
  ASSERT(ldb_test_max_next_level_overlapping_bytes(t->db) <= 20 * 1048576);
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
    ldb_dbopt_t options = test_current_options(t);
    ldb_rand_t rnd;

    options.write_buffer_size = 100000000; /* Large write buffer */
    options.compression = LDB_NO_COMPRESSION;

    test_destroy_and_reopen(t, 0);

    ASSERT_RANGE(test_size(t, "", "xyz"), 0, 0);

    test_reopen(t, &options);

    ASSERT_RANGE(test_size(t, "", "xyz"), 0, 0);

    /* Write 8MB (80 values, each 100K) */
    ASSERT(test_files_at_level(t, 0) == 0);

    ldb_rand_init(&rnd, 301);

    for (i = 0; i < N; i++)
      ASSERT(test_put(t, test_key(t, i), random_string(t, &rnd, S1)) == LDB_OK);

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
        ldb_slice_t cstart = ldb_string(test_key(t, compact_start));
        ldb_slice_t cend = ldb_string(test_key(t, compact_start + 9));

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

        ldb_test_compact_range(t->db, 0, &cstart, &cend);
      }

      ASSERT(test_files_at_level(t, 0) == 0);
      ASSERT(test_files_at_level(t, 1) > 0);
    }
  } while (test_change_options(t));
}

static void
test_db_approximate_sizes_mix_of_small_and_large(test_t *t) {
  do {
    ldb_dbopt_t options = test_current_options(t);
    const char *big1;
    ldb_rand_t rnd;
    int run;

    options.compression = LDB_NO_COMPRESSION;

    test_reopen(t, 0);

    ldb_rand_init(&rnd, 301);

    big1 = random_string(t, &rnd, 100000);

    ASSERT(test_put(t, test_key(t, 0), random_string(t, &rnd, 10000)) == LDB_OK);
    ASSERT(test_put(t, test_key(t, 1), random_string(t, &rnd, 10000)) == LDB_OK);
    ASSERT(test_put(t, test_key(t, 2), big1) == LDB_OK);
    ASSERT(test_put(t, test_key(t, 3), random_string(t, &rnd, 10000)) == LDB_OK);
    ASSERT(test_put(t, test_key(t, 4), big1) == LDB_OK);
    ASSERT(test_put(t, test_key(t, 5), random_string(t, &rnd, 10000)) == LDB_OK);
    ASSERT(test_put(t, test_key(t, 6), random_string(t, &rnd, 300000)) == LDB_OK);
    ASSERT(test_put(t, test_key(t, 7), random_string(t, &rnd, 10000)) == LDB_OK);

    if (options.reuse_logs) {
      /* Need to force a memtable compaction since recovery does not do so. */
      ASSERT(ldb_test_compact_memtable(t->db) == LDB_OK);
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

      ldb_test_compact_range(t->db, 0, NULL, NULL);
    }
  } while (test_change_options(t));
}

static void
test_db_iterator_pins_ref(test_t *t) {
  ldb_iter_t *iter;
  int i;

  test_put(t, "foo", "hello");

  /* Get iterator that will yield the current contents of the DB. */
  iter = ldb_iterator(t->db, 0);

  /* Write to force compactions */
  test_put(t, "foo", "newvalue1");

  for (i = 0; i < 100; i++) {
    const char *key = test_key(t, i);
    const char *val = string_fill2(t, key, 'v', 100000);

    ASSERT(test_put(t, key, val) == LDB_OK); /* 100K values */
  }

  test_put(t, "foo", "newvalue2");

  ldb_iter_first(iter);

  ASSERT(ldb_iter_valid(iter));

  ASSERT_EQ(iter_status(t, iter), "foo->hello");

  ldb_iter_next(iter);

  ASSERT(!ldb_iter_valid(iter));

  ldb_iter_destroy(iter);
}

static void
test_db_snapshot(test_t *t) {
  const ldb_snapshot_t *s1, *s2, *s3;

  do {
    test_put(t, "foo", "v1");

    s1 = ldb_snapshot(t->db);

    test_put(t, "foo", "v2");

    s2 = ldb_snapshot(t->db);

    test_put(t, "foo", "v3");

    s3 = ldb_snapshot(t->db);

    test_put(t, "foo", "v4");

    ASSERT_EQ("v1", test_get2(t, "foo", s1));
    ASSERT_EQ("v2", test_get2(t, "foo", s2));
    ASSERT_EQ("v3", test_get2(t, "foo", s3));
    ASSERT_EQ("v4", test_get(t, "foo"));

    ldb_release(t->db, s3);

    ASSERT_EQ("v1", test_get2(t, "foo", s1));
    ASSERT_EQ("v2", test_get2(t, "foo", s2));
    ASSERT_EQ("v4", test_get(t, "foo"));

    ldb_release(t->db, s1);

    ASSERT_EQ("v2", test_get2(t, "foo", s2));
    ASSERT_EQ("v4", test_get(t, "foo"));

    ldb_release(t->db, s2);

    ASSERT_EQ("v4", test_get(t, "foo"));
  } while (test_change_options(t));
}

static void
test_db_hidden_values_are_removed(test_t *t) {
  do {
    const ldb_snapshot_t *snapshot;
    ldb_slice_t x = ldb_string("x");
    const char *big;
    ldb_rand_t rnd;
    char *expect;

    ldb_rand_init(&rnd, 301);

    test_fill_levels(t, "a", "z");

    big = random_string(t, &rnd, 50000);

    test_put(t, "foo", big);
    test_put(t, "pastfoo", "v");

    snapshot = ldb_snapshot(t->db);

    test_put(t, "foo", "tiny");
    test_put(t, "pastfoo2", "v2"); /* Advance sequence number one more */

    ASSERT(ldb_test_compact_memtable(t->db) == LDB_OK);
    ASSERT(test_files_at_level(t, 0) > 0);

    ASSERT_EQ(big, test_get2(t, "foo", snapshot));
    ASSERT_RANGE(test_size(t, "", "pastfoo"), 50000, 60000);

    ldb_release(t->db, snapshot);

    expect = ldb_malloc(50100);

    sprintf(expect, "[ tiny, %s ]", big);

    ASSERT_EQ(test_all_entries(t, "foo"), expect);

    ldb_free(expect);

    ldb_test_compact_range(t->db, 0, NULL, &x);

    ASSERT_EQ(test_all_entries(t, "foo"), "[ tiny ]");
    ASSERT(test_files_at_level(t, 0) == 0);
    ASSERT(test_files_at_level(t, 1) >= 1);

    ldb_test_compact_range(t->db, 1, NULL, &x);

    ASSERT_EQ(test_all_entries(t, "foo"), "[ tiny ]");

    ASSERT_RANGE(test_size(t, "", "pastfoo"), 0, 1000);
  } while (test_change_options(t));
}

static void
test_db_deletion_markers_1(test_t *t) {
  const int last = LDB_MAX_MEM_COMPACT_LEVEL;
  ldb_slice_t z = ldb_string("z");

  test_put(t, "foo", "v1");

  ASSERT(ldb_test_compact_memtable(t->db) == LDB_OK);
  ASSERT(test_files_at_level(t, last) == 1); /* foo => v1 is now in last level */

  /* Place a table at level last-1 to prevent merging with preceding mutation */
  test_put(t, "a", "begin");
  test_put(t, "z", "end");

  ldb_test_compact_memtable(t->db);

  ASSERT(test_files_at_level(t, last) == 1);
  ASSERT(test_files_at_level(t, last - 1) == 1);

  test_del(t, "foo");
  test_put(t, "foo", "v2");

  ASSERT_EQ(test_all_entries(t, "foo"), "[ v2, DEL, v1 ]");
  ASSERT(ldb_test_compact_memtable(t->db) == LDB_OK); /* Moves to level last-2 */
  ASSERT_EQ(test_all_entries(t, "foo"), "[ v2, DEL, v1 ]");

  ldb_test_compact_range(t->db, last - 2, NULL, &z);

  /* DEL eliminated, but v1 remains because we aren't compacting
     that level (DEL can be eliminated because v2 hides v1). */
  ASSERT_EQ(test_all_entries(t, "foo"), "[ v2, v1 ]");
  ldb_test_compact_range(t->db, last - 1, NULL, NULL);

  /* Merging last-1 w/ last, so we are the base level
     for "foo", so DEL is removed. (as is v1). */
  ASSERT_EQ(test_all_entries(t, "foo"), "[ v2 ]");
}

static void
test_db_deletion_markers_2(test_t *t) {
  const int last = LDB_MAX_MEM_COMPACT_LEVEL;

  test_put(t, "foo", "v1");

  ASSERT(ldb_test_compact_memtable(t->db) == LDB_OK);
  ASSERT(test_files_at_level(t, last) == 1); /* foo => v1 is now in last level */

  /* Place a table at level last-1 to prevent merging with preceding mutation */
  test_put(t, "a", "begin");
  test_put(t, "z", "end");

  ldb_test_compact_memtable(t->db);

  ASSERT(test_files_at_level(t, last) == 1);
  ASSERT(test_files_at_level(t, last - 1) == 1);

  test_del(t, "foo");

  ASSERT_EQ(test_all_entries(t, "foo"), "[ DEL, v1 ]");
  ASSERT(ldb_test_compact_memtable(t->db) == LDB_OK); /* Moves to level last == LDB_OK); */
  ASSERT_EQ(test_all_entries(t, "foo"), "[ DEL, v1 ]");

  ldb_test_compact_range(t->db, last - 2, NULL, NULL);

  /* DEL kept: "last" file overlaps */
  ASSERT_EQ(test_all_entries(t, "foo"), "[ DEL, v1 ]");
  ldb_test_compact_range(t->db, last - 1, NULL, NULL);

  /* Merging last-1 w/ last, so we are the base level
     for "foo", so DEL is removed. (as is v1). */
  ASSERT_EQ(test_all_entries(t, "foo"), "[ ]");
}

static void
test_db_overlap_in_level0(test_t *t) {
  do {
    ASSERT(LDB_MAX_MEM_COMPACT_LEVEL == 2);

    /* Fill levels 1 and 2 to disable the pushing
       of new memtables to levels > 0. */
    ASSERT(test_put(t, "100", "v100") == LDB_OK);
    ASSERT(test_put(t, "999", "v999") == LDB_OK);

    ldb_test_compact_memtable(t->db);

    ASSERT(test_del(t, "100") == LDB_OK);
    ASSERT(test_del(t, "999") == LDB_OK);

    ldb_test_compact_memtable(t->db);

    ASSERT_EQ("0,1,1", test_files_per_level(t));

    /* Make files spanning the following ranges in level-0:
     *
     *   files[0]  200 .. 900
     *   files[1]  300 .. 500
     *
     * Note that files are sorted by smallest key.
     */
    ASSERT(test_put(t, "300", "v300") == LDB_OK);
    ASSERT(test_put(t, "500", "v500") == LDB_OK);

    ldb_test_compact_memtable(t->db);

    ASSERT(test_put(t, "200", "v200") == LDB_OK);
    ASSERT(test_put(t, "600", "v600") == LDB_OK);
    ASSERT(test_put(t, "900", "v900") == LDB_OK);

    ldb_test_compact_memtable(t->db);

    ASSERT_EQ("2,1,1", test_files_per_level(t));

    /* Compact away the placeholder files we created initially */
    ldb_test_compact_range(t->db, 1, NULL, NULL);
    ldb_test_compact_range(t->db, 2, NULL, NULL);

    ASSERT_EQ("2", test_files_per_level(t));

    /* Do a memtable compaction. Before bug-fix, the compaction
       would not detect the overlap with level-0 files and would
       incorrectly place the deletion in a deeper level. */
    ASSERT(test_del(t, "600") == LDB_OK);

    ldb_test_compact_memtable(t->db);

    ASSERT_EQ("3", test_files_per_level(t));
    ASSERT_EQ("NOT_FOUND", test_get(t, "600"));
  } while (test_change_options(t));
}

static void
test_db_l0_compaction_bug_issue44_a(test_t *t) {
  test_reopen(t, 0);

  ASSERT(test_put(t, "b", "v") == LDB_OK);

  test_reopen(t, 0);

  ASSERT(test_del(t, "b") == LDB_OK);
  ASSERT(test_del(t, "a") == LDB_OK);

  test_reopen(t, 0);

  ASSERT(test_del(t, "a") == LDB_OK);

  test_reopen(t, 0);

  ASSERT(test_put(t, "a", "v") == LDB_OK);

  test_reopen(t, 0);
  test_reopen(t, 0);

  ASSERT_EQ("(a->v)", test_contents(t));

  ldb_sleep_msec(1000); /* Wait for compaction to finish */

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

  ldb_sleep_msec(1000); /* Wait for compaction to finish */

  test_reopen(t, 0);
  test_put(t, "d", "dv");
  test_reopen(t, 0);
  test_put(t, "", "");
  test_reopen(t, 0);
  test_del(t, "d");
  test_del(t, "b");
  test_reopen(t, 0);

  ASSERT_EQ("(->)(c->cv)", test_contents(t));

  ldb_sleep_msec(1000); /* Wait for compaction to finish */

  ASSERT_EQ("(->)(c->cv)", test_contents(t));
}

static void
test_db_fflush_issue474(test_t *t) {
  static const int num = 100000;
  ldb_rand_t rnd;
  int i;

  ldb_rand_init(&rnd, ldb_random_seed());

  for (i = 0; i < num; i++) {
    const char *key = random_key(t, &rnd);
    const char *val = random_string(t, &rnd, 100);

    fflush(NULL);

    ASSERT(test_put(t, key, val) == LDB_OK);
  }
}

static void
test_db_comparator_check(test_t *t) {
  ldb_comparator_t comparator = *ldb_bytewise_comparator;
  ldb_dbopt_t options = test_current_options(t);

  comparator.name = "leveldb.NewComparator";

  options.comparator = &comparator;

  ASSERT(test_try_reopen(t, &options) != LDB_OK);
}

static int
test_to_number(const ldb_slice_t *x) {
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
slice_compare(const ldb_comparator_t *comparator,
              const ldb_slice_t *x,
              const ldb_slice_t *y) {
  (void)comparator;
  return test_to_number(x) - test_to_number(y);
}

static void
shortest_separator(const ldb_comparator_t *comparator,
                   ldb_buffer_t *start,
                   const ldb_slice_t *limit) {
  (void)comparator;
  test_to_number(start); /* Check format */
  test_to_number(limit); /* Check format */
}

static void
short_successor(const ldb_comparator_t *comparator, ldb_buffer_t *key) {
  (void)comparator;
  test_to_number(key); /* Check format */
}

static void
test_db_custom_comparator(test_t *t) {
  static const ldb_comparator_t comparator = {
    /* .name = */ "test.NumberComparator",
    /* .compare = */ slice_compare,
    /* .shortest_separator = */ shortest_separator,
    /* .short_successor = */ short_successor,
    /* .user_comparator = */ NULL,
    /* .state = */ NULL
  };

  ldb_dbopt_t options = test_current_options(t);
  int i, run;

  options.create_if_missing = 1;
  options.comparator = &comparator;
  options.filter_policy = NULL;     /* Cannot use bloom filters */
  options.write_buffer_size = 1000; /* Compact more often */

  test_destroy_and_reopen(t, &options);

  ASSERT(test_put(t, "[10]", "ten") == LDB_OK);
  ASSERT(test_put(t, "[0x14]", "twenty") == LDB_OK);

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
      ASSERT(test_put(t, buf, buf) == LDB_OK);
    }

    test_compact(t, "[0]", "[1000000]");
  }
}

static void
test_db_manual_compaction(test_t *t) {
  ASSERT(LDB_MAX_MEM_COMPACT_LEVEL == 2);

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

  ldb_compact(t->db, NULL, NULL);
  ASSERT_EQ("0,0,1", test_files_per_level(t));
}

static void
test_db_open_options(test_t *t) {
  ldb_dbopt_t opts = *ldb_dbopt_default;
  char dbname[LDB_PATH_MAX];
  ldb_t *db = NULL;

  (void)t;

  ASSERT(ldb_test_filename(dbname, sizeof(dbname), "db_options_test"));

  ldb_destroy(dbname, 0);

  /* Does not exist, and create_if_missing == 0: error */
  opts.create_if_missing = 0;

  ASSERT(ldb_open(dbname, &opts, &db) == LDB_INVALID);
  ASSERT(db == NULL);

  /* Does not exist, and create_if_missing == 1: OK */
  opts.create_if_missing = 1;

  ASSERT(ldb_open(dbname, &opts, &db) == LDB_OK);
  ASSERT(db != NULL);

  ldb_close(db);
  db = NULL;

  /* Does exist, and error_if_exists == 1: error */
  opts.create_if_missing = 0;
  opts.error_if_exists = 1;

  ASSERT(ldb_open(dbname, &opts, &db) == LDB_INVALID);
  ASSERT(db == NULL);

  /* Does exist, and error_if_exists == 0: OK */
  opts.create_if_missing = 1;
  opts.error_if_exists = 0;

  ASSERT(ldb_open(dbname, &opts, &db) == LDB_OK);
  ASSERT(db != NULL);

  ldb_close(db);

  ldb_destroy(dbname, 0);
}

static void
test_db_destroy_empty_dir(test_t *t) {
  ldb_dbopt_t opts = *ldb_dbopt_default;
  char dbname[LDB_PATH_MAX];
  char **names;
  int len;

  (void)t;

  ASSERT(ldb_test_filename(dbname, sizeof(dbname), "db_empty_dir"));

  ldb_remove_dir(dbname);

  ASSERT(!ldb_file_exists(dbname));
  ASSERT(ldb_create_dir(dbname) == LDB_OK);
#ifndef LDB_MEMENV
  ASSERT(ldb_file_exists(dbname));
#endif
  ASSERT((len = ldb_get_children(dbname, &names)) >= 0);
  ASSERT(0 == len);
  ASSERT(ldb_destroy(dbname, &opts) == LDB_OK);
  ASSERT(!ldb_file_exists(dbname));

  ldb_free_children(names, len);
}

static void
test_db_destroy_open_db(test_t *t) {
  ldb_dbopt_t opts = *ldb_dbopt_default;
  char dbname[LDB_PATH_MAX];
  ldb_t *db = NULL;

  (void)t;

  ASSERT(ldb_test_filename(dbname, sizeof(dbname), "open_db_dir"));

  /* ldb_remove_dir(dbname); */
  ldb_destroy(dbname, 0);

  ASSERT(!ldb_file_exists(dbname));

  opts.create_if_missing = 1;

  ASSERT(ldb_open(dbname, &opts, &db) == LDB_OK);
  ASSERT(db != NULL);

  /* Must fail to destroy an open db. */
#ifndef LDB_MEMENV
  ASSERT(ldb_file_exists(dbname));
#endif
  ASSERT(ldb_destroy(dbname, 0) != LDB_OK);
#ifndef LDB_MEMENV
  ASSERT(ldb_file_exists(dbname));
#endif

  ldb_close(db);

  /* Should succeed destroying a closed db. */
  ASSERT(ldb_destroy(dbname, 0) == LDB_OK);
  ASSERT(!ldb_file_exists(dbname));
}

static void
test_db_locking(test_t *t) {
  ldb_dbopt_t opt = test_current_options(t);
  ldb_t *db = NULL;

  ASSERT(ldb_open(t->dbname, &opt, &db) != LDB_OK);
}

#if 0
/* Check that number of files does not grow when we are out of space */
static void
test_db_no_space(test_t *t) {
  ldb_dbopt_t options = test_current_options(t);
  int i, level, num_files;

  test_reopen(t, &options);

  ASSERT(test_put(t, "foo", "v1") == LDB_OK);
  ASSERT_EQ("v1", test_get(t, "foo"));

  test_compact(t, "a", "z");

  num_files = test_count_files(t);

  /* Force out-of-space errors. */
  ldb_atomic_store(&env->no_space, 1, ldb_order_release);

  for (i = 0; i < 10; i++) {
    for (level = 0; level < LDB_NUM_LEVELS - 1; level++)
      ldb_test_compact_range(t->db, level, NULL, NULL);
  }

  ldb_atomic_store(&env->no_space, 0, ldb_order_release);

  ASSERT(test_count_files(t) < num_files + 3);
}

static void
test_db_non_writable_filesystem(test_t *t) {
  ldb_dbopt_t options = test_current_options(t);
  const char *big;
  int i, errors;

  options.write_buffer_size = 1000;

  test_reopen(t, &options);

  ASSERT(test_put(t, "foo", "v1") == LDB_OK);

  /* Force errors for new files. */
  ldb_atomic_store(&env->non_writable, 1, ldb_order_release);

  big = string_fill(t, 'x', 100000);
  errors = 0;

  for (i = 0; i < 20; i++) {
    fprintf(stderr, "iter %d; errors %d\n", i, errors);

    if (test_put(t, "foo", big) != LDB_OK) {
      errors++;
      ldb_sleep_msec(100);
    }
  }

  ASSERT(errors > 0);

  ldb_atomic_store(&env->non_writable, 0, ldb_order_release);
}

static void
test_db_write_sync_error(test_t *t) {
  /* Check that log sync errors cause the DB to disallow future writes. */
  ldb_dbopt_t options = test_current_options(t);
  ldb_writeopt_t w = *ldb_writeopt_default;
  ldb_slice_t k1 = ldb_string("k1");
  ldb_slice_t k2 = ldb_string("k2");
  ldb_slice_t k3 = ldb_string("k3");
  ldb_slice_t v1 = ldb_string("v1");
  ldb_slice_t v2 = ldb_string("v2");
  ldb_slice_t v3 = ldb_string("v3");

  /* (a) Cause log sync calls to fail */
  test_reopen(t, &options);
  ldb_atomic_store(&env->data_sync_error, 1, ldb_order_release);

  /* (b) Normal write should succeed */
  w.sync = 0;

  ASSERT(ldb_put(t->db, &k1, &v1, &w) == LDB_OK);
  ASSERT_EQ("v1", test_get(t, "k1"));

  /* (c) Do a sync write; should fail */
  w.sync = 1;

  ASSERT(ldb_put(t->db, &k2, &v2, &w) != LDB_OK);
  ASSERT_EQ("v1", test_get(t, "k1"));
  ASSERT_EQ("NOT_FOUND", test_get(t, "k2"));

  /* (d) make sync behave normally */
  ldb_atomic_store(&env->data_sync_error, 0, ldb_order_release);

  /* (e) Do a non-sync write; should fail */
  w.sync = 0;

  ASSERT(ldb_put(t->db, &k3, &v3, &w) != LDB_OK);
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
  const int last = LDB_MAX_MEM_COMPACT_LEVEL;
  int iter;

  /* We iterate twice. In the second iteration, everything is the
     same except the log record never makes it to the MANIFEST file. */
  for (iter = 0; iter < 2; iter++) {
    ldb_atomic(int) *error_type = (iter == 0) ? &env->manifest_sync_error
                                              : &env->manifest_write_error;

    /* Insert foo=>bar mapping */
    ldb_dbopt_t options = test_current_options(t);

    options.create_if_missing = 1;
    options.error_if_exists = 0;

    test_destroy_and_reopen(t, &options);

    ASSERT(test_put(t, "foo", "bar") == LDB_OK);
    ASSERT_EQ("bar", test_get(t, "foo"));

    /* Memtable compaction (will succeed) */
    ldb_test_compact_memtable(t->db);

    ASSERT_EQ("bar", test_get(t, "foo"));
    ASSERT(test_files_at_level(t, last) == 1); /* foo=>bar is now in last level */

    /* Merging compaction (will fail) */
    ldb_atomic_store(error_type, 1, ldb_order_release);
    ldb_test_compact_range(t->db, last, NULL, NULL); /* Should fail */

    ASSERT_EQ("bar", test_get(t, "foo"));

    /* Recovery: should not lose data */
    ldb_atomic_store(error_type, 0, ldb_order_release);
    test_reopen(t, &options);

    ASSERT_EQ("bar", test_get(t, "foo"));
  }
}
#endif

static void
test_db_missing_sst_file(test_t *t) {
  ldb_dbopt_t options;

  ASSERT(test_put(t, "foo", "bar") == LDB_OK);
  ASSERT_EQ("bar", test_get(t, "foo"));

  /* Dump the memtable to disk. */
  ldb_test_compact_memtable(t->db);

  ASSERT_EQ("bar", test_get(t, "foo"));

  test_close(t);

  ASSERT(test_delete_an_sst_file(t));

  options = test_current_options(t);
  options.paranoid_checks = 1;

  ASSERT(test_try_reopen(t, &options) != LDB_OK);
}

static void
test_db_still_read_sst(test_t *t) {
  ldb_dbopt_t options;

  ASSERT(test_put(t, "foo", "bar") == LDB_OK);
  ASSERT_EQ("bar", test_get(t, "foo"));

  /* Dump the memtable to disk. */
  ldb_test_compact_memtable(t->db);

  ASSERT_EQ("bar", test_get(t, "foo"));

  test_close(t);

  ASSERT(test_rename_ldb_to_sst(t) > 0);

  options = test_current_options(t);
  options.paranoid_checks = 1;

  ASSERT(test_try_reopen(t, &options) == LDB_OK);

  ASSERT_EQ("bar", test_get(t, "foo"));
}

static void
test_db_files_deleted_after_compaction(test_t *t) {
  int i, num_files;

  ASSERT(test_put(t, "foo", "v2") == LDB_OK);

  test_compact(t, "a", "z");

  num_files = test_count_files(t);

  for (i = 0; i < 10; i++) {
    ASSERT(test_put(t, "foo", "v2") == LDB_OK);
    test_compact(t, "a", "z");
  }

  ASSERT(test_count_files(t) == num_files);
}

static void
test_db_bloom_filter(test_t *t) {
  ldb_dbopt_t options = test_current_options(t);
  const int N = 10000;
  int i, reads;

  /* env->count_random_reads = 1; */

  options.block_cache = ldb_lru_create(0); /* Prevent cache hits */
  options.filter_policy = ldb_bloom_create(10);

  test_reopen(t, &options);

  /* Populate multiple layers */
  for (i = 0; i < N; i++)
    ASSERT(test_put(t, test_key(t, i), test_key(t, i)) == LDB_OK);

  test_compact(t, "a", "z");

  for (i = 0; i < N; i += 100)
    ASSERT(test_put(t, test_key(t, i), test_key(t, i)) == LDB_OK);

  ldb_test_compact_memtable(t->db);

  /* Prevent auto compactions triggered by seeks */
  /* ldb_atomic_store(&env->delay_data_sync, 1, ldb_order_release); */

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

  /* ldb_atomic_store(&env->delay_data_sync, 0, ldb_order_release); */

  test_close(t);

  ldb_lru_destroy(options.block_cache);
  ldb_bloom_destroy((ldb_bloom_t *)options.filter_policy);
}

/*
 * Multi-threaded Testing
 */

#if defined(_WIN32) || defined(LDB_PTHREAD)

#define NUM_THREADS 4
#define TEST_SECONDS 10
#define NUM_KEYS 1000

typedef struct mt_state {
  test_t *test;
  ldb_atomic(int) stop;
  ldb_atomic(int) counter[NUM_THREADS];
  ldb_atomic(int) thread_done[NUM_THREADS];
} mt_state_t;

typedef struct mt_thread {
  mt_state_t *state;
  int id;
} mt_thread_t;

static void
mt_thread_body(void *arg) {
  mt_thread_t *ctx = (mt_thread_t *)arg;
  mt_state_t *state = ctx->state;
  ldb_t *db = state->test->db;
  ldb_slice_t key, val;
  int id = ctx->id;
  int counter = 0;
  char vbuf[1500];
  ldb_rand_t rnd;
  char kbuf[20];

  ldb_rand_init(&rnd, 1000 + id);

  fprintf(stderr, "... starting thread %d\n", id);

  while (!ldb_atomic_load(&state->stop, ldb_order_acquire)) {
    int num = ldb_rand_uniform(&rnd, NUM_KEYS);

    sprintf(kbuf, "%016d", num);

    key = ldb_string(kbuf);

    ldb_atomic_store(&state->counter[id], counter, ldb_order_release);

    if (ldb_rand_one_in(&rnd, 2)) {
      /* Write values of the form <key, my id, counter>. */
      /* We add some padding for force compactions. */
      sprintf(vbuf, "%d.%d.%-1000d", num, id, counter);

      val = ldb_string(vbuf);

      ASSERT(ldb_put(db, &key, &val, 0) == LDB_OK);
    } else {
      /* Read a value and verify that it matches the pattern written above. */
      int rc = ldb_get(db, &key, &val, 0);
      int n, w, c;

      if (rc == LDB_NOTFOUND) {
        /* Key has not yet been written */
      } else {
        ASSERT(rc == LDB_OK);
        ASSERT(val.size < sizeof(vbuf));

        memcpy(vbuf, val.data, val.size);

        vbuf[val.size] = '\0';

        ldb_free(val.data);

        /* Check that the writer thread counter
           is >= the counter in the value */
        ASSERT(3 == sscanf(vbuf, "%d.%d.%d", &n, &w, &c));
        ASSERT(n == num);
        ASSERT(w >= 0);
        ASSERT(w < NUM_THREADS);
        ASSERT(c <= ldb_atomic_load(&state->counter[w], ldb_order_acquire));
      }
    }

    counter++;
  }

  ldb_atomic_store(&state->thread_done[id], 1, ldb_order_release);

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

    ldb_atomic_store(&state.stop, 0, ldb_order_release);

    for (id = 0; id < NUM_THREADS; id++) {
      ldb_atomic_store(&state.counter[id], 0, ldb_order_release);
      ldb_atomic_store(&state.thread_done[id], 0, ldb_order_release);
    }

    /* Start threads */
    for (id = 0; id < NUM_THREADS; id++) {
      ldb_thread_t thread;

      contexts[id].state = &state;
      contexts[id].id = id;

      ldb_thread_create(&thread, mt_thread_body, &contexts[id]);
      ldb_thread_detach(&thread);
    }

    /* Let them run for a while */
    ldb_sleep_msec(TEST_SECONDS * 1000);

    /* Stop the threads and wait for them to finish */
    ldb_atomic_store(&state.stop, 1, ldb_order_release);

    for (id = 0; id < NUM_THREADS; id++) {
      while (!ldb_atomic_load(&state.thread_done[id], ldb_order_acquire))
        ldb_sleep_msec(100);
    }
  } while (test_change_options(t));
}

#endif /* _WIN32 || LDB_PTHREAD */

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
batch_put(ldb_batch_t *b, const char *k, const char *v) {
  ldb_slice_t key = ldb_string(k);
  ldb_slice_t val = ldb_string(v);

  ldb_batch_put(b, &key, &val);
}

static void
batch_del(ldb_batch_t *b, const char *k) {
  ldb_slice_t key = ldb_string(k);

  ldb_batch_del(b, &key);
}

static void
check_get(ldb_t *db, const ldb_snapshot_t *db_snap,
          rb_map_t *map, rb_map_t *map_snap) {
  ldb_readopt_t opt = *ldb_readopt_default;
  rb_iter_t it;

  opt.snapshot = db_snap;

  if (map_snap == NULL)
    map_snap = map;

  it = rb_tree_iterator(map_snap);

  rb_iter_first(&it);

  while (rb_iter_valid(&it)) {
    const char *k = rb_iter_key(&it).p;
    const char *v = rb_iter_value(&it).p;
    ldb_slice_t key = ldb_string(k);
    ldb_slice_t val;

    ASSERT(ldb_get(db, &key, &val, &opt) == LDB_OK);
    ASSERT(val.size == strlen(v));
    ASSERT(memcmp(val.data, v, val.size) == 0);

    ldb_free(val.data);

    rb_iter_next(&it);
  }
}

static int
iter_equal(ldb_iter_t *iter, rb_iter_t *it) {
  ldb_slice_t k1, v1;
  rb_val_t k2, v2;

  if (!ldb_iter_valid(iter))
    return !rb_iter_valid(it);

  if (!rb_iter_valid(it))
    return !ldb_iter_valid(iter);

  k1 = ldb_iter_key(iter);
  v1 = ldb_iter_value(iter);

  k2 = rb_iter_key(it);
  v2 = rb_iter_value(it);

  if (k1.size != strlen(k2.p))
    return 0;

  if (v1.size != strlen(v2.p))
    return 0;

  return memcmp(k1.data, k2.p, k1.size) == 0 &&
         memcmp(v1.data, v2.p, v1.size) == 0;
}

static void
check_iter(ldb_t *db, const ldb_snapshot_t *db_snap,
           rb_map_t *map, rb_map_t *map_snap) {
  ldb_readopt_t opt = *ldb_readopt_default;
  ldb_vector_t keys;
  ldb_iter_t *iter;
  int count = 0;
  rb_iter_t it;
  size_t i;

  ldb_vector_init(&keys);

  opt.snapshot = db_snap;

  if (map_snap == NULL)
    map_snap = map;

  iter = ldb_iterator(db, &opt);
  it = rb_tree_iterator(map_snap);

  ASSERT(!ldb_iter_valid(iter));

  ldb_iter_first(iter);
  rb_iter_first(&it);

  while (rb_iter_valid(&it)) {
    ASSERT(ldb_iter_valid(iter));
    ASSERT(iter_equal(iter, &it));

    if ((++count % 10) == 0)
      ldb_vector_push(&keys, rb_iter_key(&it).p);

    ldb_iter_next(iter);
    rb_iter_next(&it);
  }

  ASSERT(!ldb_iter_valid(iter));

  for (i = 0; i < keys.length; i++) {
    ldb_slice_t key = ldb_string(keys.items[i]);
    rb_val_t k;

    k.p = key.data;

    ldb_iter_seek(iter, &key);
    rb_iter_seek(&it, k);

    ASSERT(iter_equal(iter, &it));
  }

  ldb_vector_clear(&keys);
  ldb_iter_destroy(iter);
}

static void
test_db_randomized(test_t *t) {
  const int N = 10000;
  ldb_rand_t rnd;

  ldb_rand_init(&rnd, ldb_random_seed());

  do {
    const ldb_snapshot_t *db_snap = NULL;
    rb_map_t *map_snap = NULL;
    int i, p, step, num;
    rb_map_t map, tmp;
    const char *k, *v;
    ldb_batch_t b;

    rb_map_init(&map, map_compare, NULL);
    rb_map_init(&tmp, map_compare, NULL);

    for (step = 0; step < N; step++) {
      if (step % 100 == 0)
        fprintf(stderr, "Step %d of %d\n", step, N);

      p = ldb_rand_uniform(&rnd, 100);

      if (p < 45) { /* Put */
        k = random_key(t, &rnd);
        v = random_string(t, &rnd, ldb_rand_one_in(&rnd, 20)
                                 ? 100 + ldb_rand_uniform(&rnd, 100)
                                 : ldb_rand_uniform(&rnd, 8));

        map_put(&map, k, v);

        ASSERT(test_put(t, k, v) == LDB_OK);
        ASSERT(test_has(t, k));
      } else if (p < 90) { /* Delete */
        k = random_key(t, &rnd);

        map_del(&map, k);

        ASSERT(test_del(t, k) == LDB_OK);
        ASSERT(!test_has(t, k));
      } else { /* Multi-element batch */
        ldb_batch_init(&b);

        num = ldb_rand_uniform(&rnd, 8);

        for (i = 0; i < num; i++) {
          if (i == 0 || !ldb_rand_one_in(&rnd, 10)) {
            k = random_key(t, &rnd);
          } else {
            /* Periodically re-use the same key from the previous iter, so
               we have multiple entries in the write batch for the same key */
          }

          if (ldb_rand_one_in(&rnd, 2)) {
            v = random_string(t, &rnd, ldb_rand_uniform(&rnd, 10));
            batch_put(&b, k, v);
            map_put(&map, k, v);
          } else {
            batch_del(&b, k);
            map_del(&map, k);
          }
        }

        ASSERT(ldb_write(t->db, &b, 0) == LDB_OK);

        ldb_batch_clear(&b);
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
          ldb_release(t->db, db_snap);

        test_reopen(t, 0);

        check_get(t->db, NULL, &map, NULL);
        check_iter(t->db, NULL, &map, NULL);

        rb_map_copy(&tmp, &map, NULL);

        map_snap = &tmp;
        db_snap = ldb_snapshot(t->db);
      }
    }

    if (map_snap != NULL)
      rb_map_clear(map_snap, NULL);

    if (db_snap != NULL)
      ldb_release(t->db, db_snap);

    rb_map_clear(&map, NULL);
    rb_map_clear(&tmp, NULL);
  } while (test_change_options(t));
}

/*
 * Execute
 */

int
main(void) {
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
#if defined(_WIN32) || defined(LDB_PTHREAD)
    test_db_multi_threaded,
#endif
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
