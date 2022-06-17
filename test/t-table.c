/*!
 * t-table.c - table test for lcdb
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

#include "util/buffer.h"
#include "util/comparator.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/random.h"
#include "util/rbt.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/strutil.h"
#include "util/testutil.h"
#include "util/vector.h"

#include "table/block_builder.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator.h"
#include "table/table_builder.h"
#include "table/table.h"

#include "dbformat.h"
#include "db_impl.h"
#include "memtable.h"
#include "write_batch.h"

/*
 * ReverseKeyComparator
 */

static void
slice_reverse(ldb_buffer_t *z, const ldb_slice_t *x) {
  const uint8_t *xp = x->data;
  size_t xn = x->size;
  uint8_t *zp;

  zp = ldb_buffer_resize(z, xn);

  if (xn > 0)
    xp += xn;

  while (xn--)
    *zp++ = *--xp;
}

static int
rev_compare(const ldb_comparator_t *cmp,
            const ldb_slice_t *x,
            const ldb_slice_t *y) {
  ldb_buffer_t a, b;
  int r;

  (void)cmp;

  ldb_buffer_init(&a);
  ldb_buffer_init(&b);

  slice_reverse(&a, x);
  slice_reverse(&b, y);

  r = ldb_compare(ldb_bytewise_comparator, &a, &b);

  ldb_buffer_clear(&a);
  ldb_buffer_clear(&b);

  return r;
}

static void
rev_shortest_separator(const ldb_comparator_t *cmp,
                       ldb_buffer_t *start,
                       const ldb_slice_t *limit) {
  ldb_buffer_t s, l;

  (void)cmp;

  ldb_buffer_init(&s);
  ldb_buffer_init(&l);

  slice_reverse(&s, start);
  slice_reverse(&l, limit);

  ldb_shortest_separator(ldb_bytewise_comparator, &s, &l);

  slice_reverse(start, &s);

  ldb_buffer_clear(&s);
  ldb_buffer_clear(&l);
}

static void
rev_short_successor(const ldb_comparator_t *cmp, ldb_buffer_t *key) {
  ldb_buffer_t s;

  (void)cmp;

  ldb_buffer_init(&s);

  slice_reverse(&s, key);

  ldb_short_successor(ldb_bytewise_comparator, &s);

  slice_reverse(key, &s);

  ldb_buffer_clear(&s);
}

static const ldb_comparator_t reverse_comparator = {
  /* .name = */ "leveldb.ReverseBytewiseComparator",
  /* .compare = */ rev_compare,
  /* .shortest_separator = */ rev_shortest_separator,
  /* .short_successor = */ rev_short_successor,
  /* .user_comparator = */ NULL,
  /* .state = */ NULL
};

static void
cmp_increment(const ldb_comparator_t *cmp, ldb_buffer_t *key) {
  if (cmp == ldb_bytewise_comparator) {
    ldb_buffer_push(key, '\0');
  } else {
    ldb_buffer_t rev;

    ASSERT(cmp == &reverse_comparator);

    ldb_buffer_init(&rev);

    slice_reverse(&rev, key);

    ldb_buffer_push(&rev, '\0');

    slice_reverse(key, &rev);

    ldb_buffer_clear(&rev);
  }
}

/*
 * RBT Callbacks
 */

static int
map_compare(rb_val_t x, rb_val_t y, void *arg) {
  const ldb_comparator_t *cmp = arg;
  ldb_buffer_t *a = x.p;
  ldb_buffer_t *b = y.p;

  return ldb_compare(cmp, a, b);
}

static void
map_clear(rb_node_t *node) {
  ldb_buffer_clear(node->key.p);
  ldb_buffer_clear(node->value.p);

  ldb_free(node->key.p);
  ldb_free(node->value.p);
}

/*
 * Constructor
 */

/* Helper class for tests to unify the interface between
   BlockBuilder/TableBuilder and Block/Table. */
typedef struct ctortbl_s ctortbl_t;

typedef struct ctor_s {
  /* The underlying constructor. */
  void *ptr;
  /* Constructor function table. */
  const ctortbl_t *table;
  rb_map_t data;
} ctor_t;

struct ctortbl_s {
  void (*clear)(void *ctor);
  /* Construct the data structure from the data in "data". */
  int (*finish)(void *ctor,
                const ldb_dbopt_t *options,
                const rb_map_t *data);
  ldb_iter_t *(*iterator)(const void *ctor);
  ldb_t *(*db)(const void *ctor);
};

static void
ctor_init(ctor_t *c,
          void *ctor,
          const ctortbl_t *tbl,
          const ldb_comparator_t *cmp) {
  c->ptr = ctor;
  c->table = tbl;
  rb_map_init(&c->data, map_compare, (void *)cmp);
}

static void
ctor_clear(ctor_t *c) {
  c->table->clear(c->ptr);
  rb_map_clear(&c->data, map_clear);
  ldb_free(c->ptr);
}

static ctor_t *
ctor_create(void *ctor, const ctortbl_t *tbl, const ldb_comparator_t *cmp) {
  ctor_t *c = ldb_malloc(sizeof(ctor_t));
  ctor_init(c, ctor, tbl, cmp);
  return c;
}

static void
ctor_destroy(ctor_t *c) {
  ctor_clear(c);
  ldb_free(c);
}

#define CTOR_FUNCTIONS(name)                                   \
                                                               \
static void                                                    \
name ## _clear_wrapped(void *ctor) {                           \
  name ## _clear((name ## _t *)ctor);                          \
}                                                              \
                                                               \
static int                                                     \
name ## _finish_wrapped(void *ctor,                            \
                        const ldb_dbopt_t *options,            \
                        const rb_map_t *data) {                \
  return name ## _finish((name ## _t *)ctor, options, data);   \
}                                                              \
                                                               \
static ldb_iter_t *                                            \
name ## _iterator_wrapped(const void *ctor) {                  \
  return name ## _iterator((const name ## _t *)ctor);          \
}                                                              \
                                                               \
static ldb_t *                                                 \
name ## _db_wrapped(const void *ctor) {                        \
  return name ## _db((const name ## _t *)ctor);                \
}                                                              \
                                                               \
static const ctortbl_t name ## _table = {                      \
  /* .clear = */ name ## _clear_wrapped,                       \
  /* .finish = */ name ## _finish_wrapped,                     \
  /* .iterator = */ name ## _iterator_wrapped,                 \
  /* .db = */ name ## _db_wrapped                              \
}

static void
ctor_add(ctor_t *c, const ldb_slice_t *key, const ldb_slice_t *value) {
  ldb_buffer_t *k;
  ldb_buffer_t *v;

  v = rb_map_get(&c->data, key);

  if (v != NULL) {
    ldb_buffer_copy(v, value);
  } else {
    k = ldb_malloc(sizeof(ldb_buffer_t));
    v = ldb_malloc(sizeof(ldb_buffer_t));

    ldb_buffer_init(k);
    ldb_buffer_init(v);

    ldb_buffer_copy(k, key);
    ldb_buffer_copy(v, value);

    ASSERT(rb_map_put(&c->data, k, v));
  }
}

static void
ctor_add_str(ctor_t *c, const char *key, const char *value) {
  ldb_slice_t k = ldb_string(key);
  ldb_slice_t v = ldb_string(value);

  ctor_add(c, &k, &v);
}

/* Finish constructing the data structure with all the keys that have
   been added so far. Returns the keys in sorted order in "*keys"
   and stores the key/value pairs in "*kvmap" */
static void
ctor_finish(ctor_t *c, const ldb_dbopt_t *options, ldb_vector_t *keys) {
  void *key;
  int rc;

  ASSERT(keys->length == 0);

  rb_map_keys(&c->data, key)
    ldb_vector_push(keys, key);

  rc = c->table->finish(c->ptr, options, &c->data);

  ASSERT(rc == LDB_OK);
}

#define ctor_iterator(ctor) (ctor)->table->iterator((ctor)->ptr)
#define ctor_db(ctor) (ctor)->table->db((ctor)->ptr)

/*
 * BlockConstructor
 */

typedef struct blockctor_s {
  const ldb_comparator_t *comparator;
  ldb_buffer_t data;
  ldb_block_t *block;
} blockctor_t;

static void
blockctor_init(blockctor_t *c, const ldb_comparator_t *cmp) {
  c->comparator = cmp;

  ldb_buffer_init(&c->data);

  c->block = NULL;
}

static void
blockctor_clear(blockctor_t *c) {
  ldb_buffer_clear(&c->data);

  if (c->block != NULL)
    ldb_block_destroy(c->block);
}

static int
blockctor_finish(blockctor_t *c,
                 const ldb_dbopt_t *options,
                 const rb_map_t *data) {
  ldb_contents_t contents;
  ldb_blockgen_t bb;
  void *key, *value;
  ldb_slice_t ret;

  if (c->block != NULL)
    ldb_block_destroy(c->block);

  c->block = NULL;

  ldb_blockgen_init(&bb, options);

  rb_map_iterate(data, key, value)
    ldb_blockgen_add(&bb, key, value);

  /* Open the block. */
  ret = ldb_blockgen_finish(&bb);
  ldb_buffer_copy(&c->data, &ret);

  contents.data = c->data;
  contents.cachable = 0;
  contents.heap_allocated = 0;

  c->block = ldb_block_create(&contents);

  ldb_blockgen_clear(&bb);

  return LDB_OK;
}

static ldb_iter_t *
blockctor_iterator(const blockctor_t *c) {
  ASSERT(c->block != NULL);
  return ldb_blockiter_create(c->block, c->comparator);
}

static ldb_t *
blockctor_db(const blockctor_t *c) {
  (void)c;
  return NULL;
}

CTOR_FUNCTIONS(blockctor);

static ctor_t *
blockctor_create(const ldb_comparator_t *cmp) {
  blockctor_t *c = ldb_malloc(sizeof(blockctor_t));

  blockctor_init(c, cmp);

  return ctor_create(c, &blockctor_table, cmp);
}

/*
 * TableConstructor
 */

typedef struct tablector_s {
  char path[LDB_PATH_MAX];
  ldb_rfile_t *source;
  ldb_table_t *table;
} tablector_t;

static void
tablector_init(tablector_t *c) {
  ASSERT(ldb_test_filename(c->path, sizeof(c->path), "test_table.ldb"));

  ldb_remove_file(c->path);

  c->source = NULL;
  c->table = NULL;
}

static void
tablector_clear(tablector_t *c) {
  if (c->table != NULL)
    ldb_table_destroy(c->table);

  if (c->source != NULL)
    ldb_rfile_destroy(c->source);

  ldb_remove_file(c->path);

  c->table = NULL;
  c->source = NULL;
}

static int
tablector_finish(tablector_t *c,
                 const ldb_dbopt_t *options,
                 const rb_map_t *data) {
  ldb_dbopt_t table_options = *ldb_dbopt_default;
  ldb_tablegen_t *tb;
  ldb_wfile_t *sink;
  void *key, *value;
  uint64_t fsize;

  tablector_clear(c);

  ASSERT(ldb_truncfile_create(c->path, &sink) == LDB_OK);

  tb = ldb_tablegen_create(options, sink);

  rb_map_iterate(data, key, value) {
    ldb_tablegen_add(tb, key, value);

    ASSERT(ldb_tablegen_status(tb) == LDB_OK);
  }

  ASSERT(ldb_tablegen_finish(tb) == LDB_OK);
  ASSERT(ldb_wfile_close(sink) == LDB_OK);

  ldb_wfile_destroy(sink);

  ASSERT(ldb_file_size(c->path, &fsize) == LDB_OK);
  ASSERT(fsize == ldb_tablegen_size(tb));

  ldb_tablegen_destroy(tb);

  /* Open the table. */
  ASSERT(ldb_seqfile_create(c->path, &c->source) == LDB_OK);

  table_options.comparator = options->comparator;

  return ldb_table_open(&table_options, c->source, fsize, &c->table);
}

static ldb_iter_t *
tablector_iterator(const tablector_t *c) {
  return ldb_tableiter_create(c->table, ldb_readopt_default);
}

static ldb_t *
tablector_db(const tablector_t *c) {
  (void)c;
  return NULL;
}

static uint64_t
ctor_approximate_offset(const ctor_t *ctor, const char *key) {
  const tablector_t *c = ctor->ptr;
  ldb_slice_t k = ldb_string(key);

  return ldb_table_approximate_offset(c->table, &k);
}

CTOR_FUNCTIONS(tablector);

static ctor_t *
tablector_create(const ldb_comparator_t *cmp) {
  tablector_t *c = ldb_malloc(sizeof(tablector_t));

  tablector_init(c);

  return ctor_create(c, &tablector_table, cmp);
}

/*
 * KeyConvertingIterator
 */

/* A helper class that converts internal format keys into user keys. */
typedef struct conviter_s {
  ldb_iter_t *it;
  int status;
} conviter_t;

static void
conviter_init(conviter_t *iter, ldb_iter_t *it) {
  iter->it = it;
  iter->status = LDB_OK;
}

static void
conviter_clear(conviter_t *iter) {
  ldb_iter_destroy(iter->it);
}

static int
conviter_valid(const conviter_t *iter) {
  return ldb_iter_valid(iter->it);
}

static void
conviter_seek(conviter_t *iter, const ldb_slice_t *target) {
  ldb_buffer_t encoded;
  ldb_pkey_t ikey;

  ldb_buffer_init(&encoded);

  ldb_pkey_init(&ikey, target, LDB_MAX_SEQUENCE, LDB_TYPE_VALUE);
  ldb_pkey_export(&encoded, &ikey);

  ldb_iter_seek(iter->it, &encoded);

  ldb_buffer_clear(&encoded);
}

static void
conviter_first(conviter_t *iter) {
  ldb_iter_first(iter->it);
}

static void
conviter_last(conviter_t *iter) {
  ldb_iter_last(iter->it);
}

static void
conviter_next(conviter_t *iter) {
  ldb_iter_next(iter->it);
}

static void
conviter_prev(conviter_t *iter) {
  ldb_iter_prev(iter->it);
}

static ldb_slice_t
conviter_key(const conviter_t *iter) {
  ldb_pkey_t key;
  ldb_slice_t k;

  ASSERT(conviter_valid(iter));

  k = ldb_iter_key(iter->it);

  if (!ldb_pkey_import(&key, &k)) {
    ((conviter_t *)iter)->status = LDB_BAD_INTERNAL_KEY;
    return ldb_string("corrupted key");
  }

  return key.user_key;
}

static ldb_slice_t
conviter_value(const conviter_t *iter) {
  return ldb_iter_value(iter->it);
}

static int
conviter_status(const conviter_t *iter) {
  if (iter->status != LDB_OK)
    return iter->status;

  return ldb_iter_status(iter->it);
}

LDB_ITERATOR_FUNCTIONS(conviter);

static ldb_iter_t *
conviter_create(ldb_iter_t *it) {
  conviter_t *iter = ldb_malloc(sizeof(conviter_t));

  conviter_init(iter, it);

  return ldb_iter_create(iter, &conviter_table);
}

/*
 * MemTableConstructor
 */

typedef struct memctor_s {
  ldb_comparator_t icmp;
  ldb_memtable_t *mt;
} memctor_t;

static void
memctor_init(memctor_t *c, const ldb_comparator_t *cmp) {
  ldb_ikc_init(&c->icmp, cmp);

  c->mt = ldb_memtable_create(&c->icmp);

  ldb_memtable_ref(c->mt);
}

static void
memctor_clear(memctor_t *c) {
  ldb_memtable_unref(c->mt);
}

static int
memctor_finish(memctor_t *c,
               const ldb_dbopt_t *options,
               const rb_map_t *data) {
  void *key, *value;
  int seq = 1;

  (void)options;

  ldb_memtable_unref(c->mt);

  c->mt = ldb_memtable_create(&c->icmp);

  ldb_memtable_ref(c->mt);

  rb_map_iterate(data, key, value) {
    ldb_memtable_add(c->mt, seq, LDB_TYPE_VALUE, key, value);
    seq++;
  }

  return LDB_OK;
}

static ldb_iter_t *
memctor_iterator(const memctor_t *c) {
  return conviter_create(ldb_memiter_create(c->mt));
}

static ldb_t *
memctor_db(const memctor_t *c) {
  (void)c;
  return NULL;
}

CTOR_FUNCTIONS(memctor);

static ctor_t *
memctor_create(const ldb_comparator_t *cmp) {
  memctor_t *c = ldb_malloc(sizeof(memctor_t));

  memctor_init(c, cmp);

  return ctor_create(c, &memctor_table, cmp);
}

/*
 * DBConstructor
 */

typedef struct dbctor_s {
  char dbname[LDB_PATH_MAX];
  const ldb_comparator_t *cmp;
  ldb_t *db;
} dbctor_t;

static void
dbctor_newdb(dbctor_t *c) {
  ldb_dbopt_t options = *ldb_dbopt_default;
  int rc;

  options.comparator = c->cmp;

  rc = ldb_destroy(c->dbname, &options);

  ASSERT(rc == LDB_OK);

  options.create_if_missing = 1;
  options.error_if_exists = 1;
  options.write_buffer_size = 10000; /* Something small to force merging. */

  rc = ldb_open(c->dbname, &options, &c->db);

  ASSERT(rc == LDB_OK);
}

static void
dbctor_init(dbctor_t *c, const ldb_comparator_t *cmp) {
  ASSERT(ldb_test_filename(c->dbname, sizeof(c->dbname), "table_testdb"));

  c->cmp = cmp;
  c->db = NULL;

  dbctor_newdb(c);
}

static void
dbctor_clear(dbctor_t *c) {
  ldb_close(c->db);
  ldb_destroy(c->dbname, NULL);
}

static int
dbctor_finish(dbctor_t *c,
              const ldb_dbopt_t *options,
              const rb_map_t *data) {
  void *key, *value;

  (void)options;

  ldb_close(c->db);

  c->db = NULL;

  dbctor_newdb(c);

  rb_map_iterate(data, key, value) {
    ldb_batch_t batch;

    ldb_batch_init(&batch);
    ldb_batch_put(&batch, key, value);

    ASSERT(ldb_write(c->db, &batch, NULL) == LDB_OK);

    ldb_batch_clear(&batch);
  }

  return LDB_OK;
}

static ldb_iter_t *
dbctor_iterator(const dbctor_t *c) {
  return ldb_iterator(c->db, ldb_readopt_default);
}

static ldb_t *
dbctor_db(const dbctor_t *c) {
  return c->db;
}

CTOR_FUNCTIONS(dbctor);

static ctor_t *
dbctor_create(const ldb_comparator_t *cmp) {
  dbctor_t *c = ldb_malloc(sizeof(dbctor_t));

  dbctor_init(c, cmp);

  return ctor_create(c, &dbctor_table, cmp);
}

/*
 * Harness Constants
 */

enum test_type { TABLE_TEST, BLOCK_TEST, MEMTABLE_TEST, DB_TEST };

struct test_args {
  enum test_type type;
  int reverse_compare;
  int restart_interval;
};

static const struct test_args test_arg_list[] = {
  {TABLE_TEST, 0, 16},
  {TABLE_TEST, 0, 1},
  {TABLE_TEST, 0, 1024},
  {TABLE_TEST, 1, 16},
  {TABLE_TEST, 1, 1},
  {TABLE_TEST, 1, 1024},

  {BLOCK_TEST, 0, 16},
  {BLOCK_TEST, 0, 1},
  {BLOCK_TEST, 0, 1024},
  {BLOCK_TEST, 1, 16},
  {BLOCK_TEST, 1, 1},
  {BLOCK_TEST, 1, 1024},

  /* Restart interval does not matter for memtables. */
  {MEMTABLE_TEST, 0, 16},
  {MEMTABLE_TEST, 1, 16},

  /* Do not bother with restart interval variations for DB. */
  {DB_TEST, 0, 16},
  {DB_TEST, 1, 16}
};

#define num_test_args ((int)lengthof(test_arg_list))

/*
 * Harness
 */

typedef struct harness {
  ldb_dbopt_t options;
  ctor_t *ctor;
} harness_t;

static void
harness_init(harness_t *h) {
  h->options = *ldb_dbopt_default;
  h->options.comparator = ldb_bytewise_comparator;
  h->ctor = NULL;
}

static void
harness_clear(harness_t *h) {
  ctor_destroy(h->ctor);
}

static void
harness_reset(harness_t *h, const struct test_args *args) {
  if (h->ctor != NULL)
    ctor_destroy(h->ctor);

  h->options = *ldb_dbopt_default;
  h->ctor = NULL;

  h->options.block_restart_interval = args->restart_interval;

  /* Use shorter block size for tests to exercise
     block boundary conditions more. */
  h->options.block_size = 256;

  if (args->reverse_compare)
    h->options.comparator = &reverse_comparator;
  else
    h->options.comparator = ldb_bytewise_comparator;

  switch (args->type) {
    case TABLE_TEST:
      h->ctor = tablector_create(h->options.comparator);
      break;
    case BLOCK_TEST:
      h->ctor = blockctor_create(h->options.comparator);
      break;
    case MEMTABLE_TEST:
      h->ctor = memctor_create(h->options.comparator);
      break;
    case DB_TEST:
      h->ctor = dbctor_create(h->options.comparator);
      break;
  }
}

static void
harness_add(harness_t *h, const ldb_slice_t *key, const ldb_slice_t *value) {
  ctor_add(h->ctor, key, value);
}

static void
harness_add_str(harness_t *h, const char *key, const char *value) {
  ctor_add_str(h->ctor, key, value);
}

static void
harness_random_key(harness_t *h,
                   ldb_buffer_t *result,
                   ldb_rand_t *rnd,
                   const ldb_vector_t *keys) {
  size_t index;

  if (keys->length == 0) {
    ldb_buffer_set_str(result, "foo");
    return;
  }

  index = ldb_rand_uniform(rnd, keys->length);

  ldb_buffer_copy(result, keys->items[index]);

  switch (ldb_rand_uniform(rnd, 3)) {
    case 0:
      /* Return an existing key. */
      break;
    case 1: {
      /* Attempt to return something smaller than an existing key. */
      if (result->size > 0 && result->data[result->size - 1] > '\0')
        result->data[result->size - 1]--;
      break;
    }
    case 2: {
      /* Return something larger than an existing key. */
      cmp_increment(h->options.comparator, result);
      break;
    }
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

  return ldb_buffer_equal(&k1, k2.p) &&
         ldb_buffer_equal(&v1, v2.p);
}

static void
harness_test_forward_scan(harness_t *h,
                          const ldb_vector_t *keys,
                          const rb_map_t *data) {
  ldb_iter_t *iter = ctor_iterator(h->ctor);
  rb_iter_t it = rb_tree_iterator(data);

  (void)keys;

  ASSERT(!ldb_iter_valid(iter));

  ldb_iter_first(iter);
  rb_iter_first(&it);

  while (rb_iter_valid(&it)) {
    ASSERT(ldb_iter_valid(iter));
    ASSERT(iter_equal(iter, &it));

    ldb_iter_next(iter);
    rb_iter_next(&it);
  }

  ASSERT(!ldb_iter_valid(iter));

  ldb_iter_destroy(iter);
}

static void
harness_test_backward_scan(harness_t *h,
                           const ldb_vector_t *keys,
                           const rb_map_t *data) {
  ldb_iter_t *iter = ctor_iterator(h->ctor);
  rb_iter_t it = rb_tree_iterator(data);

  (void)keys;

  ASSERT(!ldb_iter_valid(iter));

  ldb_iter_last(iter);
  rb_iter_last(&it);

  while (rb_iter_valid(&it)) {
    ASSERT(ldb_iter_valid(iter));
    ASSERT(iter_equal(iter, &it));

    ldb_iter_prev(iter);
    rb_iter_prev(&it);
  }

  ASSERT(!ldb_iter_valid(iter));

  ldb_iter_destroy(iter);
}

static void
harness_test_random_access(harness_t *h,
                           ldb_rand_t *rnd,
                           const ldb_vector_t *keys,
                           const rb_map_t *data) {
  static const int verbose = 0;
  ldb_iter_t *iter = ctor_iterator(h->ctor);
  rb_iter_t it = rb_tree_iterator(data);
  int i;

  ASSERT(!ldb_iter_valid(iter));

  rb_iter_first(&it);

  if (verbose)
    fprintf(stderr, "---\n");

  for (i = 0; i < 200; i++) {
    const int toss = ldb_rand_uniform(rnd, 5);

    switch (toss) {
      case 0: {
        if (ldb_iter_valid(iter)) {
          if (verbose)
            fprintf(stderr, "Next\n");

          ldb_iter_next(iter);
          rb_iter_next(&it);

          ASSERT(iter_equal(iter, &it));
        }

        break;
      }

      case 1: {
        if (verbose)
          fprintf(stderr, "SeekToFirst\n");

        ldb_iter_first(iter);
        rb_iter_first(&it);

        ASSERT(iter_equal(iter, &it));

        break;
      }

      case 2: {
        ldb_buffer_t key;
        rb_val_t k;

        k.p = &key;

        if (verbose)
          fprintf(stderr, "Seek\n");

        ldb_buffer_init(&key);

        harness_random_key(h, &key, rnd, keys);

        ldb_iter_seek(iter, &key);
        rb_iter_seek(&it, k);

        ASSERT(iter_equal(iter, &it));

        ldb_buffer_clear(&key);

        break;
      }

      case 3: {
        if (ldb_iter_valid(iter)) {
          if (verbose)
            fprintf(stderr, "Prev\n");

          ldb_iter_prev(iter);
          rb_iter_prev(&it);

          ASSERT(iter_equal(iter, &it));
        }

        break;
      }

      case 4: {
        if (verbose)
          fprintf(stderr, "SeekToLast\n");

        ldb_iter_last(iter);
        rb_iter_last(&it);

        ASSERT(iter_equal(iter, &it));

        break;
      }
    }
  }

  ldb_iter_destroy(iter);
}

static void
harness_test(harness_t *h, ldb_rand_t *rnd) {
  rb_map_t *data = &h->ctor->data;
  ldb_vector_t keys;

  ldb_vector_init(&keys);

  ctor_finish(h->ctor, &h->options, &keys);

  harness_test_forward_scan(h, &keys, data);
  harness_test_backward_scan(h, &keys, data);
  harness_test_random_access(h, rnd, &keys, data);

  ldb_vector_clear(&keys);
  rb_map_clear(data, map_clear);
}

static ldb_t *
harness_db(harness_t *h) {
  /* Returns NULL if not running against a DB. */
  return ctor_db(h->ctor);
}

/*
 * Tests
 */

static void
test_empty(harness_t *h) {
  ldb_rand_t rnd;
  int i;

  for (i = 0; i < num_test_args; i++) {
    harness_reset(h, &test_arg_list[i]);
    ldb_rand_init(&rnd, ldb_random_seed() + 1);
    harness_test(h, &rnd);
  }
}

/* Special test for a block with no restart entries. The C++ leveldb
   code never generates such blocks, but the Java version of leveldb
   seems to. */
static void
test_zero_restart_points_in_block(void) {
  ldb_contents_t contents;
  ldb_block_t block;
  ldb_iter_t *iter;
  ldb_slice_t key;
  uint8_t data[4];

  memset(data, 0, sizeof(data));

  contents.data = ldb_slice(data, sizeof(data));
  contents.cachable = 0;
  contents.heap_allocated = 0;

  ldb_block_init(&block, &contents);

  iter = ldb_blockiter_create(&block, ldb_bytewise_comparator);

  ldb_iter_first(iter);

  ASSERT(!ldb_iter_valid(iter));

  ldb_iter_last(iter);

  ASSERT(!ldb_iter_valid(iter));

  key = ldb_string("foo");
  ldb_iter_seek(iter, &key);

  ASSERT(!ldb_iter_valid(iter));

  ldb_iter_destroy(iter);
}

/* Test the empty key. */
static void
test_simple_empty_key(harness_t *h) {
  ldb_rand_t rnd;
  int i;

  for (i = 0; i < num_test_args; i++) {
    harness_reset(h, &test_arg_list[i]);
    ldb_rand_init(&rnd, ldb_random_seed() + 1);
    harness_add_str(h, "", "v");
    harness_test(h, &rnd);
  }
}

static void
test_simple_single(harness_t *h) {
  ldb_rand_t rnd;
  int i;

  for (i = 0; i < num_test_args; i++) {
    harness_reset(h, &test_arg_list[i]);
    ldb_rand_init(&rnd, ldb_random_seed() + 2);
    harness_add_str(h, "abc", "v");
    harness_test(h, &rnd);
  }
}

static void
test_simple_multi(harness_t *h) {
  ldb_rand_t rnd;
  int i;

  for (i = 0; i < num_test_args; i++) {
    harness_reset(h, &test_arg_list[i]);
    ldb_rand_init(&rnd, ldb_random_seed() + 3);
    harness_add_str(h, "abc", "v");
    harness_add_str(h, "abcd", "v");
    harness_add_str(h, "ac", "v2");
    harness_test(h, &rnd);
  }
}

static void
test_simple_special_key(harness_t *h) {
  ldb_rand_t rnd;
  int i;

  for (i = 0; i < num_test_args; i++) {
    harness_reset(h, &test_arg_list[i]);
    ldb_rand_init(&rnd, ldb_random_seed() + 4);
    harness_add_str(h, "\xff\xff", "v3");
    harness_test(h, &rnd);
  }
}

static void
test_randomized(harness_t *h) {
  int i, num_entries, e;
  ldb_buffer_t key, val;
  ldb_rand_t rnd;

  ldb_buffer_init(&key);
  ldb_buffer_init(&val);

  for (i = 0; i < num_test_args; i++) {
    harness_reset(h, &test_arg_list[i]);

    ldb_rand_init(&rnd, ldb_random_seed() + 5);

    for (num_entries = 0; num_entries < 2000;
         num_entries += (num_entries < 50 ? 1 : 200)) {
      if ((num_entries % 10) == 0) {
        fprintf(stderr, "case %d of %d: num_entries = %d\n", (i + 1),
                        (int)num_test_args, num_entries);
      }

      for (e = 0; e < num_entries; e++) {
        ldb_random_key(&key, &rnd, ldb_rand_skewed(&rnd, 4));
        ldb_random_string(&val, &rnd, ldb_rand_skewed(&rnd, 5));

        harness_add(h, &key, &val);
      }

      harness_test(h, &rnd);
    }
  }

  ldb_buffer_clear(&key);
  ldb_buffer_clear(&val);
}

static void
test_randomized_long_db(harness_t *h) {
  struct test_args args = {DB_TEST, 0, 16};
  int num_entries = 100000;
  ldb_buffer_t key, val;
  ldb_rand_t rnd;
  int files = 0;
  int e, level;

  ldb_buffer_init(&key);
  ldb_buffer_init(&val);

  ldb_rand_init(&rnd, ldb_random_seed());

  harness_reset(h, &args);

  for (e = 0; e < num_entries; e++) {
    ldb_random_key(&key, &rnd, ldb_rand_skewed(&rnd, 4));
    ldb_random_string(&val, &rnd, ldb_rand_skewed(&rnd, 5));

    harness_add(h, &key, &val);
  }

  harness_test(h, &rnd);

  /* We must have created enough data to force merging. */
  for (level = 0; level < LDB_NUM_LEVELS; level++) {
    char name[100];
    char *value;

    sprintf(name, "leveldb.num-files-at-level%d", level);

    ASSERT(ldb_property(harness_db(h), name, &value));

    files += atoi(value);

    ldb_free(value);
  }

  ASSERT(files > 0);

  ldb_buffer_clear(&key);
  ldb_buffer_clear(&val);
}

static void
test_memtable_simple(void) {
  ldb_memtable_t *memtable;
  ldb_comparator_t icmp;
  ldb_slice_t key, val;
  ldb_batch_t batch;
  ldb_iter_t *iter;
  char kbuf[100];
  char vbuf[100];

  ldb_ikc_init(&icmp, ldb_bytewise_comparator);

  memtable = ldb_memtable_create(&icmp);

  ldb_memtable_ref(memtable);

  ldb_batch_init(&batch);
  ldb_batch_set_sequence(&batch, 100);

  key = ldb_string("k1");
  val = ldb_string("v1");
  ldb_batch_put(&batch, &key, &val);

  key = ldb_string("k2");
  val = ldb_string("v2");
  ldb_batch_put(&batch, &key, &val);

  key = ldb_string("k3");
  val = ldb_string("v3");
  ldb_batch_put(&batch, &key, &val);

  key = ldb_string("largekey");
  val = ldb_string("vlarge");
  ldb_batch_put(&batch, &key, &val);

  ASSERT(ldb_batch_insert_into(&batch, memtable) == LDB_OK);

  iter = ldb_memiter_create(memtable);

  ldb_iter_first(iter);

  while (ldb_iter_valid(iter)) {
    key = ldb_iter_key(iter);
    val = ldb_iter_value(iter);

    ASSERT(key.size < sizeof(kbuf));
    ASSERT(val.size < sizeof(vbuf));

    memcpy(kbuf, key.data, key.size);
    memcpy(vbuf, val.data, val.size);

    kbuf[key.size] = '\0';
    vbuf[val.size] = '\0';

    fprintf(stderr, "key: '%s' -> '%s'\n", kbuf, vbuf);

    ldb_iter_next(iter);
  }

  ldb_iter_destroy(iter);
  ldb_memtable_unref(memtable);
  ldb_batch_clear(&batch);
}

static int
check_range(uint64_t val, uint64_t low, uint64_t high) {
  int result = (val >= low) && (val <= high);

  if (!result) {
    fprintf(stderr, "Value %lu is not in range [%lu, %lu]\n",
                    (unsigned long)(val), (unsigned long)(low),
                    (unsigned long)(high));
  }

  return result;
}

static void
test_approximate_offset_plain(void) {
  ctor_t *c = tablector_create(ldb_bytewise_comparator);
  ldb_dbopt_t options = *ldb_dbopt_default;
  uint8_t *buf = ldb_malloc(300000);
  ldb_slice_t key, val;
  ldb_vector_t keys;

  ldb_vector_init(&keys);

  memset(buf, 'x', 300000);

  ctor_add_str(c, "k01", "hello");
  ctor_add_str(c, "k02", "hello2");

  key = ldb_string("k03");
  val = ldb_slice(buf, 10000);
  ctor_add(c, &key, &val);

  key = ldb_string("k04");
  val = ldb_slice(buf, 200000);
  ctor_add(c, &key, &val);

  key = ldb_string("k05");
  val = ldb_slice(buf, 300000);
  ctor_add(c, &key, &val);

  ctor_add_str(c, "k06", "hello3");

  key = ldb_string("k07");
  val = ldb_slice(buf, 100000);
  ctor_add(c, &key, &val);

  options.block_size = 1024;
  options.compression = LDB_NO_COMPRESSION;
  options.comparator = ldb_bytewise_comparator;

  ctor_finish(c, &options, &keys);

  ASSERT(check_range(ctor_approximate_offset(c, "abc"), 0, 0));
  ASSERT(check_range(ctor_approximate_offset(c, "k01"), 0, 0));
  ASSERT(check_range(ctor_approximate_offset(c, "k01a"), 0, 0));
  ASSERT(check_range(ctor_approximate_offset(c, "k02"), 0, 0));
  ASSERT(check_range(ctor_approximate_offset(c, "k03"), 0, 0));
  ASSERT(check_range(ctor_approximate_offset(c, "k04"), 10000, 11000));
  ASSERT(check_range(ctor_approximate_offset(c, "k04a"), 210000, 211000));
  ASSERT(check_range(ctor_approximate_offset(c, "k05"), 210000, 211000));
  ASSERT(check_range(ctor_approximate_offset(c, "k06"), 510000, 511000));
  ASSERT(check_range(ctor_approximate_offset(c, "k07"), 510000, 511000));
  ASSERT(check_range(ctor_approximate_offset(c, "xyz"), 610000, 612000));

  ldb_vector_clear(&keys);
  ldb_free(buf);
  ctor_destroy(c);
}

static void
test_approximate_offset_compressed(void) {
  ctor_t *c = tablector_create(ldb_bytewise_comparator);
  ldb_dbopt_t options = *ldb_dbopt_default;
  ldb_vector_t keys;
  ldb_buffer_t val;
  ldb_slice_t key;
  ldb_rand_t rnd;

  /* Expected upper and lower bounds of space used by compressible strings. */
  static const int slop = 1000; /* Compressor effectiveness varies. */
  const int expected = 2500;    /* 10000 * compression ratio (0.25). */
  const int min_z = expected - slop;
  const int max_z = expected + slop;

  ldb_rand_init(&rnd, 301);

  ldb_buffer_init(&val);
  ldb_vector_init(&keys);

  ctor_add_str(c, "k01", "hello");

  key = ldb_string("k02");
  ldb_compressible_string(&val, &rnd, 0.25, 10000);
  ctor_add(c, &key, &val);

  ctor_add_str(c, "k03", "hello3");

  key = ldb_string("k04");
  ldb_compressible_string(&val, &rnd, 0.25, 10000);
  ctor_add(c, &key, &val);

  options.block_size = 1024;
  options.compression = LDB_SNAPPY_COMPRESSION;
  options.comparator = ldb_bytewise_comparator;

  ctor_finish(c, &options, &keys);

  ASSERT(check_range(ctor_approximate_offset(c, "abc"), 0, slop));
  ASSERT(check_range(ctor_approximate_offset(c, "k01"), 0, slop));
  ASSERT(check_range(ctor_approximate_offset(c, "k02"), 0, slop));
  /* Emitted a large compressible string, so adjust expected offset. */
  ASSERT(check_range(ctor_approximate_offset(c, "k03"), min_z, max_z));
  ASSERT(check_range(ctor_approximate_offset(c, "k04"), min_z, max_z));
  /* Emitted two large compressible strings, so adjust expected offset. */
  ASSERT(check_range(ctor_approximate_offset(c, "xyz"), 2 * min_z,
                                                        2 * max_z));

  ldb_vector_clear(&keys);
  ldb_buffer_clear(&val);
  ctor_destroy(c);
}

/*
 * Execute
 */

int
main(void) {
  harness_t h;

  harness_init(&h);

  test_empty(&h);
  test_zero_restart_points_in_block();
  test_simple_empty_key(&h);
  test_simple_single(&h);
  test_simple_multi(&h);
  test_simple_special_key(&h);
  test_randomized(&h);
  test_randomized_long_db(&h);
  test_memtable_simple();
  test_approximate_offset_plain();
  test_approximate_offset_compressed();

  harness_clear(&h);

  return 0;
}
