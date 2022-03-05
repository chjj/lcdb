/*!
 * table_test.c - table test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#undef NDEBUG

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../util/buffer.h"
#include "../util/comparator.h"
#include "../util/env.h"
#include "../util/extern.h"
#include "../util/internal.h"
#include "../util/options.h"
#include "../util/random.h"
#include "../util/rbt.h"
#include "../util/slice.h"
#include "../util/status.h"
#include "../util/strutil.h"
#include "../util/testutil.h"
#include "../util/vector.h"

#include "../dbformat.h"
#include "../db_impl.h"
#include "../memtable.h"
#include "../write_batch.h"

#include "block_builder.h"
#include "block.h"
#include "format.h"
#include "iterator.h"
#include "table_builder.h"
#include "table.h"

/*
 * ReverseKeyComparator
 */

static rdb_buffer_t *
slice_reverse(rdb_buffer_t *z, const rdb_slice_t *x) {
  const uint8_t *xp = x->data;
  size_t xn = x->size;
  uint8_t *zp;

  zp = rdb_buffer_resize(z, xn);

  xp += xn;

  while (xn--)
    *zp++ = *--xp;

  return z;
}

static int
rev_compare(const rdb_comparator_t *cmp,
            const rdb_slice_t *x,
            const rdb_slice_t *y) {
  rdb_buffer_t a, b;
  int r;

  (void)cmp;

  rdb_buffer_init(&a);
  rdb_buffer_init(&b);

  slice_reverse(&a, x);
  slice_reverse(&b, y);

  r = rdb_compare(rdb_bytewise_comparator, &a, &b);

  rdb_buffer_clear(&a);
  rdb_buffer_clear(&b);

  return r;
}

static void
rev_shortest_separator(const rdb_comparator_t *cmp,
                       rdb_buffer_t *start,
                       const rdb_slice_t *limit) {
  rdb_buffer_t s, l;

  (void)cmp;

  rdb_buffer_init(&s);
  rdb_buffer_init(&l);

  slice_reverse(&s, start);
  slice_reverse(&l, limit);

  rdb_shortest_separator(rdb_bytewise_comparator, &s, &l);

  slice_reverse(start, &s);

  rdb_buffer_clear(&s);
  rdb_buffer_clear(&l);
}

static void
rev_short_successor(const rdb_comparator_t *cmp, rdb_buffer_t *key) {
  rdb_buffer_t s;

  (void)cmp;

  rdb_buffer_init(&s);

  slice_reverse(&s, key);

  rdb_short_successor(rdb_bytewise_comparator, &s);

  slice_reverse(key, &s);

  rdb_buffer_clear(&s);
}

static const rdb_comparator_t reverse_comparator = {
  /* .name = */ "leveldb.ReverseBytewiseComparator",
  /* .compare = */ rev_compare,
  /* .shortest_separator = */ rev_shortest_separator,
  /* .short_successor = */ rev_short_successor,
  /* .user_comparator = */ NULL
};

static void
cmp_increment(const rdb_comparator_t *cmp, rdb_buffer_t *key) {
  if (cmp == rdb_bytewise_comparator) {
    rdb_buffer_push(key, '\0');
  } else {
    rdb_buffer_t rev;

    assert(cmp == &reverse_comparator);

    rdb_buffer_init(&rev);

    slice_reverse(&rev, key);

    rdb_buffer_push(&rev, '\0');

    slice_reverse(key, &rev);

    rdb_buffer_clear(&rev);
  }
}

/*
 * STLLessThan
 */

/* An STL comparator that uses a Comparator. */
static int
map_compare(rb_val_t x, rb_val_t y, void *arg) {
  const rdb_comparator_t *cmp = arg;
  rdb_buffer_t *a = x.p;
  rdb_buffer_t *b = y.p;

  return rdb_compare(cmp, a, b);
}

static void
map_clear(rb_node_t *node) {
  rdb_buffer_clear(node->key.p);
  rdb_buffer_clear(node->value.p);

  rdb_free(node->key.p);
  rdb_free(node->value.p);
}

/*
 * StringSink
 */

/* Not implemented. */

/*
 * StringSource
 */

/* Not implemented. */

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
                const rdb_dbopt_t *options,
                const rb_map_t *data);
  rdb_iter_t *(*iterator)(const void *ctor);
  rdb_t *(*db)(const void *ctor);
};

static void
ctor_init(ctor_t *c,
          void *ctor,
          const ctortbl_t *tbl,
          const rdb_comparator_t *cmp) {
  c->ptr = ctor;
  c->table = tbl;
  rb_map_init(&c->data, map_compare, (void *)cmp);
}

static void
ctor_clear(ctor_t *c) {
  c->table->clear(c->ptr);
  rb_map_clear(&c->data, map_clear);
  rdb_free(c->ptr);
}

static ctor_t *
ctor_create(void *ctor, const ctortbl_t *tbl, const rdb_comparator_t *cmp) {
  ctor_t *c = rdb_malloc(sizeof(ctor_t));
  ctor_init(c, ctor, tbl, cmp);
  return c;
}

static void
ctor_destroy(ctor_t *c) {
  ctor_clear(c);
  rdb_free(c);
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
                        const rdb_dbopt_t *options,            \
                        const rb_map_t *data) {                \
  return name ## _finish((name ## _t *)ctor, options, data);   \
}                                                              \
                                                               \
static rdb_iter_t *                                            \
name ## _iterator_wrapped(const void *ctor) {                  \
  return name ## _iterator((const name ## _t *)ctor);          \
}                                                              \
                                                               \
static rdb_t *                                                 \
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
ctor_add(ctor_t *c, const rdb_slice_t *key, const rdb_slice_t *value) {
  rdb_buffer_t *k;
  rdb_buffer_t *v;

  v = rb_map_get(&c->data, key);

  if (v != NULL) {
    rdb_buffer_copy(v, value);
  } else {
    k = rdb_malloc(sizeof(rdb_buffer_t));
    v = rdb_malloc(sizeof(rdb_buffer_t));

    rdb_buffer_init(k);
    rdb_buffer_init(v);

    rdb_buffer_copy(k, key);
    rdb_buffer_copy(v, value);

    assert(rb_map_put(&c->data, k, v));
  }
}

static void
ctor_add_str(ctor_t *c, const char *key, const char *value) {
  rdb_slice_t k = rdb_string(key);
  rdb_slice_t v = rdb_string(value);

  ctor_add(c, &k, &v);
}

/* Finish constructing the data structure with all the keys that have
   been added so far. Returns the keys in sorted order in "*keys"
   and stores the key/value pairs in "*kvmap" */
static void
ctor_finish(ctor_t *c, const rdb_dbopt_t *options, rdb_vector_t *keys) {
  void *key;
  int rc;

  assert(keys->length == 0);

  rb_map_keys(&c->data, key)
    rdb_vector_push(keys, key);

  rc = c->table->finish(c->ptr, options, &c->data);

  assert(rc == RDB_OK);
}

#define ctor_iterator(ctor) (ctor)->table->iterator((ctor)->ptr)
#define ctor_db(ctor) (ctor)->table->db((ctor)->ptr)

/*
 * BlockConstructor
 */

typedef struct blockctor_s {
  const rdb_comparator_t *comparator;
  rdb_buffer_t data;
  rdb_block_t *block;
} blockctor_t;

static void
blockctor_init(blockctor_t *c, const rdb_comparator_t *cmp) {
  c->comparator = cmp;

  rdb_buffer_init(&c->data);

  c->block = NULL;
}

static void
blockctor_clear(blockctor_t *c) {
  rdb_buffer_clear(&c->data);

  if (c->block != NULL)
    rdb_block_destroy(c->block);
}

static int
blockctor_finish(blockctor_t *c,
                 const rdb_dbopt_t *options,
                 const rb_map_t *data) {
  rdb_blockcontents_t contents;
  rdb_blockbuilder_t bb;
  void *key, *value;
  rdb_slice_t ret;

  if (c->block != NULL)
    rdb_block_destroy(c->block);

  c->block = NULL;

  rdb_blockbuilder_init(&bb, options);

  rb_map_iterate(data, key, value)
    rdb_blockbuilder_add(&bb, key, value);

  /* Open the block. */
  ret = rdb_blockbuilder_finish(&bb);
  rdb_buffer_copy(&c->data, &ret);

  contents.data = c->data;
  contents.cachable = 0;
  contents.heap_allocated = 0;

  c->block = rdb_block_create(&contents);

  rdb_blockbuilder_clear(&bb);

  return RDB_OK;
}

static rdb_iter_t *
blockctor_iterator(const blockctor_t *c) {
  assert(c->block != NULL);
  return rdb_blockiter_create(c->block, c->comparator);
}

static rdb_t *
blockctor_db(const blockctor_t *c) {
  (void)c;
  return NULL;
}

CTOR_FUNCTIONS(blockctor);

static ctor_t *
blockctor_create(const rdb_comparator_t *cmp) {
  blockctor_t *c = rdb_malloc(sizeof(blockctor_t));

  blockctor_init(c, cmp);

  return ctor_create(c, &blockctor_table, cmp);
}

/*
 * TableConstructor
 */

typedef struct tablector_s {
  char path[RDB_PATH_MAX];
  rdb_rfile_t *source;
  rdb_table_t *table;
} tablector_t;


static void
tablector_init(tablector_t *c) {
  assert(rdb_test_directory(c->path, sizeof(c->path)));
  assert(rdb_join(c->path, sizeof(c->path), c->path, "test_table.ldb"));

  c->source = NULL;
  c->table = NULL;
}

static void
tablector_clear(tablector_t *c) {
  if (c->table != NULL)
    rdb_table_destroy(c->table);

  if (c->source != NULL) {
    rdb_rfile_destroy(c->source);
    rdb_remove_file(c->path);
  }

  c->table = NULL;
  c->source = NULL;
}

static int
tablector_finish(tablector_t *c,
                 const rdb_dbopt_t *options,
                 const rb_map_t *data) {
  rdb_dbopt_t table_options = *rdb_dbopt_default;
  rdb_tablebuilder_t *tb;
  rdb_wfile_t *sink;
  void *key, *value;
  uint64_t fsize;

  tablector_clear(c);

  assert(rdb_truncfile_create(c->path, &sink) == RDB_OK);

  tb = rdb_tablebuilder_create(options, sink);

  rb_map_iterate(data, key, value) {
    rdb_tablebuilder_add(tb, key, value);

    assert(rdb_tablebuilder_ok(tb));
  }

  assert(rdb_tablebuilder_finish(tb) == RDB_OK);
  assert(rdb_wfile_close(sink) == RDB_OK);

  rdb_wfile_destroy(sink);

  assert(rdb_get_file_size(c->path, &fsize) == RDB_OK);
  assert(fsize == rdb_tablebuilder_file_size(tb));

  rdb_tablebuilder_destroy(tb);

  /* Open the table. */
  assert(rdb_seqfile_create(c->path, &c->source) == RDB_OK);

  table_options.comparator = options->comparator;

  return rdb_table_open(&table_options, c->source, fsize, &c->table);
}

static rdb_iter_t *
tablector_iterator(const tablector_t *c) {
  return rdb_tableiter_create(c->table, rdb_readopt_default);
}

static rdb_t *
tablector_db(const tablector_t *c) {
  (void)c;
  return NULL;
}

static uint64_t
ctor_approximate_offsetof(const ctor_t *ctor, const char *key) {
  const tablector_t *c = ctor->ptr;
  rdb_slice_t k = rdb_string(key);

  return rdb_table_approximate_offsetof(c->table, &k);
}

CTOR_FUNCTIONS(tablector);

static ctor_t *
tablector_create(const rdb_comparator_t *cmp) {
  tablector_t *c = rdb_malloc(sizeof(tablector_t));

  tablector_init(c);

  return ctor_create(c, &tablector_table, cmp);
}

/*
 * KeyConvertingIterator
 */

/* A helper class that converts internal format keys into user keys. */
typedef struct conviter_s {
  rdb_iter_t *it;
  int status;
} conviter_t;

static void
conviter_init(conviter_t *iter, rdb_iter_t *it) {
  iter->it = it;
  iter->status = RDB_OK;
}

static void
conviter_clear(conviter_t *iter) {
  rdb_iter_destroy(iter->it);
}

static int
conviter_valid(const conviter_t *iter) {
  return rdb_iter_valid(iter->it);
}

static void
conviter_seek(conviter_t *iter, const rdb_slice_t *target) {
  rdb_buffer_t encoded;
  rdb_pkey_t ikey;

  rdb_buffer_init(&encoded);

  rdb_pkey_init(&ikey, target, RDB_MAX_SEQUENCE, RDB_TYPE_VALUE);
  rdb_pkey_export(&encoded, &ikey);

  rdb_iter_seek(iter->it, &encoded);

  rdb_buffer_clear(&encoded);
}

static void
conviter_seek_first(conviter_t *iter) {
  rdb_iter_seek_first(iter->it);
}

static void
conviter_seek_last(conviter_t *iter) {
  rdb_iter_seek_last(iter->it);
}

static void
conviter_next(conviter_t *iter) {
  rdb_iter_next(iter->it);
}

static void
conviter_prev(conviter_t *iter) {
  rdb_iter_prev(iter->it);
}

static rdb_slice_t
conviter_key(const conviter_t *iter) {
  rdb_pkey_t key;
  rdb_slice_t k;

  assert(conviter_valid(iter));

  k = rdb_iter_key(iter->it);

  if (!rdb_pkey_import(&key, &k)) {
    ((conviter_t *)iter)->status = RDB_CORRUPTION;
    return rdb_string("corrupted key");
  }

  return key.user_key;
}

static rdb_slice_t
conviter_value(const conviter_t *iter) {
  return rdb_iter_value(iter->it);
}

static int
conviter_status(const conviter_t *iter) {
  if (iter->status != RDB_OK)
    return iter->status;

  return rdb_iter_status(iter->it);
}

RDB_ITERATOR_FUNCTIONS(conviter);

static rdb_iter_t *
conviter_create(rdb_iter_t *it) {
  conviter_t *iter = rdb_malloc(sizeof(conviter_t));

  conviter_init(iter, it);

  return rdb_iter_create(iter, &conviter_table);
}

/*
 * MemTableConstructor
 */

typedef struct memctor_s {
  rdb_comparator_t icmp;
  rdb_memtable_t *mt;
} memctor_t;

static void
memctor_init(memctor_t *c, const rdb_comparator_t *cmp) {
  rdb_ikc_init(&c->icmp, cmp);

  c->mt = rdb_memtable_create(&c->icmp);

  rdb_memtable_ref(c->mt);
}

static void
memctor_clear(memctor_t *c) {
  rdb_memtable_unref(c->mt);
}

static int
memctor_finish(memctor_t *c,
               const rdb_dbopt_t *options,
               const rb_map_t *data) {
  void *key, *value;
  int seq = 1;

  (void)options;

  rdb_memtable_unref(c->mt);

  c->mt = rdb_memtable_create(&c->icmp);

  rdb_memtable_ref(c->mt);

  rb_map_iterate(data, key, value) {
    rdb_memtable_add(c->mt, seq, RDB_TYPE_VALUE, key, value);
    seq++;
  }

  return RDB_OK;
}

static rdb_iter_t *
memctor_iterator(const memctor_t *c) {
  return conviter_create(rdb_memiter_create(c->mt));
}

static rdb_t *
memctor_db(const memctor_t *c) {
  (void)c;
  return NULL;
}

CTOR_FUNCTIONS(memctor);

static ctor_t *
memctor_create(const rdb_comparator_t *cmp) {
  memctor_t *c = rdb_malloc(sizeof(memctor_t));

  memctor_init(c, cmp);

  return ctor_create(c, &memctor_table, cmp);
}

/*
 * DBConstructor
 */

typedef struct dbctor_s {
  const rdb_comparator_t *cmp;
  rdb_t *db;
} dbctor_t;

static void
dbctor_newdb(dbctor_t *c) {
  rdb_dbopt_t options = *rdb_dbopt_default;
  char name[RDB_PATH_MAX];
  int rc;

  assert(rdb_test_directory(name, sizeof(name)));
  assert(rdb_join(name, sizeof(name), name, "table_testdb"));

  options.comparator = c->cmp;

  rc = rdb_destroy_db(name, &options);

  assert(rc == RDB_OK);

  options.create_if_missing = 1;
  options.error_if_exists = 1;
  options.write_buffer_size = 10000; /* Something small to force merging. */

  rc = rdb_open(name, &options, &c->db);

  assert(rc == RDB_OK);
}

static void
dbctor_init(dbctor_t *c, const rdb_comparator_t *cmp) {
  c->cmp = cmp;
  c->db = NULL;

  dbctor_newdb(c);
}

static void
dbctor_clear(dbctor_t *c) {
  char name[RDB_PATH_MAX];

  assert(rdb_test_directory(name, sizeof(name)));
  assert(rdb_join(name, sizeof(name), name, "table_testdb"));

  rdb_destroy_db(name, 0);

  rdb_close(c->db);
}

static int
dbctor_finish(dbctor_t *c,
              const rdb_dbopt_t *options,
              const rb_map_t *data) {
  void *key, *value;

  (void)options;

  rdb_close(c->db);

  c->db = NULL;

  dbctor_newdb(c);

  rb_map_iterate(data, key, value) {
    rdb_batch_t batch;

    rdb_batch_init(&batch);
    rdb_batch_put(&batch, key, value);

    assert(rdb_write(c->db, &batch, 0) == RDB_OK);

    rdb_batch_clear(&batch);
  }

  return RDB_OK;
}

static rdb_iter_t *
dbctor_iterator(const dbctor_t *c) {
  return rdb_iterator(c->db, rdb_readopt_default);
}

static rdb_t *
dbctor_db(const dbctor_t *c) {
  return c->db;
}

CTOR_FUNCTIONS(dbctor);

static ctor_t *
dbctor_create(const rdb_comparator_t *cmp) {
  dbctor_t *c = rdb_malloc(sizeof(dbctor_t));

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
  rdb_dbopt_t options;
  ctor_t *ctor;
} harness_t;

static void
harness_init(harness_t *h) {
  h->options = *rdb_dbopt_default;
  h->options.comparator = rdb_bytewise_comparator;
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

  h->options = *rdb_dbopt_default;
  h->ctor = NULL;

  h->options.block_restart_interval = args->restart_interval;

  /* Use shorter block size for tests to exercise
     block boundary conditions more. */
  h->options.block_size = 256;

  if (args->reverse_compare)
    h->options.comparator = &reverse_comparator;
  else
    h->options.comparator = rdb_bytewise_comparator;

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
harness_add(harness_t *h, const rdb_slice_t *key, const rdb_slice_t *value) {
  ctor_add(h->ctor, key, value);
}

static void
harness_add_str(harness_t *h, const char *key, const char *value) {
  ctor_add_str(h->ctor, key, value);
}

static void
harness_random_key(harness_t *h,
                   rdb_buffer_t *result,
                   rdb_rand_t *rnd,
                   const rdb_vector_t *keys) {
  size_t index;

  if (keys->length == 0) {
    rdb_buffer_set_str(result, "foo");
    return;
  }

  index = rdb_rand_uniform(rnd, keys->length);

  rdb_buffer_copy(result, keys->items[index]);

  switch (rdb_rand_uniform(rnd, 3)) {
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

  return rdb_buffer_equal(&k1, k2.p)
      && rdb_buffer_equal(&v1, v2.p);
}

static void
harness_test_forward_scan(harness_t *h,
                          const rdb_vector_t *keys,
                          const rb_map_t *data) {
  rdb_iter_t *iter = ctor_iterator(h->ctor);
  rb_iter_t it = rb_tree_iterator(data);

  (void)keys;

  assert(!rdb_iter_valid(iter));

  rdb_iter_seek_first(iter);
  rb_iter_seek_first(&it);

  while (rb_iter_valid(&it)) {
    assert(rdb_iter_valid(iter));
    assert(iter_equal(iter, &it));

    rdb_iter_next(iter);
    rb_iter_next(&it);
  }

  assert(!rdb_iter_valid(iter));

  rdb_iter_destroy(iter);
}

static void
harness_test_backward_scan(harness_t *h,
                           const rdb_vector_t *keys,
                           const rb_map_t *data) {
  rdb_iter_t *iter = ctor_iterator(h->ctor);
  rb_iter_t it = rb_tree_iterator(data);

  (void)keys;

  assert(!rdb_iter_valid(iter));

  rdb_iter_seek_last(iter);
  rb_iter_seek_last(&it);

  while (rb_iter_valid(&it)) {
    assert(rdb_iter_valid(iter));
    assert(iter_equal(iter, &it));

    rdb_iter_prev(iter);
    rb_iter_prev(&it);
  }

  assert(!rdb_iter_valid(iter));

  rdb_iter_destroy(iter);
}

static void
harness_test_random_access(harness_t *h,
                           rdb_rand_t *rnd,
                           const rdb_vector_t *keys,
                           const rb_map_t *data) {
  static const int verbose = 0;
  rdb_iter_t *iter = ctor_iterator(h->ctor);
  rb_iter_t it = rb_tree_iterator(data);
  int i;

  assert(!rdb_iter_valid(iter));

  rb_iter_seek_first(&it);

  if (verbose)
    fprintf(stderr, "---\n");

  for (i = 0; i < 200; i++) {
    const int toss = rdb_rand_uniform(rnd, 5);

    switch (toss) {
      case 0: {
        if (rdb_iter_valid(iter)) {
          if (verbose)
            fprintf(stderr, "Next\n");

          rdb_iter_next(iter);
          rb_iter_next(&it);

          assert(iter_equal(iter, &it));
        }

        break;
      }

      case 1: {
        if (verbose)
          fprintf(stderr, "SeekToFirst\n");

        rdb_iter_seek_first(iter);
        rb_iter_seek_first(&it);

        assert(iter_equal(iter, &it));

        break;
      }

      case 2: {
        rdb_buffer_t key;
        rb_val_t k;

        k.p = &key;

        if (verbose)
          fprintf(stderr, "Seek\n");

        rdb_buffer_init(&key);

        harness_random_key(h, &key, rnd, keys);

        rdb_iter_seek(iter, &key);
        rb_iter_seek(&it, k);

        assert(iter_equal(iter, &it));

        rdb_buffer_clear(&key);

        break;
      }

      case 3: {
        if (rdb_iter_valid(iter)) {
          if (verbose)
            fprintf(stderr, "Prev\n");

          rdb_iter_prev(iter);
          rb_iter_prev(&it);

          assert(iter_equal(iter, &it));
        }

        break;
      }

      case 4: {
        if (verbose)
          fprintf(stderr, "SeekToLast\n");

        rdb_iter_seek_last(iter);
        rb_iter_seek_last(&it);

        assert(iter_equal(iter, &it));

        break;
      }
    }
  }

  rdb_iter_destroy(iter);
}

static void
harness_test(harness_t *h, rdb_rand_t *rnd) {
  rb_map_t *data = &h->ctor->data;
  rdb_vector_t keys;

  rdb_vector_init(&keys);

  ctor_finish(h->ctor, &h->options, &keys);

  harness_test_forward_scan(h, &keys, data);
  harness_test_backward_scan(h, &keys, data);
  harness_test_random_access(h, rnd, &keys, data);

  rdb_vector_clear(&keys);
  rb_map_clear(data, map_clear);
}

static rdb_t *
harness_db(harness_t *h) {
  /* Returns nullptr if not running against a DB. */
  return ctor_db(h->ctor);
}

/*
 * Tests
 */

static void
test_empty(harness_t *h) {
  rdb_rand_t rnd;
  int i;

  for (i = 0; i < num_test_args; i++) {
    harness_reset(h, &test_arg_list[i]);
    rdb_rand_init(&rnd, rdb_random_seed() + 1);
    harness_test(h, &rnd);
  }
}

/* Special test for a block with no restart entries. The C++ leveldb
   code never generates such blocks, but the Java version of leveldb
   seems to. */
static void
test_zero_restart_points_in_block(void) {
  rdb_blockcontents_t contents;
  rdb_block_t block;
  rdb_iter_t *iter;
  rdb_slice_t key;
  uint8_t data[4];

  memset(data, 0, sizeof(data));

  contents.data = rdb_slice(data, sizeof(data));
  contents.cachable = 0;
  contents.heap_allocated = 0;

  rdb_block_init(&block, &contents);

  iter = rdb_blockiter_create(&block, rdb_bytewise_comparator);

  rdb_iter_seek_first(iter);

  assert(!rdb_iter_valid(iter));

  rdb_iter_seek_last(iter);

  assert(!rdb_iter_valid(iter));

  key = rdb_string("foo");
  rdb_iter_seek(iter, &key);

  assert(!rdb_iter_valid(iter));

  rdb_iter_destroy(iter);
}

/* Test the empty key. */
static void
test_simple_empty_key(harness_t *h) {
  rdb_rand_t rnd;
  int i;

  for (i = 0; i < num_test_args; i++) {
    harness_reset(h, &test_arg_list[i]);
    rdb_rand_init(&rnd, rdb_random_seed() + 1);
    harness_add_str(h, "", "v");
    harness_test(h, &rnd);
  }
}

static void
test_simple_single(harness_t *h) {
  rdb_rand_t rnd;
  int i;

  for (i = 0; i < num_test_args; i++) {
    harness_reset(h, &test_arg_list[i]);
    rdb_rand_init(&rnd, rdb_random_seed() + 2);
    harness_add_str(h, "abc", "v");
    harness_test(h, &rnd);
  }
}

static void
test_simple_multi(harness_t *h) {
  rdb_rand_t rnd;
  int i;

  for (i = 0; i < num_test_args; i++) {
    harness_reset(h, &test_arg_list[i]);
    rdb_rand_init(&rnd, rdb_random_seed() + 3);
    harness_add_str(h, "abc", "v");
    harness_add_str(h, "abcd", "v");
    harness_add_str(h, "ac", "v2");
    harness_test(h, &rnd);
  }
}

static void
test_simple_special_key(harness_t *h) {
  rdb_rand_t rnd;
  int i;

  for (i = 0; i < num_test_args; i++) {
    harness_reset(h, &test_arg_list[i]);
    rdb_rand_init(&rnd, rdb_random_seed() + 4);
    harness_add_str(h, "\xff\xff", "v3");
    harness_test(h, &rnd);
  }
}

static void
test_randomized(harness_t *h) {
  int i, num_entries, e;
  rdb_buffer_t key, val;
  rdb_rand_t rnd;

  rdb_buffer_init(&key);
  rdb_buffer_init(&val);

  for (i = 0; i < num_test_args; i++) {
    harness_reset(h, &test_arg_list[i]);

    rdb_rand_init(&rnd, rdb_random_seed() + 5);

    for (num_entries = 0; num_entries < 2000;
         num_entries += (num_entries < 50 ? 1 : 200)) {
      if ((num_entries % 10) == 0) {
        fprintf(stderr, "case %d of %d: num_entries = %d\n", (i + 1),
                        (int)num_test_args, num_entries);
      }

      for (e = 0; e < num_entries; e++) {
        rdb_random_key(&key, &rnd, rdb_rand_skewed(&rnd, 4));
        rdb_random_string(&val, &rnd, rdb_rand_skewed(&rnd, 5));

        harness_add(h, &key, &val);
      }

      harness_test(h, &rnd);
    }
  }

  rdb_buffer_clear(&key);
  rdb_buffer_clear(&val);
}

static void
test_randomized_long_db(harness_t *h) {
  struct test_args args = {DB_TEST, 0, 16};
  int num_entries = 100000;
  rdb_buffer_t key, val;
  rdb_rand_t rnd;
  int files = 0;
  int e, level;

  rdb_buffer_init(&key);
  rdb_buffer_init(&val);

  rdb_rand_init(&rnd, rdb_random_seed());

  harness_reset(h, &args);

  for (e = 0; e < num_entries; e++) {
    rdb_random_key(&key, &rnd, rdb_rand_skewed(&rnd, 4));
    rdb_random_string(&val, &rnd, rdb_rand_skewed(&rnd, 5));

    harness_add(h, &key, &val);
  }

  harness_test(h, &rnd);

  /* We must have created enough data to force merging. */
  for (level = 0; level < RDB_NUM_LEVELS; level++) {
    char name[100];
    char *value;

    sprintf(name, "leveldb.num-files-at-level%d", level);

    assert(rdb_get_property(harness_db(h), name, &value));

    files += atoi(value);

    rdb_free(value);
  }

  assert(files > 0);

  rdb_buffer_clear(&key);
  rdb_buffer_clear(&val);
}

static void
test_memtable_simple(void) {
  rdb_memtable_t *memtable;
  rdb_comparator_t icmp;
  rdb_slice_t key, val;
  rdb_batch_t batch;
  rdb_iter_t *iter;
  char kbuf[100];
  char vbuf[100];

  rdb_ikc_init(&icmp, rdb_bytewise_comparator);

  memtable = rdb_memtable_create(&icmp);

  rdb_memtable_ref(memtable);

  rdb_batch_init(&batch);
  rdb_batch_set_sequence(&batch, 100);

  key = rdb_string("k1");
  val = rdb_string("v1");
  rdb_batch_put(&batch, &key, &val);

  key = rdb_string("k2");
  val = rdb_string("v2");
  rdb_batch_put(&batch, &key, &val);

  key = rdb_string("k3");
  val = rdb_string("v3");
  rdb_batch_put(&batch, &key, &val);

  key = rdb_string("largekey");
  val = rdb_string("vlarge");
  rdb_batch_put(&batch, &key, &val);

  assert(rdb_batch_insert_into(&batch, memtable) == RDB_OK);

  iter = rdb_memiter_create(memtable);

  rdb_iter_seek_first(iter);

  while (rdb_iter_valid(iter)) {
    key = rdb_iter_key(iter);
    val = rdb_iter_value(iter);

    assert(key.size < sizeof(kbuf));
    assert(val.size < sizeof(vbuf));

    memcpy(kbuf, key.data, key.size);
    memcpy(vbuf, val.data, val.size);

    kbuf[key.size] = '\0';
    vbuf[val.size] = '\0';

    fprintf(stderr, "key: '%s' -> '%s'\n", kbuf, vbuf);

    rdb_iter_next(iter);
  }

  rdb_iter_destroy(iter);
  rdb_memtable_unref(memtable);
  rdb_batch_clear(&batch);
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
test_approximate_offsetof_plain(void) {
  ctor_t *c = tablector_create(rdb_bytewise_comparator);
  rdb_dbopt_t options = *rdb_dbopt_default;
  uint8_t *buf = rdb_malloc(300000);
  rdb_slice_t key, val;
  rdb_vector_t keys;

  rdb_vector_init(&keys);

  memset(buf, 'x', 300000);

  ctor_add_str(c, "k01", "hello");
  ctor_add_str(c, "k02", "hello2");

  key = rdb_string("k03");
  val = rdb_slice(buf, 10000);
  ctor_add(c, &key, &val);

  key = rdb_string("k04");
  val = rdb_slice(buf, 200000);
  ctor_add(c, &key, &val);

  key = rdb_string("k05");
  val = rdb_slice(buf, 300000);
  ctor_add(c, &key, &val);

  ctor_add_str(c, "k06", "hello3");

  key = rdb_string("k07");
  val = rdb_slice(buf, 100000);
  ctor_add(c, &key, &val);

  options.block_size = 1024;
  options.compression = RDB_NO_COMPRESSION;
  options.comparator = rdb_bytewise_comparator;

  ctor_finish(c, &options, &keys);

  assert(check_range(ctor_approximate_offsetof(c, "abc"), 0, 0));
  assert(check_range(ctor_approximate_offsetof(c, "k01"), 0, 0));
  assert(check_range(ctor_approximate_offsetof(c, "k01a"), 0, 0));
  assert(check_range(ctor_approximate_offsetof(c, "k02"), 0, 0));
  assert(check_range(ctor_approximate_offsetof(c, "k03"), 0, 0));
  assert(check_range(ctor_approximate_offsetof(c, "k04"), 10000, 11000));
  assert(check_range(ctor_approximate_offsetof(c, "k04a"), 210000, 211000));
  assert(check_range(ctor_approximate_offsetof(c, "k05"), 210000, 211000));
  assert(check_range(ctor_approximate_offsetof(c, "k06"), 510000, 511000));
  assert(check_range(ctor_approximate_offsetof(c, "k07"), 510000, 511000));
  assert(check_range(ctor_approximate_offsetof(c, "xyz"), 610000, 612000));

  rdb_vector_clear(&keys);
  rdb_free(buf);
  ctor_destroy(c);
}

static void
test_approximate_offsetof_compressed(void) {
  ctor_t *c = tablector_create(rdb_bytewise_comparator);
  rdb_dbopt_t options = *rdb_dbopt_default;
  rdb_vector_t keys;
  rdb_buffer_t val;
  rdb_slice_t key;
  rdb_rand_t rnd;

  /* Expected upper and lower bounds of space used by compressible strings. */
  static const int slop = 1000; /* Compressor effectiveness varies. */
  const int expected = 2500;    /* 10000 * compression ratio (0.25). */
  const int min_z = expected - slop;
  const int max_z = expected + slop;

  rdb_rand_init(&rnd, 301);

  rdb_buffer_init(&val);
  rdb_vector_init(&keys);

  ctor_add_str(c, "k01", "hello");

  key = rdb_string("k02");
  rdb_compressible_string(&val, &rnd, 0.25, 10000);
  ctor_add(c, &key, &val);

  ctor_add_str(c, "k03", "hello3");

  key = rdb_string("k04");
  rdb_compressible_string(&val, &rnd, 0.25, 10000);
  ctor_add(c, &key, &val);

  options.block_size = 1024;
  options.compression = RDB_SNAPPY_COMPRESSION;
  options.comparator = rdb_bytewise_comparator;

  ctor_finish(c, &options, &keys);

  assert(check_range(ctor_approximate_offsetof(c, "abc"), 0, slop));
  assert(check_range(ctor_approximate_offsetof(c, "k01"), 0, slop));
  assert(check_range(ctor_approximate_offsetof(c, "k02"), 0, slop));
  /* Emitted a large compressible string, so adjust expected offset. */
  assert(check_range(ctor_approximate_offsetof(c, "k03"), min_z, max_z));
  assert(check_range(ctor_approximate_offsetof(c, "k04"), min_z, max_z));
  /* Emitted two large compressible strings, so adjust expected offset. */
  assert(check_range(ctor_approximate_offsetof(c, "xyz"), 2 * min_z,
                                                          2 * max_z));

  rdb_vector_clear(&keys);
  rdb_buffer_clear(&val);
  ctor_destroy(c);
}

/*
 * Execute
 */

RDB_EXTERN int
rdb_test_table(void);

int
rdb_test_table(void) {
  harness_t h;

  rdb_env_init();
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
  test_approximate_offsetof_plain();
  test_approximate_offsetof_compressed();

  harness_clear(&h);
  rdb_env_clear();

  return 0;
}
