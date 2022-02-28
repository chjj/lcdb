/*!
 * memtable.c - memtable for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

#include "table/iterator.h"

#include "util/arena.h"
#include "util/buffer.h"
#include "util/coding.h"
#include "util/comparator.h"
#include "util/internal.h"
#include "util/slice.h"
#include "util/status.h"

#include "dbformat.h"
#include "memtable.h"
#include "skiplist.h"

/*
 * MemTable
 */

struct rdb_memtable_s {
  rdb_comparator_t comparator;
  int refs;
  rdb_arena_t arena;
  rdb_skiplist_t table;
};

static void
rdb_memtable_init(rdb_memtable_t *mt, const rdb_comparator_t *comparator) {
  assert(comparator->user_comparator != NULL);

  mt->comparator = *comparator;
  mt->refs = 0;

  rdb_arena_init(&mt->arena);

  rdb_skiplist_init(&mt->table, &mt->comparator, &mt->arena);
}

static void
rdb_memtable_clear(rdb_memtable_t *mt) {
  assert(mt->refs == 0);

  rdb_arena_clear(&mt->arena);
}

rdb_memtable_t *
rdb_memtable_create(const rdb_comparator_t *comparator) {
  rdb_memtable_t *mt = rdb_malloc(sizeof(rdb_memtable_t));
  rdb_memtable_init(mt, comparator);
  return mt;
}

void
rdb_memtable_destroy(rdb_memtable_t *mt) {
  rdb_memtable_clear(mt);
  rdb_free(mt);
}

void
rdb_memtable_ref(rdb_memtable_t *mt) {
  ++mt->refs;
}

void
rdb_memtable_unref(rdb_memtable_t *mt) {
  --mt->refs;

  assert(mt->refs >= 0);

  if (mt->refs <= 0)
    rdb_memtable_destroy(mt);
}

size_t
rdb_memtable_usage(const rdb_memtable_t *mt) {
  return rdb_arena_usage(&mt->arena);
}

void
rdb_memtable_add(rdb_memtable_t *mt,
                 rdb_seqnum_t sequence,
                 rdb_valtype_t type,
                 const rdb_slice_t *key,
                 const rdb_slice_t *value) {
  /* Format of an entry is concatenation of:
   *
   *  key_size     : varint32 of internal_key.size()
   *  key bytes    : char[internal_key.size()]
   *  tag          : uint64((sequence << 8) | type)
   *  value_size   : varint32 of value.size()
   *  value bytes  : char[value.size()]
   */
  size_t val_size = value->size;
  size_t ikey_size = key->size + 8;
  uint8_t *tp, *zp;
  size_t zn = 0;

  zn += rdb_varint32_size(ikey_size) + ikey_size;
  zn += rdb_varint32_size(val_size) + val_size;

  tp = rdb_arena_alloc(&mt->arena, zn);
  zp = tp;

  zp = rdb_varint32_write(zp, ikey_size);
  zp = rdb_raw_write(zp, key->data, key->size);
  zp = rdb_fixed64_write(zp, (sequence << 8) | type);

  zp = rdb_varint32_write(zp, val_size);
  zp = rdb_raw_write(zp, value->data, value->size);

  assert(zp == tp + zn);

  rdb_skiplist_insert(&mt->table, tp);
}

int
rdb_memtable_get(rdb_memtable_t *mt,
                 const rdb_lkey_t *key,
                 rdb_buffer_t *value,
                 int *status) {
  rdb_slice_t mkey = rdb_lkey_memtable_key(key);
  rdb_skipiter_t iter;

  rdb_skipiter_init(&iter, &mt->table);
  rdb_skipiter_seek(&iter, mkey.data);

  if (rdb_skipiter_valid(&iter)) {
    /* Entry format is:
     *
     *    klength  varint32
     *    userkey  char[klength]
     *    tag      uint64
     *    vlength  varint32
     *    value    char[vlength]
     *
     * Check that it belongs to same user key. We do not check the
     * sequence number since the seek() call above should have skipped
     * all entries with overly large sequence numbers.
     */
    const rdb_comparator_t *cmp = mt->comparator.user_comparator;
    rdb_slice_t okey = rdb_slice_decode(rdb_skipiter_key(&iter));
    rdb_slice_t ukey = rdb_lkey_user_key(key);

    assert(okey.size >= 8);

    okey.size -= 8;

    if (rdb_compare(cmp, &okey, &ukey) == 0) {
      /* Correct user key. */
      uint64_t tag = rdb_fixed64_decode(okey.data + okey.size);

      switch ((rdb_valtype_t)(tag & 0xff)) {
        case RDB_TYPE_VALUE: {
          if (value != NULL) {
            rdb_slice_t val = rdb_slice_decode(okey.data + okey.size + 8);
            rdb_buffer_copy(value, &val);
          }
          return 1;
        }

        case RDB_TYPE_DELETION: {
          *status = RDB_NOTFOUND;
          return 1;
        }
      }
    }
  }

  return 0;
}

/*
 * MemTable Iterator
 */

typedef struct rdb_memiter_s {
  rdb_skipiter_t iter;
  rdb_buffer_t tmp;
} rdb_memiter_t;

static void
rdb_memiter_init(rdb_memiter_t *iter, const rdb_skiplist_t *table) {
  rdb_skipiter_init(&iter->iter, table);
  rdb_buffer_init(&iter->tmp);
}

static void
rdb_memiter_clear(rdb_memiter_t *iter) {
  rdb_buffer_clear(&iter->tmp);
}

static int
rdb_memiter_valid(const rdb_memiter_t *iter) {
  return rdb_skipiter_valid(&iter->iter);
}

static void
rdb_memiter_seek(rdb_memiter_t *iter, const rdb_slice_t *key) {
  rdb_buffer_t *tmp = &iter->tmp;

  rdb_buffer_reset(tmp);
  rdb_slice_export(tmp, key);

  rdb_skipiter_seek(&iter->iter, tmp->data);
}

static void
rdb_memiter_seek_first(rdb_memiter_t *iter) {
  rdb_skipiter_seek_first(&iter->iter);
}

static void
rdb_memiter_seek_last(rdb_memiter_t *iter) {
  rdb_skipiter_seek_last(&iter->iter);
}

static void
rdb_memiter_next(rdb_memiter_t *iter) {
  rdb_skipiter_next(&iter->iter);
}

static void
rdb_memiter_prev(rdb_memiter_t *iter) {
  rdb_skipiter_prev(&iter->iter);
}

static rdb_slice_t
rdb_memiter_key(const rdb_memiter_t *iter) {
  return rdb_slice_decode(rdb_skipiter_key(&iter->iter));
}

static rdb_slice_t
rdb_memiter_value(const rdb_memiter_t *iter) {
  rdb_slice_t key = rdb_memiter_key(iter);
  return rdb_slice_decode(key.data + key.size);
}

static int
rdb_memiter_status(const rdb_memiter_t *iter) {
  (void)iter;
  return RDB_OK;
}

RDB_ITERATOR_FUNCTIONS(rdb_memiter);

rdb_iter_t *
rdb_memiter_create(const rdb_memtable_t *mt) {
  rdb_memiter_t *iter = rdb_malloc(sizeof(rdb_memiter_t));

  rdb_memiter_init(iter, &mt->table);

  return rdb_iter_create(iter, &rdb_memiter_table);
}
