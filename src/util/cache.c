/*!
 * cache.c - lru cache for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "cache.h"
#include "hash.h"
#include "internal.h"
#include "port.h"
#include "slice.h"

/* LRU cache implementation
 *
 * Cache entries have an "in_cache" boolean indicating whether the cache has a
 * reference on the entry.  The only ways that this can become false without the
 * entry being passed to its "deleter" are via Erase(), via Insert() when
 * an element with a duplicate key is inserted, or on destruction of the cache.
 *
 * The cache keeps two linked lists of items in the cache.  All items in the
 * cache are in one list or the other, and never both.  Items still referenced
 * by clients but erased from the cache are in neither list.  The lists are:
 * - in-use:  contains the items currently referenced by clients, in no
 *   particular order.  (This list is used for invariant checking.  If we
 *   removed the check, elements that would otherwise be on this list could be
 *   left as disconnected singleton lists.)
 * - LRU:  contains the items not currently referenced by clients, in LRU order
 * Elements are moved between these lists by the Ref() and Unref() methods,
 * when they detect an element in the cache acquiring or losing its only
 * external reference.
 */

/*
 * Constants
 */

#define RDB_SHARD_BITS 4
#define RDB_SHARDS (1 << RDB_SHARD_BITS)

/*
 * LRU Handle
 */

/* An entry is a variable length heap-allocated structure.  Entries
   are kept in a circular doubly linked list ordered by access time. */
struct rdb_lruhandle_s {
  void *value;
  void (*deleter)(const rdb_slice_t *key, void *value);
  struct rdb_lruhandle_s *next_hash;
  struct rdb_lruhandle_s *next;
  struct rdb_lruhandle_s *prev;
  size_t charge;
  size_t key_length;
  int in_cache;      /* Whether entry is in the cache. */
  uint32_t refs;     /* References, including cache reference, if present. */
  uint32_t hash;     /* Hash of key(); used for fast sharding and comparisons */
  unsigned char key_data[1];  /* Beginning of key */
};

static rdb_slice_t
rdb_lruhandle_key(const rdb_lruhandle_t *handle) {
  rdb_slice_t key;
  rdb_slice_set(&key, handle->key_data, handle->key_length);
  return key;
}

/*
 * LRU Table
 */

/* We provide our own simple hash table since it removes a whole bunch
 * of porting hacks and is also faster than some of the built-in hash
 * table implementations in some of the compiler/runtime combinations
 * we have tested. E.g., readrandom speeds up by ~5% over the g++
 * 4.4.3's builtin hashtable.
 */
typedef struct rdb_lrutable_s {
  /* The table consists of an array of buckets where each bucket is
     a linked list of cache entries that hash into the bucket. */
  uint32_t length;
  uint32_t elems;
  rdb_lruhandle_t **list;
} rdb_lrutable_t;

static int
handle_equal(const rdb_lruhandle_t *x, const rdb_slice_t *y) {
  return rdb_memcmp4(x->key_data, x->key_length, y->data, y->size);
}

/* Return a pointer to slot that points to a cache entry that
 * matches key/hash. If there is no such cache entry, return a
 * pointer to the trailing slot in the corresponding linked list.
 */
static rdb_lruhandle_t **
rdb_lrutable_find(rdb_lrutable_t *tbl, const rdb_slice_t *key, uint32_t hash) {
  rdb_lruhandle_t **ptr = &tbl->list[hash & (tbl->length - 1)];

  while (*ptr != NULL && ((*ptr)->hash != hash || !handle_equal(*ptr, key)))
    ptr = &(*ptr)->next_hash;

  return ptr;
}

static void
rdb_lrutable_resize(rdb_lrutable_t *tbl) {
  rdb_lruhandle_t **new_list;
  uint32_t new_length = 4;
  uint32_t count = 0;
  uint32_t i;

  while (new_length < tbl->elems)
    new_length *= 2;

  new_list = rdb_malloc(new_length * sizeof(rdb_lruhandle_t *));

  memset(new_list, 0, sizeof(new_list[0]) * new_length);

  for (i = 0; i < tbl->length; i++) {
    rdb_lruhandle_t *h = tbl->list[i];

    while (h != NULL) {
      rdb_lruhandle_t *next = h->next_hash;
      uint32_t hash = h->hash;
      rdb_lruhandle_t **ptr;

      ptr = &new_list[hash & (new_length - 1)];
      h->next_hash = *ptr;
      *ptr = h;
      h = next;
      count++;
    }
  }

  assert(tbl->elems == count);

  if (tbl->list != NULL)
    rdb_free(tbl->list);

  tbl->list = new_list;
  tbl->length = new_length;
}

static void
rdb_lrutable_init(rdb_lrutable_t *tbl) {
  tbl->length = 0;
  tbl->elems = 0;
  tbl->list = NULL;

  rdb_lrutable_resize(tbl);
}

static void
rdb_lrutable_clear(rdb_lrutable_t *tbl) {
  if (tbl->list != NULL)
    rdb_free(tbl->list);
}

static rdb_lruhandle_t *
rdb_lrutable_lookup(rdb_lrutable_t *tbl,
                    const rdb_slice_t *key,
                    uint32_t hash) {
  return *rdb_lrutable_find(tbl, key, hash);
}

static rdb_lruhandle_t *
rdb_lrutable_insert(rdb_lrutable_t *tbl, rdb_lruhandle_t *h) {
  rdb_slice_t key = rdb_lruhandle_key(h);
  rdb_lruhandle_t **ptr = rdb_lrutable_find(tbl, &key, h->hash);
  rdb_lruhandle_t *old = *ptr;

  h->next_hash = (old == NULL ? NULL : old->next_hash);

  *ptr = h;

  if (old == NULL) {
    ++tbl->elems;
    if (tbl->elems > tbl->length) {
      /* Since each cache entry is fairly large, we aim
         for a small average linked list length (<= 1). */
      rdb_lrutable_resize(tbl);
    }
  }

  return old;
}

static rdb_lruhandle_t *
rdb_lrutable_remove(rdb_lrutable_t *tbl,
                    const rdb_slice_t *key,
                    uint32_t hash) {
  rdb_lruhandle_t **ptr = rdb_lrutable_find(tbl, key, hash);
  rdb_lruhandle_t *result = *ptr;

  if (result != NULL) {
    *ptr = result->next_hash;
    --tbl->elems;
  }

  return result;
}

/*
 * Shard
 */

/* A single shard of sharded cache. */
typedef struct rdb_shard_s {
  /* Initialized before use. */
  size_t capacity;

  /* mutex protects the following state. */
  rdb_mutex_t mutex;
  size_t usage;

  /* Dummy head of LRU list. */
  /* lru.prev is newest entry, lru.next is oldest entry. */
  /* Entries have refs==1 and in_cache==1. */
  rdb_lruhandle_t list;

  /* Dummy head of in-use list. */
  /* Entries are in use by clients, and have refs >= 2 and in_cache==1. */
  rdb_lruhandle_t in_use;

  rdb_lrutable_t table;
} rdb_shard_t;

static size_t
rdb_shard_total_charge(rdb_shard_t *lru) {
  size_t usage;
  rdb_mutex_lock(&lru->mutex);
  usage = lru->usage;
  rdb_mutex_unlock(&lru->mutex);
  return usage;
}

static void
rdb_shard_append(rdb_lruhandle_t *list, rdb_lruhandle_t *e) {
  /* Make "e" newest entry by inserting just before *list */
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

static void
rdb_shard_remove(rdb_lruhandle_t *e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

static void
rdb_shard_ref(rdb_shard_t *lru, rdb_lruhandle_t *e) {
  if (e->refs == 1 && e->in_cache) { /* If on lru->list, move to lru->in_use. */
    rdb_shard_remove(e);
    rdb_shard_append(&lru->in_use, e);
  }
  e->refs++;
}

static void
rdb_shard_unref(rdb_shard_t *lru, rdb_lruhandle_t *e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) { /* Deallocate. */
    rdb_slice_t key = rdb_lruhandle_key(e);
    assert(!e->in_cache);
    e->deleter(&key, e->value);
    rdb_free(e);
  } else if (e->in_cache && e->refs == 1) {
    /* No longer in use; move to lru->list. */
    rdb_shard_remove(e);
    rdb_shard_append(&lru->list, e);
  }
}

static void
rdb_shard_init(rdb_shard_t *lru) {
  memset(lru, 0, sizeof(*lru));

  rdb_mutex_init(&lru->mutex);

  lru->capacity = 0;
  lru->usage = 0;

  /* Make empty circular linked lists. */
  lru->list.next = &lru->list;
  lru->list.prev = &lru->list;

  lru->in_use.next = &lru->in_use;
  lru->in_use.prev = &lru->in_use;

  rdb_lrutable_init(&lru->table);
}

static void
rdb_shard_clear(rdb_shard_t *lru) {
  rdb_lruhandle_t *e, *next;

  assert(lru->in_use.next == &lru->in_use); /* Error if caller has an unreleased handle */

  for (e = lru->list.next; e != &lru->list; e = next) {
    next = e->next;

    assert(e->in_cache);

    e->in_cache = 0;

    assert(e->refs == 1); /* Invariant of lru->list. */

    rdb_shard_unref(lru, e);
  }

  rdb_lrutable_clear(&lru->table);

  rdb_mutex_destroy(&lru->mutex);
}

static rdb_lruhandle_t *
rdb_shard_lookup(rdb_shard_t *lru, const rdb_slice_t *key, uint32_t hash) {
  rdb_lruhandle_t *e;

  rdb_mutex_lock(&lru->mutex);

  e = rdb_lrutable_lookup(&lru->table, key, hash);

  if (e != NULL)
    rdb_shard_ref(lru, e);

  rdb_mutex_unlock(&lru->mutex);

  return e;
}

static void
rdb_shard_release(rdb_shard_t *lru, rdb_lruhandle_t *handle) {
  rdb_mutex_lock(&lru->mutex);
  rdb_shard_unref(lru, handle);
  rdb_mutex_unlock(&lru->mutex);
}

/* If e != NULL, finish removing *e from the cache; it has already been
   removed from the hash table. Return whether e != NULL. */
static int
rdb_shard_finish_erase(rdb_shard_t *lru, rdb_lruhandle_t *e) {
  if (e != NULL) {
    assert(e->in_cache);
    rdb_shard_remove(e);
    e->in_cache = 0;
    lru->usage -= e->charge;
    rdb_shard_unref(lru, e);
  }
  return e != NULL;
}

static void
rdb_shard_erase(rdb_shard_t *lru, const rdb_slice_t *key, uint32_t hash) {
  rdb_mutex_lock(&lru->mutex);
  rdb_shard_finish_erase(lru, rdb_lrutable_remove(&lru->table, key, hash));
  rdb_mutex_unlock(&lru->mutex);
}

static void
rdb_shard_prune(rdb_shard_t *lru) {
  rdb_mutex_lock(&lru->mutex);

  while (lru->list.next != &lru->list) {
    rdb_lruhandle_t *e = lru->list.next;
    rdb_slice_t e_key = rdb_lruhandle_key(e);

    assert(e->refs == 1);

    rdb_shard_finish_erase(lru,
      rdb_lrutable_remove(&lru->table, &e_key, e->hash));
  }

  rdb_mutex_unlock(&lru->mutex);
}

static rdb_lruhandle_t *
rdb_shard_insert(rdb_shard_t *lru,
                 const rdb_slice_t *key,
                 uint32_t hash,
                 void *value,
                 size_t charge,
                 void (*deleter)(const rdb_slice_t *key, void *value)) {
  rdb_lruhandle_t *e;

  rdb_mutex_lock(&lru->mutex);

  e = rdb_malloc(sizeof(rdb_lruhandle_t) - 1 + key->size);

  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key->size;
  e->hash = hash;
  e->in_cache = 0;
  e->refs = 1; /* for the returned handle. */

  memcpy(e->key_data, key->data, key->size);

  if (lru->capacity > 0) {
    e->refs++; /* for the cache's reference. */
    e->in_cache = 1;
    rdb_shard_append(&lru->in_use, e);
    lru->usage += charge;
    rdb_shard_finish_erase(lru, rdb_lrutable_insert(&lru->table, e));
  } else { /* don't cache. (lru->capacity==0 is supported and turns off caching.) */
    /* next is read by key() in an assert, so it must be initialized */
    e->next = NULL;
  }

  while (lru->usage > lru->capacity && lru->list.next != &lru->list) {
    rdb_lruhandle_t *old = lru->list.next;
    rdb_slice_t old_key = rdb_lruhandle_key(old);

    assert(old->refs == 1);

    rdb_shard_finish_erase(lru,
      rdb_lrutable_remove(&lru->table, &old_key, old->hash));
  }

  rdb_mutex_unlock(&lru->mutex);

  return e;
}

/*
 * LRU Cache
 */

struct rdb_lru_s {
  rdb_shard_t shard[RDB_SHARDS];
  rdb_mutex_t id_mutex;
  uint64_t last_id;
};

static uint32_t
rdb_lru_hash(const rdb_slice_t *s) {
  return rdb_hash(s->data, s->size, 0);
}

static uint32_t
rdb_lru_shard(uint32_t hash) {
  return hash >> (32 - RDB_SHARD_BITS);
}

static void
rdb_lru_init(rdb_lru_t *lru, size_t capacity) {
  size_t per_shard = (capacity + RDB_SHARDS - 1) / RDB_SHARDS;
  int i;

  rdb_mutex_init(&lru->id_mutex);

  lru->last_id = 0;

  for (i = 0; i < RDB_SHARDS; i++) {
    rdb_shard_init(&lru->shard[i]);

    lru->shard[i].capacity = per_shard;
  }
}

static void
rdb_lru_clear(rdb_lru_t *lru) {
  int i;

  for (i = 0; i < RDB_SHARDS; i++)
    rdb_shard_clear(&lru->shard[i]);

  rdb_mutex_destroy(&lru->id_mutex);
}

rdb_lru_t *
rdb_lru_create(size_t capacity) {
  rdb_lru_t *lru = rdb_malloc(sizeof(rdb_lru_t));
  rdb_lru_init(lru, capacity);
  return lru;
}

void
rdb_lru_destroy(rdb_lru_t *lru) {
  rdb_lru_clear(lru);
  rdb_free(lru);
}

rdb_lruhandle_t *
rdb_lru_insert(rdb_lru_t *lru,
               const rdb_slice_t *key,
               void *value,
               size_t charge,
               void (*deleter)(const rdb_slice_t *key, void *value)) {
  uint32_t hash = rdb_lru_hash(key);
  rdb_shard_t *shard = &lru->shard[rdb_lru_shard(hash)];
  return rdb_shard_insert(shard, key, hash, value, charge, deleter);
}

rdb_lruhandle_t *
rdb_lru_lookup(rdb_lru_t *lru, const rdb_slice_t *key) {
  uint32_t hash = rdb_lru_hash(key);
  rdb_shard_t *shard = &lru->shard[rdb_lru_shard(hash)];
  return rdb_shard_lookup(shard, key, hash);
}

void
rdb_lru_release(rdb_lru_t *lru, rdb_lruhandle_t *handle) {
  rdb_shard_t *shard = &lru->shard[rdb_lru_shard(handle->hash)];
  rdb_shard_release(shard, handle);
}

void
rdb_lru_erase(rdb_lru_t *lru, const rdb_slice_t *key) {
  uint32_t hash = rdb_lru_hash(key);
  rdb_shard_t *shard = &lru->shard[rdb_lru_shard(hash)];
  rdb_shard_erase(shard, key, hash);
}

void *
rdb_lru_value(rdb_lruhandle_t *handle) {
  return handle->value;
}

uint32_t
rdb_lru_newid(rdb_lru_t *lru) {
  uint64_t id;
  rdb_mutex_lock(&lru->id_mutex);
  id = ++lru->last_id;
  rdb_mutex_unlock(&lru->id_mutex);
  return id;
}

void
rdb_lru_prune(rdb_lru_t *lru) {
  int i;

  for (i = 0; i < RDB_SHARDS; i++)
    rdb_shard_prune(&lru->shard[i]);
}

size_t
rdb_lru_total_charge(rdb_lru_t *lru) {
  size_t total = 0;
  int i;

  for (i = 0; i < RDB_SHARDS; i++)
    total += rdb_shard_total_charge(&lru->shard[i]);

  return total;
}
