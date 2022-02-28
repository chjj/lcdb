/*!
 * cache.h - lru cache for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_CACHE_H
#define RDB_CACHE_H

#include <stddef.h>
#include <stdint.h>
#include "extern.h"
#include "types.h"

/*
 * Types
 */

typedef struct rdb_lru_s rdb_lru_t;

/* Opaque handle to an entry stored in the cache. */
typedef struct rdb_lruhandle_s rdb_lruhandle_t;

/*
 * LRU Cache
 */

/* Create a new cache with a fixed size capacity. This implementation
   of Cache uses a least-recently-used eviction policy. */
RDB_EXTERN rdb_lru_t *
rdb_lru_create(size_t capacity);

/* Destroys all existing entries by calling the "deleter"
   function that was passed to the constructor. */
RDB_EXTERN void
rdb_lru_destroy(rdb_lru_t *lru);

/* Insert a mapping from key->value into the cache and assign it
 * the specified charge against the total cache capacity.
 *
 * Returns a handle that corresponds to the mapping. The caller
 * must call release(handle) when the returned mapping is no
 * longer needed.
 *
 * When the inserted entry is no longer needed, the key and
 * value will be passed to "deleter".
 */
rdb_lruhandle_t *
rdb_lru_insert(rdb_lru_t *lru,
               const rdb_slice_t *key,
               void *value,
               size_t charge,
               void (*deleter)(const rdb_slice_t *key, void *value));

/* If the cache has no mapping for "key", returns NULL.
 *
 * Else return a handle that corresponds to the mapping. The caller
 * must call release(handle) when the returned mapping is no
 * longer needed.
 */
rdb_lruhandle_t *
rdb_lru_lookup(rdb_lru_t *lru, const rdb_slice_t *key);

/* Release a mapping returned by a previous lookup().
 * REQUIRES: handle must not have been released yet.
 * REQUIRES: handle must have been returned by a method on *this.
 */
void
rdb_lru_release(rdb_lru_t *lru, rdb_lruhandle_t *handle);

/* If the cache contains entry for key, erase it. Note that the
 * underlying entry will be kept around until all existing handles
 * to it have been released.
 */
void
rdb_lru_erase(rdb_lru_t *lru, const rdb_slice_t *key);

/* Return the value encapsulated in a handle returned by a
 * successful lookup().
 * REQUIRES: handle must not have been released yet.
 * REQUIRES: handle must have been returned by a method on *this.
 */
void *
rdb_lru_value(rdb_lruhandle_t *handle);

/* Return a new numeric id. May be used by multiple clients who are
 * sharing the same cache to partition the key space. Typically the
 * client will allocate a new id at startup and prepend the id to
 * its cache keys.
 */
uint32_t
rdb_lru_newid(rdb_lru_t *lru);

/* Remove all cache entries that are not actively in use. Memory-constrained
 * applications may wish to call this method to reduce memory usage.
 * Default implementation of prune() does nothing. Subclasses are strongly
 * encouraged to override the default implementation. A future release of
 * leveldb may change prune() to a pure abstract method.
 */
void
rdb_lru_prune(rdb_lru_t *lru);

/* Return an estimate of the combined charges of all elements stored in the
   cache. */
size_t
rdb_lru_total_charge(rdb_lru_t *lru);

#endif /* RDB_CACHE_H */
