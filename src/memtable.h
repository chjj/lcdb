/*!
 * memtable.h - memtable for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_MEMTABLE_H
#define RDB_MEMTABLE_H

#include "util/types.h"

/*
 * Types
 */

struct rdb_comparator_s;
struct rdb_iter_s;
struct rdb_lkey_s;

typedef struct rdb_memtable_s rdb_memtable_t;

/*
 * MemTable
 */

/* MemTables are reference counted. The initial reference count
   is zero and the caller must call ref() at least once. */
rdb_memtable_t *
rdb_memtable_create(const struct rdb_comparator_s *comparator);

void
rdb_memtable_destroy(rdb_memtable_t *mt);

/* Increase reference count. */
void
rdb_memtable_ref(rdb_memtable_t *mt);

/* Drop reference count. Delete if no more references exist. */
void
rdb_memtable_unref(rdb_memtable_t *mt);

/* Returns an estimate of the number of bytes of data in use by this
   data structure. It is safe to call when memtable is being modified. */
size_t
rdb_memtable_usage(const rdb_memtable_t *mt);

/* Add an entry into memtable that maps key to value at the
   specified sequence number and with the specified type.
   Typically value will be empty if type==RDB_TYPE_DELETION. */
void
rdb_memtable_add(rdb_memtable_t *mt,
                 rdb_seqnum_t sequence,
                 rdb_valtype_t type,
                 const rdb_slice_t *key,
                 const rdb_slice_t *value);

/* If memtable contains a value for key, store it in *value and return true.
   If memtable contains a deletion for key, store a NOTFOUND error
   in *status and return true.
   Else, return false. */
int
rdb_memtable_get(rdb_memtable_t *mt,
                 const struct rdb_lkey_s *key,
                 rdb_buffer_t *value,
                 int *status);

/*
 * MemTable Iterator
 */

/* Return an iterator that yields the contents of the memtable.
 *
 * The caller must ensure that the underlying memtable remains live
 * while the returned iterator is live. The keys returned by this
 * iterator are internal keys encoded by rdb_pkey_export in the
 * src/dbformat.{h,c} module.
 */
struct rdb_iter_s *
rdb_memiter_create(const rdb_memtable_t *mt);

#endif /* RDB_MEMTABLE_H */
