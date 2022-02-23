/*!
 * table.h - sorted string table for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_TABLE_H
#define RDB_TABLE_H

#include <stdint.h>

#include "../util/types.h"

/*
 * Types
 */

struct rdb_dbopt_s;
struct rdb_iter_s;
struct rdb_readopt_s;
struct rdb_rfile_s;

/* A table is a sorted map from strings to strings. Tables are
   immutable and persistent. A table may be safely accessed from
   multiple threads without external synchronization. */
typedef struct rdb_table_s rdb_table_t;

/*
 * Table
 */

/* Attempt to open the table that is stored in bytes [0..file_size)
 * of "file", and read the metadata entries necessary to allow
 * retrieving data from the table.
 *
 * If successful, returns ok and sets "*table" to the newly opened
 * table. The client should delete "*table" when no longer needed.
 * If there was an error while initializing the table, sets "*table"
 * to NULL and returns a non-ok status. Does not take ownership of
 * "*source", but the client must ensure that "source" remains live
 * for the duration of the returned table's lifetime.
 *
 * *file must remain live while this Table is in use.
 */
int
rdb_table_open(const struct rdb_dbopt_s *options,
               struct rdb_rfile_s *file,
               uint64_t size,
               rdb_table_t **table);

void
rdb_table_destroy(rdb_table_t *table);


/* Returns a new iterator over the table contents.
 * The result of NewIterator() is initially invalid (caller must
 * call one of the Seek methods on the iterator before using it).
 */
struct rdb_iter_s *
rdb_tableiter_create(const rdb_table_t *table,
                     const struct rdb_readopt_s *options);

/* Calls (*handle_result)(arg, ...) with the entry found after a call
 * to Seek(key). May not make such a call if filter policy says
 * that key is not present.
 */
int
rdb_table_internal_get(rdb_table_t *table,
                       const struct rdb_readopt_s *options,
                       const rdb_slice_t *k,
                       void *arg,
                       void (*handle_result)(void *,
                                             const rdb_slice_t *,
                                             const rdb_slice_t *));

/* Given a key, return an approximate byte offset in the file where
 * the data for that key begins (or would begin if the key were
 * present in the file). The returned value is in terms of file
 * bytes, and so includes effects like compression of the underlying data.
 * E.g., the approximate offset of the last key in the table will
 * be close to the file length.
 */
uint64_t
rdb_table_approximate_offsetof(const rdb_table_t *table,
                               const rdb_slice_t *key);

#endif /* RDB_TABLE_H */
