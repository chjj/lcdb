/*!
 * table_builder.h - sorted string table builder for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_TABLE_BUILDER_H
#define RDB_TABLE_BUILDER_H

#include <stdint.h>

#include "../util/types.h"

/*
 * Types
 */

struct rdb_dbopt_s;
struct rdb_wfile_s;

typedef struct rdb_tablebuilder_s rdb_tablebuilder_t;

/*
 * Table Builder
 */

/* Create a builder that will store the contents of the table it is
 * building in *file. Does not close the file. It is up to the
 * caller to close the file after calling finish().
 */
rdb_tablebuilder_t *
rdb_tablebuilder_create(const struct rdb_dbopt_s *options,
                        struct rdb_wfile_s *file);

/* REQUIRES: Either finish() or abandon() has been called. */
void
rdb_tablebuilder_destroy(rdb_tablebuilder_t *tb);

/* Change the options used by this builder. Note: only some of the
 * option fields can be changed after construction. If a field is
 * not allowed to change dynamically and its value in the structure
 * passed to the constructor is different from its value in the
 * structure passed to this method, this method will return an error
 * without changing any fields.
 */
int
rdb_tablebuilder_change_options(rdb_tablebuilder_t *tb,
                                const struct rdb_dbopt_s *options);

/* Add key,value to the table being constructed. */
/* REQUIRES: key is after any previously added key according to comparator. */
/* REQUIRES: finish(), abandon() have not been called */
void
rdb_tablebuilder_add(rdb_tablebuilder_t *tb,
                     const rdb_slice_t *key,
                     const rdb_slice_t *value);

/* Advanced operation: flush any buffered key/value pairs to file.
 * Can be used to ensure that two adjacent entries never live in
 * the same data block. Most clients should not need to use this method.
 * REQUIRES: finish(), abandon() have not been called
 */
void
rdb_tablebuilder_flush(rdb_tablebuilder_t *tb);

/* Return non-ok iff some error has been detected. */
int
rdb_tablebuilder_status(const rdb_tablebuilder_t *tb);

/* Finish building the table. Stops using the file passed to the
 * constructor after this function returns.
 * REQUIRES: finish(), abandon() have not been called
 */
int
rdb_tablebuilder_finish(rdb_tablebuilder_t *tb);

/* Indicate that the contents of this builder should be abandoned. Stops
 * using the file passed to the constructor after this function returns.
 * If the caller is not going to call finish(), it must call abandon()
 * before destroying this builder.
 * REQUIRES: finish(), abandon() have not been called
 */
void
rdb_tablebuilder_abandon(rdb_tablebuilder_t *tb);

/* Number of calls to add() so far. */
uint64_t
rdb_tablebuilder_num_entries(const rdb_tablebuilder_t *tb);

/* Size of the file generated so far. If invoked after a successful
   finish() call, returns the size of the final generated file. */
uint64_t
rdb_tablebuilder_file_size(const rdb_tablebuilder_t *tb);

#endif /* RDB_TABLE_BUILDER_H */
