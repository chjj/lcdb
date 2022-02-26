/*!
 * builder.h - table building function for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_BUILDER_H
#define RDB_BUILDER_H

/*
 * Types
 */

struct rdb_dbopt_s;
struct rdb_filemeta_s;
struct rdb_iter_s;
struct rdb_tcache_s;

/*
 * BuildTable
 */

/* Build a Table file from the contents of *iter.  The generated file
   will be named according to meta->number. On success, the rest of
   *meta will be filled with metadata about the generated table.
   If no data is present in *iter, meta->file_size will be set to
   zero, and no Table file will be produced. */
int
rdb_build_table(const char *prefix,
                const struct rdb_dbopt_s *options,
                struct rdb_tcache_s *table_cache,
                struct rdb_iter_s *iter,
                struct rdb_filemeta_s *meta);

#endif /* RDB_BUILDER_H */
