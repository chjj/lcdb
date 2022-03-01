/*!
 * builder.c - table building function for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdlib.h>

#include "table/iterator.h"
#include "table/table_builder.h"

#include "util/env.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/status.h"

#include "dbformat.h"
#include "filename.h"
#include "table_cache.h"
#include "version_edit.h"

/*
 * BuildTable
 */

int
rdb_build_table(const char *prefix,
                const rdb_dbopt_t *options,
                rdb_tcache_t *table_cache,
                rdb_iter_t *iter,
                rdb_filemeta_t *meta) {
  char fname[RDB_PATH_MAX];
  int rc = RDB_OK;

  meta->file_size = 0;

  rdb_iter_seek_first(iter);

  if (!rdb_table_filename(fname, sizeof(fname), prefix, meta->number))
    return RDB_INVALID;

  if (rdb_iter_valid(iter)) {
    rdb_tablebuilder_t *builder;
    rdb_slice_t key, val;
    rdb_wfile_t *file;
    rdb_iter_t *it;

    rc = rdb_truncfile_create(fname, &file);

    if (rc != RDB_OK)
      return rc;

    builder = rdb_tablebuilder_create(options, file);

    key = rdb_iter_key(iter);

    /* meta->smallest.DecodeFrom(key); */
    rdb_ikey_copy(&meta->smallest, &key);

    for (; rdb_iter_valid(iter); rdb_iter_next(iter)) {
      key = rdb_iter_key(iter);
      val = rdb_iter_value(iter);

      rdb_tablebuilder_add(builder, &key, &val);
    }

    if (key.size > 0) {
      /* meta->largest.DecodeFrom(key); */
      rdb_ikey_copy(&meta->largest, &key);
    }

    /* Finish and check for builder errors. */
    rc = rdb_tablebuilder_finish(builder);

    if (rc == RDB_OK) {
      meta->file_size = rdb_tablebuilder_file_size(builder);

      assert(meta->file_size > 0);
    }

    rdb_tablebuilder_destroy(builder);

    /* Finish and check for file errors. */
    if (rc == RDB_OK)
      rc = rdb_wfile_sync(file);

    if (rc == RDB_OK)
      rc = rdb_wfile_close(file);

    rdb_wfile_destroy(file);
    file = NULL;

    if (rc == RDB_OK) {
      /* Verify that the table is usable. */
      it = rdb_tcache_iterate(table_cache,
                              rdb_readopt_default,
                              meta->number,
                              meta->file_size,
                              NULL);

      rc = rdb_iter_status(it);

      rdb_iter_destroy(it);
    }
  }

  /* Check for input iterator errors. */
  if (rdb_iter_status(iter) != RDB_OK)
    rc = rdb_iter_status(iter);

  if (rc == RDB_OK && meta->file_size > 0)
    ; /* Keep it. */
  else
    rdb_remove_file(fname);

  return rc;
}
