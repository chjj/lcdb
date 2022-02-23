/*!
 * table.c - sorted string table for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "../util/bloom.h"
#include "../util/cache.h"
#include "../util/coding.h"
#include "../util/comparator.h"
#include "../util/env.h"
#include "../util/internal.h"
#include "../util/options.h"
#include "../util/slice.h"
#include "../util/status.h"

#include "block.h"
#include "filter_block.h"
#include "format.h"
#include "iterator.h"
#include "table.h"
#include "two_level_iterator.h"

/*
 * Table
 */

struct rdb_table_s {
  rdb_dbopt_t options;
  int status;
  rdb_rfile_t *file;
  uint64_t cache_id;
  rdb_filterreader_t *filter;
  const uint8_t *filter_data;
  rdb_blockhandle_t metaindex_handle; /* Handle to metaindex_block: saved from footer. */
  rdb_block_t *index_block;
};

static void
rdb_table_init(rdb_table_t *table) {
  memset(table, 0, sizeof(*table));
}

static void
rdb_table_clear(rdb_table_t *table) {
  if (table->filter != NULL)
    rdb_free(table->filter);

  if (table->filter_data != NULL)
    rdb_free((void *)table->filter_data);

  rdb_block_destroy(table->index_block);
}

static void
rdb_table_read_filter(rdb_table_t *table,
                      const rdb_slice_t *filter_handle_value) {
  rdb_readopt_t opt = *rdb_readopt_default;
  rdb_blockhandle_t filter_handle;
  rdb_blockcontents_t block;
  int rc;

  if (!rdb_blockhandle_import(&filter_handle, filter_handle_value))
    return;

  /* We might want to unify with ReadBlock() if we start
     requiring checksum verification in Table::Open. */
  if (table->options.paranoid_checks)
    opt.verify_checksums = 1;

  rc = rdb_read_block(&block,
                      table->file,
                      &opt,
                      &filter_handle);

  if (rc != RDB_OK)
    return;

  if (block.heap_allocated)
    table->filter_data = block.data.data; /* Will need to delete later. */

  table->filter = rdb_malloc(sizeof(rdb_filterreader_t));

  rdb_filterreader_init(table->filter,
                        table->options.filter_policy,
                        &block.data);
}

static void
rdb_table_read_meta(rdb_table_t *table, const rdb_footer_t *footer) {
  rdb_readopt_t opt = *rdb_readopt_default;
  rdb_blockcontents_t contents;
  rdb_block_t *meta;
  rdb_iter_t *iter;
  rdb_slice_t key;
  int rc;

  if (table->options.filter_policy == NULL)
    return; /* Do not need any metadata. */

  if (table->options.paranoid_checks)
    opt.verify_checksums = 1;

  rc = rdb_read_block(&contents,
                      table->file,
                      &opt,
                      &footer->metaindex_handle);

  if (rc != RDB_OK) {
    /* Do not propagate errors since meta info is not needed for operation. */
    return;
  }

  meta = rdb_block_create(&contents);
  iter = rdb_blockiter_create(meta, rdb_bytewise_comparator);

  rdb_slice_set_str(&key, table->options.filter_policy->name);
  rdb_iter_seek(iter, &key);

  if (rdb_iter_valid(iter)) {
    rdb_slice_t iter_key = rdb_iter_key(iter);

    if (rdb_slice_equal(&iter_key, &key)) {
      rdb_slice_t iter_value = rdb_iter_value(iter);
      rdb_table_read_filter(table, &iter_value);
    }
  }

  rdb_iter_destroy(iter);
  rdb_block_destroy(meta);
}

int
rdb_table_open(const rdb_dbopt_t *options,
               rdb_rfile_t *file,
               uint64_t size,
               rdb_table_t **table) {
  rdb_readopt_t opt = *rdb_readopt_default;
  rdb_blockcontents_t contents;
  uint8_t buf[RDB_FOOTER_SIZE];
  rdb_footer_t footer;
  rdb_slice_t input;
  int rc;

  *table = NULL;

  if (size < RDB_FOOTER_SIZE)
    return RDB_CORRUPTION; /* "file is too short to be an sstable" */

  rc = rdb_rfile_pread(file,
                       &input,
                       buf,
                       RDB_FOOTER_SIZE,
                       size - RDB_FOOTER_SIZE);

  if (rc != RDB_OK)
    return RDB_CORRUPTION;

  if (!rdb_footer_import(&footer, &input))
    return RDB_CORRUPTION;

  /* Read the index block. */
  if (options->paranoid_checks)
    opt.verify_checksums = 1;

  rc = rdb_read_block(&contents,
                      file,
                      &opt,
                      &footer.index_handle);

  if (rc == RDB_OK) {
    /* We've successfully read the footer and the
       index block: we're ready to serve requests. */
    rdb_block_t *index_block = rdb_block_create(&contents);
    rdb_table_t *tbl = rdb_malloc(sizeof(rdb_table_t));

    rdb_table_init(tbl);

    tbl->options = *options;
    tbl->file = file;
    tbl->metaindex_handle = footer.metaindex_handle;
    tbl->index_block = index_block;
    tbl->cache_id = 0;
    tbl->filter_data = NULL;
    tbl->filter = NULL;

    if (options->block_cache != NULL)
      tbl->cache_id = rdb_lru_newid(options->block_cache);

    rdb_table_read_meta(tbl, &footer);

    *table = tbl;
  }

  return rc;
}

void
rdb_table_destroy(rdb_table_t *table) {
  rdb_table_clear(table);
  rdb_free(table);
}

static void
delete_block(void *arg, void *ignored) {
  rdb_block_t *block = (rdb_block_t *)arg;
  (void)ignored;
  rdb_block_destroy(block);
}

static void
delete_cached_block(const rdb_slice_t *key, void *value) {
  rdb_block_t *block = (rdb_block_t *)value;
  (void)key;
  rdb_block_destroy(block);
}

static void
release_block(void *arg, void *h) {
  rdb_lru_t *cache = (rdb_lru_t *)arg;
  rdb_lruhandle_t *handle = (rdb_lruhandle_t *)h;

  rdb_lru_release(cache, handle);
}

/* Convert an index iterator value (i.e., an encoded BlockHandle)
   into an iterator over the contents of the corresponding block. */
static rdb_iter_t *
rdb_table_blockreader(void *arg,
                      const rdb_readopt_t *options,
                      const rdb_slice_t *index_value) {
  rdb_table_t *table = (rdb_table_t *)arg;
  rdb_lru_t *block_cache = table->options.block_cache;
  rdb_block_t *block = NULL;
  rdb_lruhandle_t *cache_handle = NULL;
  rdb_blockhandle_t handle;
  rdb_iter_t *iter;
  int rc = RDB_OK;

  /* We intentionally allow extra stuff in index_value so that we
     can add more features in the future. */

  if (!rdb_blockhandle_import(&handle, index_value))
    rc = RDB_CORRUPTION;

  if (rc == RDB_OK) {
    rdb_blockcontents_t contents;

    if (block_cache != NULL) {
      uint8_t cache_key_buffer[16];
      rdb_slice_t key;

      rdb_fixed64_write(cache_key_buffer + 0, table->cache_id);
      rdb_fixed64_write(cache_key_buffer + 8, handle.offset);

      rdb_slice_set(&key, cache_key_buffer, sizeof(cache_key_buffer));

      cache_handle = rdb_lru_lookup(block_cache, &key);

      if (cache_handle != NULL) {
        block = (rdb_block_t *)rdb_lru_value(cache_handle);
      } else {
        rc = rdb_read_block(&contents, table->file, options, &handle);

        if (rc == RDB_OK) {
          block = rdb_block_create(&contents);

          if (contents.cachable && options->fill_cache) {
            cache_handle = rdb_lru_insert(block_cache,
                                          &key,
                                          block,
                                          block->size,
                                          &delete_cached_block);
          }
        }
      }
    } else {
      rc = rdb_read_block(&contents, table->file, options, &handle);

      if (rc == RDB_OK)
        block = rdb_block_create(&contents);
    }
  }

  if (block != NULL) {
    iter = rdb_blockiter_create(block, table->options.comparator);

    if (cache_handle == NULL)
      rdb_iter_register_cleanup(iter, &delete_block, block, NULL);
    else
      rdb_iter_register_cleanup(iter, &release_block, block_cache, cache_handle);
  } else {
    iter = rdb_emptyiter_create(rc);
  }

  return iter;
}

rdb_iter_t *
rdb_tableiter_create(const rdb_table_t *table, const rdb_readopt_t *options) {
  rdb_iter_t *iter = rdb_blockiter_create(table->index_block,
                                          table->options.comparator);

  return rdb_twoiter_create(iter,
                            &rdb_table_blockreader,
                            (void *)table,
                            options);
}

int
rdb_table_internal_get(rdb_table_t *table,
                       const rdb_readopt_t *options,
                       const rdb_slice_t *k,
                       void *arg,
                       void (*handle_result)(void *,
                                             const rdb_slice_t *,
                                             const rdb_slice_t *)) {
  rdb_iter_t *index_iter;
  int rc = RDB_OK;

  index_iter = rdb_blockiter_create(table->index_block,
                                    table->options.comparator);

  rdb_iter_seek(index_iter, k);

  if (rdb_iter_valid(index_iter)) {
    rdb_slice_t iter_value = rdb_iter_value(index_iter);
    rdb_filterreader_t *filter = table->filter;
    rdb_blockhandle_t handle;

    if (filter != NULL
        && rdb_blockhandle_import(&handle, &iter_value)
        && !rdb_filterreader_matches(filter, handle.offset, k)) {
      /* Not found. */
    } else {
      rdb_iter_t *block_iter = rdb_table_blockreader(table,
                                                     options,
                                                     &iter_value);

      rdb_iter_seek(block_iter, k);

      if (rdb_iter_valid(block_iter)) {
        rdb_slice_t block_iter_key = rdb_iter_key(block_iter);
        rdb_slice_t block_iter_value = rdb_iter_value(block_iter);

        (*handle_result)(arg, &block_iter_key, &block_iter_value);
      }

      rc = rdb_iter_status(block_iter);

      rdb_iter_destroy(block_iter);
    }
  }

  if (rc == RDB_OK)
    rc = rdb_iter_status(index_iter);

  rdb_iter_destroy(index_iter);

  return rc;
}

uint64_t
rdb_table_approximate_offsetof(const rdb_table_t *table,
                               const rdb_slice_t *key) {
  rdb_iter_t *index_iter;
  uint64_t result;

  index_iter = rdb_blockiter_create(table->index_block,
                                    table->options.comparator);

  rdb_iter_seek(index_iter, key);

  if (rdb_iter_valid(index_iter)) {
    rdb_slice_t input = rdb_iter_value(index_iter);
    rdb_blockhandle_t handle;

    if (rdb_blockhandle_import(&handle, &input)) {
      result = handle.offset;
    } else {
      /* Strange: we can't decode the block handle in the index block.
         We'll just return the offset of the metaindex block, which is
         close to the whole file size for this case. */
      result = table->metaindex_handle.offset;
    }
  } else {
    /* key is past the last key in the file. Approximate the offset
       by returning the offset of the metaindex block (which is
       right near the end of the file). */
    result = table->metaindex_handle.offset;
  }

  rdb_iter_destroy(index_iter);

  return result;
}
