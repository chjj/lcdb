/*!
 * table_builder.c - sorted string table builder for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

#include "../util/bloom.h"
#include "../util/buffer.h"
#include "../util/coding.h"
#include "../util/comparator.h"
#include "../util/crc32c.h"
#include "../util/env.h"
#include "../util/internal.h"
#include "../util/options.h"
#include "../util/slice.h"
#include "../util/snappy.h"
#include "../util/status.h"

#include "block_builder.h"
#include "filter_block.h"
#include "format.h"
#include "table_builder.h"

/*
 * Table Builder
 */

struct rdb_tablebuilder_s {
  rdb_dbopt_t options;
  rdb_dbopt_t index_block_options;
  rdb_wfile_t *file;
  uint64_t offset;
  int status;
  rdb_blockbuilder_t data_block;
  rdb_blockbuilder_t index_block;
  rdb_buffer_t last_key;
  int64_t num_entries;
  int closed; /* Either finish() or abandon() has been called. */
  rdb_filterbuilder_t filter_block_;
  rdb_filterbuilder_t *filter_block;

  /* We do not emit the index entry for a block until we have seen the
     first key for the next data block.  This allows us to use shorter
     keys in the index block.  For example, consider a block boundary
     between the keys "the quick brown fox" and "the who".  We can use
     "the r" as the key for the index block entry since it is >= all
     entries in the first block and < all entries in subsequent
     blocks. */
  /* Invariant: tb->pending_index_entry is true only if data_block is empty. */
  int pending_index_entry;
  rdb_blockhandle_t pending_handle; /* Handle to add to index block. */
  rdb_buffer_t compressed_output;
};

static void
rdb_tablebuilder_init(rdb_tablebuilder_t *tb,
                      const rdb_dbopt_t *options,
                      rdb_wfile_t *file) {
  tb->options = *options;
  tb->index_block_options = *options;
  tb->file = file;
  tb->offset = 0;
  tb->status = RDB_OK;

  rdb_blockbuilder_init(&tb->data_block, &tb->options);
  rdb_blockbuilder_init(&tb->index_block, &tb->index_block_options);

  rdb_buffer_init(&tb->last_key);

  tb->num_entries = 0;
  tb->closed = 0;
  tb->filter_block = NULL;
  tb->pending_index_entry = 0;

  rdb_blockhandle_init(&tb->pending_handle);
  rdb_buffer_init(&tb->compressed_output);

  tb->index_block_options.block_restart_interval = 1;

  if (options->filter_policy != NULL) {
    tb->filter_block = &tb->filter_block_;

    rdb_filterbuilder_init(tb->filter_block, options->filter_policy);
    rdb_filterbuilder_start_block(tb->filter_block, 0);
  }
}

static void
rdb_tablebuilder_clear(rdb_tablebuilder_t *tb) {
  assert(tb->closed); /* Catch errors where caller forgot to call finish(). */

  rdb_blockbuilder_clear(&tb->data_block);
  rdb_blockbuilder_clear(&tb->index_block);

  rdb_buffer_clear(&tb->last_key);
  rdb_buffer_clear(&tb->compressed_output);

  if (tb->filter_block != NULL)
    rdb_filterbuilder_clear(tb->filter_block);
}

rdb_tablebuilder_t *
rdb_tablebuilder_create(const rdb_dbopt_t *options, rdb_wfile_t *file) {
  rdb_tablebuilder_t *tb = rdb_malloc(sizeof(rdb_tablebuilder_t));
  rdb_tablebuilder_init(tb, options, file);
  return tb;
}

void
rdb_tablebuilder_destroy(rdb_tablebuilder_t *tb) {
  rdb_tablebuilder_clear(tb);
  rdb_free(tb);
}

int
rdb_tablebuilder_ok(const rdb_tablebuilder_t *tb) {
  return tb->status == RDB_OK;
}

int
rdb_tablebuilder_change_options(rdb_tablebuilder_t *tb,
                                const rdb_dbopt_t *options) {
  /* Note: if more fields are added to Options, update
     this function to catch changes that should not be allowed to
     change in the middle of building a Table. */
  if (options->comparator != tb->options.comparator)
    return RDB_INVALID; /* "changing comparator while building table" */

  /* Note that any live BlockBuilders point to tb->options and therefore
     will automatically pick up the updated options. */
  tb->options = *options;
  tb->index_block_options = *options;
  tb->index_block_options.block_restart_interval = 1;

  return RDB_OK;
}

static void
rdb_tablebuilder_write_raw_block(rdb_tablebuilder_t *tb,
                                 const rdb_slice_t *block_contents,
                                 enum rdb_compression type,
                                 rdb_blockhandle_t *handle) {
  handle->offset = tb->offset;
  handle->size = block_contents->size;

  tb->status = rdb_wfile_append(tb->file, block_contents);

  if (tb->status == RDB_OK) {
    uint8_t trailer[RDB_BLOCK_TRAILER_SIZE];
    rdb_slice_t trail;
    uint32_t crc;

    trailer[0] = type;

    crc = rdb_crc32c_value(block_contents->data, block_contents->size);
    crc = rdb_crc32c_extend(crc, trailer, 1); /* Extend crc to cover block type. */

    rdb_fixed32_write(trailer + 1, rdb_crc32c_mask(crc));

    rdb_slice_set(&trail, trailer, sizeof(trailer));

    tb->status = rdb_wfile_append(tb->file, &trail);

    if (tb->status == RDB_OK)
      tb->offset += block_contents->size + sizeof(trailer);
  }
}

static void
rdb_tablebuilder_write_block(rdb_tablebuilder_t *tb,
                             rdb_blockbuilder_t *block,
                             rdb_blockhandle_t *handle) {
  /* File format contains a sequence of blocks where each block has:
   *
   *    block_data: uint8[n]
   *    type: uint8
   *    crc: uint32
   */
  rdb_slice_t raw, block_contents;
  enum rdb_compression type;

  assert(rdb_tablebuilder_ok(tb));

  raw = rdb_blockbuilder_finish(block);
  type = tb->options.compression;

  switch (type) {
    case RDB_NO_COMPRESSION: {
      block_contents = raw;
      break;
    }

    case RDB_SNAPPY_COMPRESSION: {
      rdb_buffer_t *compressed = &tb->compressed_output;
      int size = rdb_snappy_encode_size(raw.size);

      assert(size >= 0);

      rdb_buffer_grow(compressed, size);

      compressed->size = rdb_snappy_encode(compressed->data,
                                           raw.data, raw.size);

      if (compressed->size < raw.size - (raw.size / 8)) {
        block_contents = *compressed;
      } else {
        /* Snappy not supported, or compressed less than
           12.5%, so just store uncompressed form. */
        block_contents = raw;
        type = RDB_NO_COMPRESSION;
      }

      break;
    }

    default: {
      abort(); /* LCOV_EXCL_LINE */
      break;
    }
  }

  rdb_tablebuilder_write_raw_block(tb, &block_contents, type, handle);

  /* rdb_buffer_reset(&tb->compressed_output); */

  rdb_blockbuilder_reset(block);
}

void
rdb_tablebuilder_add(rdb_tablebuilder_t *tb,
                     const rdb_slice_t *key,
                     const rdb_slice_t *value) {
  size_t estimated_block_size;

  assert(!tb->closed);

  if (!rdb_tablebuilder_ok(tb))
    return;

  if (tb->num_entries > 0)
    assert(rdb_compare(tb->options.comparator, key, &tb->last_key) > 0);

  if (tb->pending_index_entry) {
    uint8_t tmp[RDB_BLOCKHANDLE_MAX];
    rdb_buffer_t handle_encoding;

    assert(rdb_blockbuilder_empty(&tb->data_block));

    rdb_shortest_separator(tb->options.comparator, &tb->last_key, key);
    rdb_buffer_rwset(&handle_encoding, tmp, sizeof(tmp));
    rdb_blockhandle_export(&handle_encoding, &tb->pending_handle);
    rdb_blockbuilder_add(&tb->index_block, &tb->last_key, &handle_encoding);
    tb->pending_index_entry = 0;
  }

  if (tb->filter_block != NULL)
    rdb_filterbuilder_add_key(tb->filter_block, key);

  /* rdb_buffer_set(&tb->last_key, key->data, key->size); */
  rdb_buffer_copy(&tb->last_key, key);

  tb->num_entries++;

  rdb_blockbuilder_add(&tb->data_block, key, value);

  estimated_block_size = rdb_blockbuilder_size_estimate(&tb->data_block);

  if (estimated_block_size >= tb->options.block_size)
    rdb_tablebuilder_flush(tb);
}

void
rdb_tablebuilder_flush(rdb_tablebuilder_t *tb) {
  assert(!tb->closed);

  if (!rdb_tablebuilder_ok(tb))
    return;

  if (rdb_blockbuilder_empty(&tb->data_block))
    return;

  assert(!tb->pending_index_entry);

  rdb_tablebuilder_write_block(tb, &tb->data_block, &tb->pending_handle);

  if (rdb_tablebuilder_ok(tb)) {
    tb->pending_index_entry = 1;
    tb->status = rdb_wfile_flush(tb->file);
  }

  if (tb->filter_block != NULL)
    rdb_filterbuilder_start_block(tb->filter_block, tb->offset);
}

int
rdb_tablebuilder_status(const rdb_tablebuilder_t *tb) {
  return tb->status;
}

int
rdb_tablebuilder_finish(rdb_tablebuilder_t *tb) {
  rdb_blockhandle_t filter_handle;
  rdb_blockhandle_t metaindex_handle;
  rdb_blockhandle_t index_handle;

  rdb_tablebuilder_flush(tb);

  assert(!tb->closed);

  tb->closed = 1;

  /* Write filter block. */
  if (rdb_tablebuilder_ok(tb) && tb->filter_block != NULL) {
    rdb_slice_t contents = rdb_filterbuilder_finish(tb->filter_block);

    rdb_tablebuilder_write_raw_block(tb,
                                     &contents,
                                     RDB_NO_COMPRESSION,
                                     &filter_handle);
  }

  /* Write metaindex block. */
  if (rdb_tablebuilder_ok(tb)) {
    rdb_blockbuilder_t metaindex_block;

    rdb_blockbuilder_init(&metaindex_block, &tb->options);

    if (tb->filter_block != NULL) {
      /* Add mapping from "filter.Name" to location of filter data. */
      uint8_t tmp[RDB_BLOCKHANDLE_MAX];
      rdb_buffer_t handle_encoding;
      rdb_slice_t key;
      char name[72];

      if (!rdb_bloom_name(name, sizeof(name), tb->options.filter_policy)) {
        rdb_blockbuilder_clear(&metaindex_block);
        return RDB_INVALID;
      }

      rdb_slice_set_str(&key, name);
      rdb_buffer_rwset(&handle_encoding, tmp, sizeof(tmp));
      rdb_blockhandle_export(&handle_encoding, &filter_handle);
      rdb_blockbuilder_add(&metaindex_block, &key, &handle_encoding);
    }

    rdb_tablebuilder_write_block(tb, &metaindex_block, &metaindex_handle);

    rdb_blockbuilder_clear(&metaindex_block);
  }

  /* Write index block. */
  if (rdb_tablebuilder_ok(tb)) {
    if (tb->pending_index_entry) {
      uint8_t tmp[RDB_BLOCKHANDLE_MAX];
      rdb_buffer_t handle_encoding;

      rdb_short_successor(tb->options.comparator, &tb->last_key);
      rdb_buffer_rwset(&handle_encoding, tmp, sizeof(tmp));
      rdb_blockhandle_export(&handle_encoding, &tb->pending_handle);
      rdb_blockbuilder_add(&tb->index_block, &tb->last_key, &handle_encoding);
      tb->pending_index_entry = 0;
    }

    rdb_tablebuilder_write_block(tb, &tb->index_block, &index_handle);
  }

  /* Write footer. */
  if (rdb_tablebuilder_ok(tb)) {
    uint8_t tmp[RDB_FOOTER_SIZE];
    rdb_buffer_t footer_encoding;
    rdb_footer_t footer;

    footer.metaindex_handle = metaindex_handle;
    footer.index_handle = index_handle;

    rdb_buffer_rwset(&footer_encoding, tmp, sizeof(tmp));
    rdb_footer_export(&footer_encoding, &footer);

    tb->status = rdb_wfile_append(tb->file, &footer_encoding);

    if (tb->status == RDB_OK)
      tb->offset += footer_encoding.size;
  }

  return tb->status;
}

void
rdb_tablebuilder_abandon(rdb_tablebuilder_t *tb) {
  assert(!tb->closed);
  tb->closed = 1;
}

uint64_t
rdb_tablebuilder_num_entries(const rdb_tablebuilder_t *tb) {
  return tb->num_entries;
}

uint64_t
rdb_tablebuilder_file_size(const rdb_tablebuilder_t *tb) {
  return tb->offset;
}
