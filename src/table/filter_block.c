/*!
 * filter_block.c - filter block builder/reader for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "../util/array.h"
#include "../util/bloom.h"
#include "../util/buffer.h"
#include "../util/coding.h"
#include "../util/slice.h"
#include "../util/vector.h"

#include "filter_block.h"

/*
 * Constants
 */

/* Generate new filter every 2KB of data. */
#define RDB_FILTER_BASE_LG 11
#define RDB_FILTER_BASE (1 << RDB_FILTER_BASE_LG)

/*
 * Filter Builder
 */

void
rdb_filterbuilder_init(rdb_filterbuilder_t *fb, const rdb_bloom_t *policy) {
  fb->policy = policy;

  rdb_buffer_init(&fb->keys);
  rdb_array_init(&fb->start);
  rdb_buffer_init(&fb->result);
  rdb_array_init(&fb->filter_offsets);
}

void
rdb_filterbuilder_clear(rdb_filterbuilder_t *fb) {
  rdb_buffer_clear(&fb->keys);
  rdb_array_clear(&fb->start);
  rdb_buffer_clear(&fb->result);
  rdb_array_clear(&fb->filter_offsets);
}

static void
rdb_filterbuilder_generate_filter(rdb_filterbuilder_t *fb);

void
rdb_filterbuilder_start_block(rdb_filterbuilder_t *fb, uint64_t block_offset) {
  uint64_t filter_index = (block_offset / RDB_FILTER_BASE);

  assert(filter_index >= fb->filter_offsets.length);

  while (filter_index > fb->filter_offsets.length)
    rdb_filterbuilder_generate_filter(fb);
}

void
rdb_filterbuilder_add_key(rdb_filterbuilder_t *fb, const rdb_slice_t *key) {
  rdb_slice_t k = *key;

  rdb_array_push(&fb->start, fb->keys.size);
  rdb_buffer_append(&fb->keys, k.data, k.size);
}

rdb_slice_t
rdb_filterbuilder_finish(rdb_filterbuilder_t *fb) {
  uint32_t array_offset;
  size_t i;

  if (fb->start.length > 0)
    rdb_filterbuilder_generate_filter(fb);

  /* Append array of per-filter offsets. */
  array_offset = fb->result.size;

  for (i = 0; i < fb->filter_offsets.length; i++)
    rdb_buffer_fixed32(&fb->result, fb->filter_offsets.items[i]);

  rdb_buffer_fixed32(&fb->result, array_offset);

  /* Save encoding parameter in result. */
  rdb_buffer_push(&fb->result, RDB_FILTER_BASE_LG);

  return fb->result;
}

static void
rdb_filterbuilder_generate_filter(rdb_filterbuilder_t *fb) {
  size_t num_keys = fb->start.length;
  size_t i, bytes, bits;
  uint8_t *data;

  if (num_keys == 0) {
    /* Fast path if there are no keys for this filter. */
    rdb_array_push(&fb->filter_offsets, fb->result.size);
    return;
  }

  /* Generate filter for current set of keys and append to result. */
  rdb_array_push(&fb->start, fb->keys.size); /* Simplify length computation. */
  rdb_array_push(&fb->filter_offsets, fb->result.size);

  bytes = rdb_bloom_size(fb->policy, num_keys);
  data = rdb_buffer_pad(&fb->result, bytes + 1);
  bits = bytes * 8;

  for (i = 0; i < num_keys; i++) {
    const uint8_t *base = fb->keys.data + fb->start.items[i];
    size_t length = fb->start.items[i + 1] - fb->start.items[i];
    rdb_slice_t key;

    rdb_slice_set(&key, base, length);

    fb->policy->add(fb->policy, data, &key, bits);
  }

  data[bytes] = fb->policy->k;

  rdb_buffer_reset(&fb->keys);
  rdb_array_reset(&fb->start);
}

/*
 * Filter Reader
 */

void
rdb_filterreader_init(rdb_filterreader_t *fr,
                      const rdb_bloom_t *policy,
                      const rdb_slice_t *contents) {
  size_t n, last_word;

  fr->policy = policy;
  fr->data = NULL;
  fr->offset = NULL;
  fr->num = 0;
  fr->base_lg = 0;

  n = contents->size;

  if (n < 5)
    return; /* 1 byte for base_lg and 4 for start of offset array. */

  fr->base_lg = contents->data[n - 1];

  last_word = rdb_fixed32_decode(contents->data + n - 5);

  if (last_word > n - 5)
    return;

  fr->data = contents->data;
  fr->offset = fr->data + last_word;
  fr->num = (n - 5 - last_word) / 4;
}

int
rdb_filterreader_matches(const rdb_filterreader_t *fr,
                         uint64_t block_offset,
                         const rdb_slice_t *key) {
  uint64_t index = block_offset >> fr->base_lg;

  if (index < fr->num) {
    uint32_t start = rdb_fixed32_decode(fr->offset + index * 4);
    uint32_t limit = rdb_fixed32_decode(fr->offset + index * 4 + 4);

    if (start <= limit && limit <= (size_t)(fr->offset - fr->data)) {
      rdb_slice_t filter;

      rdb_slice_set(&filter, fr->data + start, limit - start);

      return fr->policy->match(&filter, key);
    }

    if (start == limit) {
      /* Empty filters do not match any keys. */
      return 0;
    }
  }

  return 1; /* Errors are treated as potential matches. */
}
