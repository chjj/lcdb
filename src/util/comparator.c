/*!
 * comparator.c - comparator for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stddef.h>
#include <stdint.h>

#include "buffer.h"
#include "comparator.h"
#include "internal.h"
#include "slice.h"

/*
 * Bytewise Comparator
 */

static int
slice_compare(const rdb_comparator_t *comparator,
              const rdb_slice_t *x,
              const rdb_slice_t *y) {
  (void)comparator;
  return rdb_memcmp4(x->data, x->size, y->data, y->size);
}

static void
shortest_separator(const rdb_comparator_t *comparator,
                   rdb_buffer_t *start,
                   const rdb_slice_t *limit) {
  /* Find length of common prefix. */
  size_t min_length = RDB_MIN(start->size, limit->size);
  size_t diff_index = 0;

  (void)comparator;

  while (diff_index < min_length &&
         start->data[diff_index] == limit->data[diff_index]) {
    diff_index++;
  }

  if (diff_index >= min_length) {
    /* Do not shorten if one string is a prefix of the other. */
  } else {
    uint8_t diff_byte = start->data[diff_index];

    if (diff_byte < 0xff && diff_byte + 1 < limit->data[diff_index]) {
      start->data[diff_index]++;

      rdb_buffer_resize(start, diff_index + 1);
    }
  }
}

static void
short_successor(const rdb_comparator_t *comparator, rdb_buffer_t *key) {
  /* Find first character that can be incremented. */
  size_t i;

  (void)comparator;

  for (i = 0; i < key->size; i++) {
    if (key->data[i] != 0xff) {
      key->data[i] += 1;
      rdb_buffer_resize(key, i + 1);
      return;
    }
  }

  /* key is a run of 0xffs. Leave it alone. */
}

static const rdb_comparator_t bytewise_comparator = {
  /* .name = */ "leveldb.BytewiseComparator",
  /* .compare = */ slice_compare,
  /* .shortest_separator = */ shortest_separator,
  /* .short_successor = */ short_successor,
  /* .user_comparator = */ NULL
};

/*
 * Globals
 */

const rdb_comparator_t *rdb_bytewise_comparator = &bytewise_comparator;
