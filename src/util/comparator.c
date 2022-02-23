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

static void
find_shortest_separator(rdb_buffer_t *start, const rdb_slice_t *limit) {
  /* Find length of common prefix. */
  size_t min_length = RDB_MIN(start->size, limit->size);
  size_t diff_index = 0;

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
find_short_successor(rdb_buffer_t *key) {
  /* Find first character that can be incremented. */
  size_t i;

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
  /* .compare = */ rdb_slice_compare,
  /* .find_shortest_separator = */ find_shortest_separator,
  /* .find_short_successor = */ find_short_successor
};

/*
 * Globals
 */

const rdb_comparator_t *rdb_bytewise_comparator = &bytewise_comparator;
