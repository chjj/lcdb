/*!
 * hash.c - hash for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stddef.h>
#include <stdint.h>
#include "coding.h"
#include "hash.h"

uint32_t
rdb_hash(const uint8_t *data, size_t n, uint32_t seed) {
  /* Similar to murmur hash. */
  const uint8_t *limit = data + n;
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  uint32_t h = seed ^ (n * m);

  /* Pick up four bytes at a time. */
  while (data + 4 <= limit) {
    uint32_t w = rdb_fixed32_decode(data);
    data += 4;
    h += w;
    h *= m;
    h ^= (h >> 16);
  }

  /* Pick up remaining bytes. */
  switch (limit - data) {
    case 3:
      h += data[2] << 16;
      /* fallthrough */
    case 2:
      h += data[1] << 8;
      /* fallthrough */
    case 1:
      h += data[0];
      h *= m;
      h ^= (h >> r);
      break;
  }

  return h;
}
