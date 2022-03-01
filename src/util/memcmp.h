/*!
 * memcmp.h - memcmp for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_MEMCMP_H
#define RDB_MEMCMP_H

#include <stddef.h>
#include <string.h>
#include "internal.h"

RDB_STATIC int
rdb_memcmp4(const void *x, size_t xn, const void *y, size_t yn) {
  size_t n = xn < yn ? xn : yn;
  int r = n ? memcmp(x, y, n) : 0;

  if (r == 0) {
    if (xn < yn)
      r = -1;
    else if (xn > yn)
      r = +1;
  }

  return r;
}

#endif /* RDB_MEMCMP_H */
