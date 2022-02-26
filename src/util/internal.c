/*!
 * internal.c - internal utils for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdlib.h>
#include <string.h>
#include "internal.h"

/*
 * Helpers
 */

void
rdb__internal_no_empty(void);

void
rdb__internal_no_empty(void) {
  return;
}

#if 0
RDB_MALLOC void *
rdb_malloc(size_t size) {
  void *ptr = malloc(size);

  if (ptr == NULL)
    abort(); /* LCOV_EXCL_LINE */

  return ptr;
}

RDB_MALLOC void *
rdb_realloc(void *ptr, size_t size) {
  ptr = realloc(ptr, size);

  if (ptr == NULL)
    abort(); /* LCOV_EXCL_LINE */

  return ptr;
}

void
rdb_free(void *ptr) {
  if (ptr == NULL) {
    abort(); /* LCOV_EXCL_LINE */
    return;
  }

  free(ptr);
}

#if 0
int
rdb_memcmp4(const void *x, size_t xn, const void *y, size_t yn) {
  size_t n = xn < yn ? xn : yn;

  if (n > 0) {
    int cmp = memcmp(x, y, n);

    if (cmp != 0)
      return cmp;
  }

  if (xn != yn)
    return xn < yn ? -1 : 1;

  return 0;
}
#endif

int
rdb_memcmp4(const void *x, size_t xn, const void *y, size_t yn) {
  const unsigned char *xp = (const unsigned char *)x;
  const unsigned char *yp = (const unsigned char *)y;
  size_t n = xn < yn ? xn : yn;
  size_t i;

  for (i = 0; i < n; i++) {
    if (xp[i] != yp[i])
      return (int)xp[i] - (int)yp[i];
  }

  if (xn != yn)
    return xn < yn ? -1 : 1;

  return 0;
}
#endif
