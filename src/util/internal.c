/*!
 * internal.c - internal utils for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdio.h>
#include <stdlib.h>
#include "internal.h"

/*
 * Helpers
 */

RDB_NORETURN void
rdb_assert_fail(const char *file, int line, const char *expr) {
  /* LCOV_EXCL_START */
  fprintf(stderr, "%s:%d: Assertion `%s' failed.\n", file, line, expr);
  fflush(stderr);
  abort();
  /* LCOV_EXCL_STOP */
}

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
