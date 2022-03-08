/*!
 * tests.h - test utilities for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_TESTS_H
#define RDB_TESTS_H

/*
 * Assertions
 */

#undef ASSERT

#define ASSERT(expr) do {                       \
  if (!(expr))                                  \
    rdb_assert_fail(__FILE__, __LINE__, #expr); \
} while (0)

/*
 * Functions
 */

void
rdb_assert_fail(const char *file, int line, const char *expr);

#endif /* RDB_TESTS_H */
