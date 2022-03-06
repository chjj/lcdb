/*!
 * testutil.h - test utilities for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_TESTUTIL_H
#define RDB_TESTUTIL_H

#include <stddef.h>
#include <stdint.h>

#include "internal.h"
#include "types.h"

/*
 * Types
 */

struct rdb_rand_s;

/*
 * Assertions
 */

#undef ASSERT

#define ASSERT(expr) do {                       \
  if (UNLIKELY(!(expr)))                        \
    rdb_assert_fail(__FILE__, __LINE__, #expr); \
} while (0)

/*
 * Test Utils
 */

/* Returns the random seed used at the start of the current test run. */
uint32_t
rdb_random_seed(void);

/* Store in *dst a random string of length "len" and return a slice that
   references the generated data. */
rdb_slice_t *
rdb_random_string(rdb_buffer_t *dst, struct rdb_rand_s *rnd, size_t len);

/* Return a random key with the specified length that may contain interesting
   characters (e.g. \x00, \xff, etc.). */
rdb_slice_t *
rdb_random_key(rdb_buffer_t *dst, struct rdb_rand_s *rnd, size_t len);

/* Store in *dst a string of length "len" that will compress to
   "N*compressed_fraction" bytes and return a slice that references
   the generated data. */
rdb_slice_t *
rdb_compressible_string(rdb_buffer_t *dst,
                        struct rdb_rand_s *rnd,
                        double compressed_fraction,
                        size_t len);

#endif /* RDB_TESTUTIL_H */
