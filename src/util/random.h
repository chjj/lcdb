/*!
 * random.h - random number generator for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_RANDOM_H
#define RDB_RANDOM_H

#include <stdint.h>

/* A very simple random number generator.  Not especially good at
   generating truly random bits, but good enough for our needs in this
   package. */

typedef struct rdb_rand_s {
  uint32_t seed;
} rdb_rand_t;

void
rdb_rand_init(rdb_rand_t *rnd, uint32_t seed);

uint32_t
rdb_rand_next(rdb_rand_t *rnd);

uint32_t
rdb_rand_uniform(rdb_rand_t *rnd, uint32_t n);

uint32_t
rdb_rand_one_in(rdb_rand_t *rnd, uint32_t n);

uint32_t
rdb_rand_skewed(rdb_rand_t *rnd, int max_log);

#endif /* RDB_RANDOM_H */
