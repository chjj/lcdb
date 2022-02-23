/*!
 * arena.h - arena for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_ARENA_H
#define RDB_ARENA_H

#include <stddef.h>
#include <stdint.h>
#include "atomic.h"
#include "types.h"

/*
 * Types
 */

typedef struct rdb_arena_s {
  /* Allocation state. */
  uint8_t *data;
  size_t left;
  /* Total memory usage of the arena. */
  rdb_atomic(size_t) usage;
  /* Array of allocated memory blocks. */
  rdb_vector_t blocks;
} rdb_arena_t;

/*
 * Arena
 */

void
rdb_arena_init(rdb_arena_t *arena);

void
rdb_arena_clear(rdb_arena_t *arena);

/* Returns an estimate of the total memory usage of data allocated
   by the arena. */
size_t
rdb_arena_usage(const rdb_arena_t *arena);

/* Return a pointer to a newly allocated memory block of "bytes" bytes. */
void *
rdb_arena_alloc(rdb_arena_t *arena, size_t size);

/* Allocate memory with the normal alignment guarantees provided by malloc. */
void *
rdb_arena_alloc_aligned(rdb_arena_t *arena, size_t size);

#endif /* RDB_ARENA_H */
