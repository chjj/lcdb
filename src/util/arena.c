/*!
 * arena.c - arena for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include "arena.h"
#include "atomic.h"
#include "internal.h"
#include "vector.h"

/*
 * Constants
 */

#define RDB_ARENA_BLOCK 4096

/*
 * Arena
 */

void
rdb_arena_init(rdb_arena_t *arena) {
  arena->data = NULL;
  arena->left = 0;
  arena->usage = 0;

  rdb_vector_init(&arena->blocks);
}

void
rdb_arena_clear(rdb_arena_t *arena) {
  size_t i;

  for (i = 0; i < arena->blocks.length; i++)
    rdb_free(arena->blocks.items[i]);

  rdb_vector_clear(&arena->blocks);
}

size_t
rdb_arena_usage(const rdb_arena_t *arena) {
  return rdb_atomic_load(&arena->usage, rdb_order_relaxed);
}

static void *
rdb_arena_alloc_block(rdb_arena_t *arena, size_t size) {
  void *result = rdb_malloc(size);

  rdb_vector_push(&arena->blocks, result);

  rdb_atomic_fetch_add(&arena->usage,
                       size + sizeof(void *),
                       rdb_order_relaxed);

  return result;
}

static void *
rdb_arena_alloc_fallback(rdb_arena_t *arena, size_t size) {
  void *result;

  if (size > RDB_ARENA_BLOCK / 4) {
    /* Object is more than a quarter of our block size.
       Allocate it separately to avoid wasting too much
       space in leftover bytes. */
    return rdb_arena_alloc_block(arena, size);
  }

  /* We waste the remaining space in the current block. */
  arena->data = rdb_arena_alloc_block(arena, RDB_ARENA_BLOCK);
  arena->left = RDB_ARENA_BLOCK;

  result = arena->data;

  arena->data += size;
  arena->left -= size;

  return result;
}

void *
rdb_arena_alloc(rdb_arena_t *arena, size_t size) {
  /* The semantics of what to return are a bit messy if we allow
     0-byte allocations, so we disallow them here (we don't need
     them for our internal use). */
  assert(size > 0);

  if (size <= arena->left) {
    void *result = arena->data;

    arena->data += size;
    arena->left -= size;

    return result;
  }

  return rdb_arena_alloc_fallback(arena, size);
}

void *
rdb_arena_alloc_aligned(rdb_arena_t *arena, size_t size) {
  static const int align = sizeof(void *) > 8 ? sizeof(void *) : 8;
  size_t current_mod = (uintptr_t)arena->data & (align - 1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = size + slop;
  void *result;

  assert((align & (align - 1)) == 0);

  if (needed <= arena->left) {
    result = arena->data + slop;
    arena->data += needed;
    arena->left -= needed;
  } else {
    /* rdb_arena_alloc_fallback always returns aligned memory. */
    result = rdb_arena_alloc_fallback(arena, size);
  }

  assert(((uintptr_t)result & (align - 1)) == 0);

  return result;
}
