/*!
 * vector.c - shallow vector for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "internal.h"
#include "vector.h"

/*
 * Vector
 */

void
rdb_vector_init(rdb_vector_t *z) {
  z->items = NULL;
  z->alloc = 0;
  z->length = 0;
}

void
rdb_vector_clear(rdb_vector_t *z) {
  if (z->alloc > 0)
    rdb_free(z->items);

  z->items = NULL;
  z->alloc = 0;
  z->length = 0;
}

void
rdb_vector_reset(rdb_vector_t *z) {
  z->length = 0;
}

void
rdb_vector_grow(rdb_vector_t *z, size_t zn) {
  if (zn > z->alloc) {
    z->items = (void **)rdb_realloc(z->items, zn * sizeof(void *));
    z->alloc = zn;
  }
}

void
rdb_vector_push(rdb_vector_t *z, const void *x) {
  if (z->length == z->alloc)
    rdb_vector_grow(z, (z->alloc * 3) / 2 + (z->alloc <= 1));

  z->items[z->length++] = (void *)x;
}

void *
rdb_vector_pop(rdb_vector_t *z) {
  assert(z->length > 0);
  return z->items[--z->length];
}

void *
rdb_vector_top(const rdb_vector_t *z) {
  assert(z->length > 0);
  return (void *)z->items[z->length - 1];
}

void
rdb_vector_resize(rdb_vector_t *z, size_t zn) {
  rdb_vector_grow(z, zn);
  z->length = zn;
}

void
rdb_vector_copy(rdb_vector_t *z, const rdb_vector_t *x) {
  size_t i;

  rdb_vector_resize(z, x->length);

  for (i = 0; i < x->length; i++)
    z->items[i] = x->items[i];
}
