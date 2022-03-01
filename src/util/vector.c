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
  z->length = 0;
  z->alloc = 0;
}

void
rdb_vector_clear(rdb_vector_t *z) {
  if (z->alloc > 0)
    rdb_free(z->items);

  z->items = NULL;
  z->length = 0;
  z->alloc = 0;
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

void
rdb_vector_swap(rdb_vector_t *x, rdb_vector_t *y) {
  rdb_vector_t t = *x;
  *x = *y;
  *y = t;
}

/**
 * Quicksort (faster than libc's qsort -- no memcpy necessary)
 * https://en.wikipedia.org/wiki/Quicksort#Hoare_partition_scheme
 */

static void
rdb_swap(void **items, int i, int j) {
  void *item = items[i];

  items[i] = items[j];
  items[j] = item;
}

static int
rdb_partition(void **items, int lo, int hi, int (*cmp)(void *, void *)) {
  void *pivot = items[(hi + lo) >> 1];
  int i = lo - 1;
  int j = hi + 1;

  for (;;) {
    do i++; while (i < hi && cmp(items[i], pivot) < 0);
    do j--; while (j > lo && cmp(items[j], pivot) > 0);

    if (i >= j)
      return j;

    rdb_swap(items, i, j);
  }
}

static void
rdb_qsort(void **items, int lo, int hi, int (*cmp)(void *, void *)) {
  if (lo >= 0 && hi >= 0 && lo < hi) {
    int p = rdb_partition(items, lo, hi, cmp);

    rdb_qsort(items, lo, p, cmp);
    rdb_qsort(items, p + 1, hi, cmp);
  }
}

void
rdb_vector_sort(rdb_vector_t *z, int (*cmp)(void *, void *)) {
  if (z->length > 1)
    rdb_qsort(z->items, 0, z->length - 1, cmp);
}
