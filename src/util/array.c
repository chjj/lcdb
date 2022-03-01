/*!
 * array.c - integer vector for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "array.h"
#include "internal.h"

/*
 * Integer Vector
 */

void
rdb_array_init(rdb_array_t *z) {
  z->items = NULL;
  z->length = 0;
  z->alloc = 0;
}

void
rdb_array_clear(rdb_array_t *z) {
  if (z->alloc > 0)
    rdb_free(z->items);

  z->items = NULL;
  z->length = 0;
  z->alloc = 0;
}

void
rdb_array_reset(rdb_array_t *z) {
  z->length = 0;
}

void
rdb_array_grow(rdb_array_t *z, size_t zn) {
  if (zn > z->alloc) {
    z->items = (int64_t *)rdb_realloc(z->items, zn * sizeof(int64_t));
    z->alloc = zn;
  }
}

void
rdb_array_push(rdb_array_t *z, int64_t x) {
  if (z->length == z->alloc)
    rdb_array_grow(z, (z->alloc * 3) / 2 + (z->alloc <= 1));

  z->items[z->length++] = x;
}

int64_t
rdb_array_pop(rdb_array_t *z) {
  assert(z->length > 0);
  return z->items[--z->length];
}

int64_t
rdb_array_top(const rdb_array_t *z) {
  assert(z->length > 0);
  return z->items[z->length - 1];
}

void
rdb_array_resize(rdb_array_t *z, size_t zn) {
  rdb_array_grow(z, zn);
  z->length = zn;
}

void
rdb_array_copy(rdb_array_t *z, const rdb_array_t *x) {
  size_t i;

  rdb_array_resize(z, x->length);

  for (i = 0; i < x->length; i++)
    z->items[i] = x->items[i];
}

void
rdb_array_swap(rdb_array_t *x, rdb_array_t *y) {
  rdb_array_t t = *x;
  *x = *y;
  *y = t;
}

/**
 * Quicksort (faster than libc's qsort -- no memcpy necessary)
 * https://en.wikipedia.org/wiki/Quicksort#Hoare_partition_scheme
 */

static void
rdb_swap(int64_t *items, int i, int j) {
  int64_t item = items[i];

  items[i] = items[j];
  items[j] = item;
}

static int
rdb_partition(int64_t *items, int lo, int hi, int (*cmp)(int64_t, int64_t)) {
  int64_t pivot = items[(hi + lo) >> 1];
  int i = lo - 1;
  int j = hi + 1;

  for (;;) {
    do i++; while (cmp(items[i], pivot) < 0);
    do j--; while (cmp(items[j], pivot) > 0);

    if (i >= j)
      return j;

    rdb_swap(items, i, j);
  }
}

static void
rdb_qsort(int64_t *items, int lo, int hi, int (*cmp)(int64_t, int64_t)) {
  if (lo >= 0 && hi >= 0 && lo < hi) {
    int p = rdb_partition(items, lo, hi, cmp);

    rdb_qsort(items, lo, p, cmp);
    rdb_qsort(items, p + 1, hi, cmp);
  }
}

void
rdb_array_sort(rdb_array_t *z, int (*cmp)(int64_t, int64_t)) {
  if (z->length > 1)
    rdb_qsort(z->items, 0, z->length - 1, cmp);
}
