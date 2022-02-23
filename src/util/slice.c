/*!
 * slice.c - slice for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "buffer.h"
#include "coding.h"
#include "hash.h"
#include "internal.h"
#include "slice.h"

/*
 * Slice
 */

void
rdb_slice_init(rdb_slice_t *z) {
  z->data = NULL;
  z->size = 0;
  z->alloc = 0;
}

void
rdb_slice_reset(rdb_slice_t *z) {
  z->size = 0;
}

void
rdb_slice_set(rdb_slice_t *z, const uint8_t *xp, size_t xn) {
  z->data = (uint8_t *)xp;
  z->size = xn;
  z->alloc = 0;
}

void
rdb_slice_set_str(rdb_slice_t *z, const char *xp) {
  rdb_slice_set(z, (const uint8_t *)xp, strlen(xp));
}

void
rdb_slice_copy(rdb_slice_t *z, const rdb_slice_t *x) {
  rdb_slice_set(z, x->data, x->size);
}

uint32_t
rdb_slice_hash(const rdb_slice_t *x) {
  return rdb_hash(x->data, x->size, 0);
}

int
rdb_slice_equal(const rdb_slice_t *x, const rdb_slice_t *y) {
  if (x->size != y->size)
    return 0;

  if (x->size == 0)
    return 1;

  return memcmp(x->data, y->data, y->size) == 0;
}

int
rdb_slice_compare(const rdb_slice_t *x, const rdb_slice_t *y) {
  return rdb_memcmp4(x->data, x->size, y->data, y->size);
}

size_t
rdb_slice_size(const rdb_slice_t *x) {
  return rdb_size_size(x->size) + x->size;
}

uint8_t *
rdb_slice_write(uint8_t *zp, const rdb_slice_t *x) {
  zp = rdb_size_write(zp, x->size);
  zp = rdb_raw_write(zp, x->data, x->size);
  return zp;
}

void
rdb_slice_export(rdb_buffer_t *z, const rdb_slice_t *x) {
  uint8_t *zp = rdb_buffer_expand(z, 5 + x->size);
  size_t xn = rdb_slice_write(zp, x) - zp;

  z->size += xn;
}

int
rdb_slice_read(rdb_slice_t *z, const uint8_t **xp, size_t *xn) {
  const uint8_t *zp;
  size_t zn;

  if (!rdb_size_read(&zn, xp, xn))
    return 0;

  if (!rdb_zraw_read(&zp, zn, xp, xn))
    return 0;

  rdb_slice_set(z, zp, zn);

  return 1;
}

int
rdb_slice_slurp(rdb_slice_t *z, rdb_slice_t *x) {
  return rdb_slice_read(z, (const uint8_t **)&x->data, &x->size);
}

int
rdb_slice_import(rdb_slice_t *z, const rdb_slice_t *x) {
  rdb_slice_t tmp = *x;
  return rdb_slice_slurp(z, &tmp);
}
