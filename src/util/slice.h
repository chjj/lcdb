/*!
 * slice.h - slice for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_SLICE_H
#define RDB_SLICE_H

#include <stddef.h>
#include <stdint.h>
#include "internal.h"
#include "types.h"

/*
 * Slice
 */

#if 0
rdb_slice_t
rdb_slice(const uint8_t *xp, size_t xn);

void
rdb_slice_init(rdb_slice_t *z);

void
rdb_slice_reset(rdb_slice_t *z);

void
rdb_slice_set(rdb_slice_t *z, const uint8_t *xp, size_t xn);
#endif

RDB_STATIC rdb_slice_t
rdb_slice(const uint8_t *xp, size_t xn) {
  rdb_slice_t z;

  z.data = (uint8_t *)xp;
  z.size = xn;
  z.alloc = 0;

  return z;
}

RDB_STATIC void
rdb_slice_init(rdb_slice_t *z) {
  z->data = NULL;
  z->size = 0;
  z->alloc = 0;
}

RDB_STATIC void
rdb_slice_reset(rdb_slice_t *z) {
  z->data = NULL;
  z->size = 0;
  z->alloc = 0;
}

RDB_STATIC void
rdb_slice_set(rdb_slice_t *z, const uint8_t *xp, size_t xn) {
  z->data = (uint8_t *)xp;
  z->size = xn;
  z->alloc = 0;
}

void
rdb_slice_set_str(rdb_slice_t *z, const char *xp);

void
rdb_slice_copy(rdb_slice_t *z, const rdb_slice_t *x);

uint32_t
rdb_slice_hash(const rdb_slice_t *x);

int
rdb_slice_equal(const rdb_slice_t *x, const rdb_slice_t *y);

int
rdb_slice_compare(const rdb_slice_t *x, const rdb_slice_t *y);

void
rdb_slice_eat(rdb_slice_t *z, size_t xn);

size_t
rdb_slice_size(const rdb_slice_t *x);

uint8_t *
rdb_slice_write(uint8_t *zp, const rdb_slice_t *x);

void
rdb_slice_export(rdb_buffer_t *z, const rdb_slice_t *x);

int
rdb_slice_read(rdb_slice_t *z, const uint8_t **xp, size_t *xn);

int
rdb_slice_slurp(rdb_slice_t *z, rdb_slice_t *x);

int
rdb_slice_import(rdb_slice_t *z, const rdb_slice_t *x);

/* See GetLengthPrefixedSlice in memtable.cc. */
rdb_slice_t
rdb_slice_decode(const uint8_t *xp);

#endif /* RDB_SLICE_H */
