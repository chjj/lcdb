/*!
 * slice.h - slice for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_SLICE_H
#define RDB_SLICE_H

#include <stddef.h>
#include <stdint.h>
#include "types.h"

/*
 * Slice
 */

void
rdb_slice_init(rdb_slice_t *z);

void
rdb_slice_reset(rdb_slice_t *z);

void
rdb_slice_set(rdb_slice_t *z, const uint8_t *xp, size_t xn);

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

#endif /* RDB_SLICE_H */
