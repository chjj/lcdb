/*!
 * buffer.h - buffer for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_BUFFER_H
#define RDB_BUFFER_H

#include <stddef.h>
#include <stdint.h>
#include "types.h"

/*
 * Buffer
 */

void
rdb_buffer_init(rdb_buffer_t *z);

void
rdb_buffer_clear(rdb_buffer_t *z);

void
rdb_buffer_reset(rdb_buffer_t *z);

uint8_t *
rdb_buffer_grow(rdb_buffer_t *z, size_t zn);

uint8_t *
rdb_buffer_expand(rdb_buffer_t *z, size_t xn);

uint8_t *
rdb_buffer_resize(rdb_buffer_t *z, size_t zn);

void
rdb_buffer_set(rdb_buffer_t *z, const uint8_t *xp, size_t xn);

void
rdb_buffer_set_str(rdb_buffer_t *z, const char *xp);

void
rdb_buffer_copy(rdb_buffer_t *z, const rdb_buffer_t *x);

void
rdb_buffer_swap(rdb_buffer_t *x, rdb_buffer_t *y);

void
rdb_buffer_roset(rdb_buffer_t *z, const uint8_t *xp, size_t xn);

void
rdb_buffer_rocopy(rdb_buffer_t *z, const rdb_buffer_t *x);

void
rdb_buffer_rwset(rdb_buffer_t *z, uint8_t *zp, size_t zn);

uint32_t
rdb_buffer_hash(const rdb_buffer_t *x);

int
rdb_buffer_equal(const rdb_buffer_t *x, const rdb_buffer_t *y);

int
rdb_buffer_compare(const rdb_buffer_t *x, const rdb_buffer_t *y);

void
rdb_buffer_push(rdb_buffer_t *z, int x);

void
rdb_buffer_append(rdb_buffer_t *z, const uint8_t *xp, size_t xn);

void
rdb_buffer_concat(rdb_buffer_t *z, const rdb_slice_t *x);

void
rdb_buffer_string(rdb_buffer_t *z, const char *xp);

void
rdb_buffer_number(rdb_buffer_t *z, uint64_t x);

void
rdb_buffer_escape(rdb_buffer_t *z, const rdb_slice_t *x);

uint8_t *
rdb_buffer_pad(rdb_buffer_t *z, size_t xn);

void
rdb_buffer_fixed32(rdb_buffer_t *z, uint32_t x);

void
rdb_buffer_fixed64(rdb_buffer_t *z, uint64_t x);

void
rdb_buffer_varint32(rdb_buffer_t *z, uint32_t x);

void
rdb_buffer_varint64(rdb_buffer_t *z, uint64_t x);

size_t
rdb_buffer_size(const rdb_buffer_t *x);

uint8_t *
rdb_buffer_write(uint8_t *zp, const rdb_buffer_t *x);

void
rdb_buffer_export(rdb_buffer_t *z, const rdb_buffer_t *x);

int
rdb_buffer_read(rdb_buffer_t *z, const uint8_t **xp, size_t *xn);

int
rdb_buffer_slurp(rdb_buffer_t *z, rdb_slice_t *x);

int
rdb_buffer_import(rdb_buffer_t *z, const rdb_slice_t *x);

#endif /* RDB_BUFFER_H */
