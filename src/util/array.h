/*!
 * array.h - integer vector for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_ARRAY_H
#define RDB_ARRAY_H

#include <stddef.h>
#include <stdint.h>
#include "types.h"

/*
 * Integer Vector
 */

void
rdb_array_init(rdb_array_t *z);

void
rdb_array_clear(rdb_array_t *z);

void
rdb_array_reset(rdb_array_t *z);

void
rdb_array_grow(rdb_array_t *z, size_t zn);

void
rdb_array_push(rdb_array_t *z, int64_t x);

int64_t
rdb_array_pop(rdb_array_t *z);

int64_t
rdb_array_top(const rdb_array_t *z);

void
rdb_array_resize(rdb_array_t *z, size_t zn);

void
rdb_array_copy(rdb_array_t *z, const rdb_array_t *x);

#endif /* RDB_ARRAY_H */
