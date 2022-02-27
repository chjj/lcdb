/*!
 * vector.h - shallow vector for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_VECTOR_H
#define RDB_VECTOR_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include "types.h"

/*
 * Vector
 */

void
rdb_vector_init(rdb_vector_t *z);

void
rdb_vector_clear(rdb_vector_t *z);

void
rdb_vector_reset(rdb_vector_t *z);

void
rdb_vector_grow(rdb_vector_t *z, size_t zn);

void
rdb_vector_push(rdb_vector_t *z, const void *x);

void *
rdb_vector_pop(rdb_vector_t *z);

void *
rdb_vector_top(const rdb_vector_t *z);

void
rdb_vector_resize(rdb_vector_t *z, size_t zn);

void
rdb_vector_copy(rdb_vector_t *z, const rdb_vector_t *x);

void
rdb_vector_swap(rdb_vector_t *x, rdb_vector_t *y);

void
rdb_vector_sort(rdb_vector_t *z, int (*cmp)(void *, void *));

#ifdef __cplusplus
}
#endif

#endif /* RDB_VECTOR_H */
