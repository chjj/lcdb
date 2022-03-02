/*!
 * hash.h - hash for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_HASH_H
#define RDB_HASH_H

#include <stddef.h>
#include <stdint.h>

/*
 * Hash
 */

uint32_t
rdb_hash(const uint8_t *data, size_t size, uint32_t seed);

#endif /* RDB_HASH_H */
