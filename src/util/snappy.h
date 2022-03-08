/*!
 * snappy.h - snappy for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_SNAPPY_H
#define RDB_SNAPPY_H

#include <stddef.h>
#include <stdint.h>

/*
 * Snappy
 */

#define snappy_encode_size rdb_snappy_encode_size
#define snappy_encode rdb_snappy_encode
#define snappy_decode_size rdb_snappy_decode_size
#define snappy_decode rdb_snappy_decode

int
snappy_encode_size(size_t *zn, size_t xn);

size_t
snappy_encode(uint8_t *zp, const uint8_t *xp, size_t xn);

int
snappy_decode_size(size_t *zn, const uint8_t *xp, size_t xn);

int
snappy_decode(uint8_t *zp, const uint8_t *xp, size_t xn);

#endif /* RDB_SNAPPY_H */
