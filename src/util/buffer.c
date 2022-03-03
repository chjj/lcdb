/*!
 * buffer.c - buffer for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "buffer.h"
#include "coding.h"
#include "hash.h"
#include "internal.h"
#include "memcmp.h"
#include "strutil.h"

/*
 * Buffer
 */

void
rdb_buffer_init(rdb_buffer_t *z) {
  z->data = NULL;
  z->size = 0;
  z->alloc = 0;
}

void
rdb_buffer_clear(rdb_buffer_t *z) {
  if (z->alloc > 0)
    rdb_free(z->data);

  z->data = NULL;
  z->size = 0;
  z->alloc = 0;
}

void
rdb_buffer_reset(rdb_buffer_t *z) {
  z->size = 0;
}

uint8_t *
rdb_buffer_grow(rdb_buffer_t *z, size_t zn) {
  if (zn > z->alloc) {
    z->data = (uint8_t *)rdb_realloc(z->data, zn);
    z->alloc = zn;
  }

  return z->data;
}

uint8_t *
rdb_buffer_expand(rdb_buffer_t *z, size_t xn) {
  size_t zn = z->size + xn;

  if (zn > z->alloc) {
    size_t alloc = (z->alloc * 3) / 2;

    if (alloc < zn)
      alloc = zn;

    z->data = (uint8_t *)rdb_realloc(z->data, alloc);
    z->alloc = alloc;
  }

  if (z->alloc == 0)
    return NULL;

  return z->data + z->size;
}

uint8_t *
rdb_buffer_resize(rdb_buffer_t *z, size_t zn) {
  rdb_buffer_grow(z, zn);
  z->size = zn;
  return z->data;
}

void
rdb_buffer_set(rdb_buffer_t *z, const uint8_t *xp, size_t xn) {
  rdb_buffer_grow(z, xn);

  if (xn > 0)
    memcpy(z->data, xp, xn);

  z->size = xn;
}

void
rdb_buffer_set_str(rdb_buffer_t *z, const char *xp) {
  rdb_buffer_set(z, (const uint8_t *)xp, strlen(xp));
}

void
rdb_buffer_copy(rdb_buffer_t *z, const rdb_buffer_t *x) {
  rdb_buffer_set(z, x->data, x->size);
}

void
rdb_buffer_swap(rdb_buffer_t *x, rdb_buffer_t *y) {
  rdb_buffer_t t = *x;
  *x = *y;
  *y = t;
}

void
rdb_buffer_roset(rdb_buffer_t *z, const uint8_t *xp, size_t xn) {
  z->data = (uint8_t *)xp;
  z->size = xn;
  z->alloc = 0;
}

void
rdb_buffer_rocopy(rdb_buffer_t *z, const rdb_buffer_t *x) {
  rdb_buffer_roset(z, x->data, x->size);
}

void
rdb_buffer_rwset(rdb_buffer_t *z, uint8_t *zp, size_t zn) {
  z->data = zp;
  z->size = 0;
  z->alloc = zn;
}

uint32_t
rdb_buffer_hash(const rdb_buffer_t *x) {
  return rdb_hash(x->data, x->size, 0);
}

int
rdb_buffer_equal(const rdb_buffer_t *x, const rdb_buffer_t *y) {
  if (x->size != y->size)
    return 0;

  if (x->size == 0)
    return 1;

  return memcmp(x->data, y->data, y->size) == 0;
}

int
rdb_buffer_compare(const rdb_buffer_t *x, const rdb_buffer_t *y) {
  return rdb_memcmp4(x->data, x->size, y->data, y->size);
}

void
rdb_buffer_push(rdb_buffer_t *z, int x) {
  if (z->size == z->alloc)
    rdb_buffer_grow(z, (z->alloc * 3) / 2 + (z->alloc <= 1));

  z->data[z->size++] = x & 0xff;
}

void
rdb_buffer_append(rdb_buffer_t *z, const uint8_t *xp, size_t xn) {
  uint8_t *zp = rdb_buffer_expand(z, xn);

  if (xn > 0)
    memcpy(zp, xp, xn);

  z->size += xn;
}

void
rdb_buffer_concat(rdb_buffer_t *z, const rdb_slice_t *x) {
  rdb_buffer_append(z, x->data, x->size);
}

void
rdb_buffer_string(rdb_buffer_t *z, const char *xp) {
  rdb_buffer_append(z, (const uint8_t *)xp, strlen(xp));
}

void
rdb_buffer_number(rdb_buffer_t *z, uint64_t x) {
  uint8_t *zp = rdb_buffer_expand(z, 21);

  z->size += rdb_encode_int((char *)zp, x, 0);
}

void
rdb_buffer_escape(rdb_buffer_t *z, const rdb_slice_t *x) {
  uint8_t *zp = rdb_buffer_expand(z, x->size * 4 + 1);
  size_t i;

#define nibble(x) ((x) < 10 ? (x) + '0' : (x) - 10 + 'a')

  for (i = 0; i < x->size; i++) {
    int ch = x->data[i];

    if (ch >= ' ' && ch <= '~') {
      *zp++ = ch;
    } else {
      *zp++ = '\\';
      *zp++ = 'x';
      *zp++ = nibble(ch >> 4);
      *zp++ = nibble(ch & 15);
    }
  }

#undef nibble

  z->size = zp - z->data;
}

uint8_t *
rdb_buffer_pad(rdb_buffer_t *z, size_t xn) {
  uint8_t *zp = rdb_buffer_expand(z, xn);

  if (xn > 0)
    memset(zp, 0, xn);

  z->size += xn;

  return zp;
}

void
rdb_buffer_fixed32(rdb_buffer_t *z, uint32_t x) {
  uint8_t *zp = rdb_buffer_expand(z, 4);

  rdb_fixed32_write(zp, x);

  z->size += 4;
}

void
rdb_buffer_fixed64(rdb_buffer_t *z, uint64_t x) {
  uint8_t *zp = rdb_buffer_expand(z, 8);

  rdb_fixed64_write(zp, x);

  z->size += 8;
}

void
rdb_buffer_varint32(rdb_buffer_t *z, uint32_t x) {
  uint8_t *zp = rdb_buffer_expand(z, 5);
  size_t xn = rdb_varint32_write(zp, x) - zp;

  z->size += xn;
}

void
rdb_buffer_varint64(rdb_buffer_t *z, uint64_t x) {
  uint8_t *zp = rdb_buffer_expand(z, 9);
  size_t xn = rdb_varint64_write(zp, x) - zp;

  z->size += xn;
}

size_t
rdb_buffer_size(const rdb_buffer_t *x) {
  return rdb_varint32_size(x->size) + x->size;
}

uint8_t *
rdb_buffer_write(uint8_t *zp, const rdb_buffer_t *x) {
  zp = rdb_varint32_write(zp, x->size);
  zp = rdb_raw_write(zp, x->data, x->size);
  return zp;
}

void
rdb_buffer_export(rdb_buffer_t *z, const rdb_buffer_t *x) {
  uint8_t *zp = rdb_buffer_expand(z, 5 + x->size);
  size_t xn = rdb_buffer_write(zp, x) - zp;

  z->size += xn;
}

int
rdb_buffer_read(rdb_buffer_t *z, const uint8_t **xp, size_t *xn) {
  const uint8_t *zp;
  uint32_t zn;

  if (!rdb_varint32_read(&zn, xp, xn))
    return 0;

  if (!rdb_zraw_read(&zp, zn, xp, xn))
    return 0;

  rdb_buffer_set(z, zp, zn);

  return 1;
}

int
rdb_buffer_slurp(rdb_buffer_t *z, rdb_slice_t *x) {
  return rdb_buffer_read(z, (const uint8_t **)&x->data, &x->size);
}

int
rdb_buffer_import(rdb_buffer_t *z, const rdb_slice_t *x) {
  rdb_slice_t tmp = *x;
  return rdb_buffer_slurp(z, &tmp);
}
