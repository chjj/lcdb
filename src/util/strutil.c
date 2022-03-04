/*!
 * strutil.c - string utilities for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include "strutil.h"

/*
 * String
 */

int
rdb_starts_with(const char *xp, const char *yp) {
  while (*xp && *xp == *yp) {
    xp++;
    yp++;
  }

  return *yp == 0;
}

int
rdb_size_int(uint64_t x) {
  int n = 0;

  do {
    n++;
    x /= 10;
  } while (x != 0);

  return n;
}

int
rdb_encode_int(char *zp, uint64_t x, int pad) {
  int n = rdb_size_int(x);
  int i;

  if (n < pad)
    n = pad;

  zp[n] = '\0';

  for (i = n - 1; i >= 0; i--) {
    zp[i] = '0' + (int)(x % 10);
    x /= 10;
  }

  return n;
}

int
rdb_decode_int(uint64_t *z, const char **xp) {
  const int last = '0' + (int)(UINT64_MAX % 10);
  const uint64_t limit = UINT64_MAX / 10;
  const char *sp = *xp;
  uint64_t x = 0;
  int n = 0;

  while (*sp) {
    int ch = *sp;

    if (ch < '0' || ch > '9')
      break;

    if (++n > 20)
      return 0;

    if (x > limit || (x == limit && ch > last))
      return 0;

    x *= 10;
    x += (ch - '0');

    sp++;
  }

  if (n == 0)
    return 0;

  *xp = sp;
  *z = x;

  return 1;
}

char *
rdb_basename(const char *fname) {
#if defined(_WIN32)
  size_t len = strlen(fname);

  while (len > 0) {
    if (fname[len - 1] == '/' || fname[len - 1] == '\\')
      break;

    len--;
  }

  return (char *)fname + len;
#else
  const char *base = strrchr(fname, '/');

  if (base == NULL)
    base = fname;
  else
    base += 1;

  return (char *)base;
#endif
}

int
rdb_dirname(char *buf, size_t size, const char *fname) {
  const char *base = rdb_basename(fname);
  size_t pos;

  if (base == fname) {
    if (size < 2)
      return 0;

    *buf++ = '.';
    *buf++ = '\0';
  } else {
    pos = (base - 1) - fname;

    if (pos == 0)
      pos = 1;

    if (pos + 1 > size)
      return 0;

    memcpy(buf, fname, pos + 1);

    buf[pos] = '\0';
  }

  return 1;
}

int
rdb_join(char *zp, size_t zn, const char *xp, const char *yp) {
  if (strlen(xp) + strlen(yp) + 2 > zn)
    return 0;

  while (*xp)
    *zp++ = *xp++;

#if defined(_WIN32)
  *zp++ = '\\';
#else
  *zp++ = '/';
#endif

  while (*yp)
    *zp++ = *yp++;

  *zp = '\0';

  return 1;
}
