/*!
 * filename.c - filename utilities for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "util/env.h"
#include "util/slice.h"
#include "util/status.h"

#include "filename.h"

/*
 * Helpers
 */

int
starts_with(const char *xp, const char *yp) {
  while (*xp && *xp == *yp) {
    xp++;
    yp++;
  }

  return *yp == 0;
}

static int
size_int(uint64_t x) {
  int n = 0;

  do {
    n++;
    x /= 10;
  } while (x != 0);

  return n;
}

int
encode_int(char *zp, uint64_t x, int pad) {
  int n = size_int(x);
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
decode_int(uint64_t *z, const char **xp) {
  const char *sp = *xp;
  uint64_t x = 0;
  int n = 0;

  while (*sp) {
    int ch = *sp;

    if (ch < '0' || ch > '9')
      break;

    if (++n > 20)
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

static int
make_filename(char *buf,
              size_t size,
              const char *prefix,
              uint64_t num,
              const char *ext) {
  char tmp[128];
  char id[32];

  encode_int(id, num, 6);

  sprintf(tmp, "%s.%s", id, ext);

  return rdb_path_join(buf, size, prefix, tmp);
}

/*
 * Filename
 */

int
rdb_log_filename(char *buf, size_t size, const char *prefix, uint64_t num) {
  assert(num > 0);
  return make_filename(buf, size, prefix, num, "log");
}

int
rdb_table_filename(char *buf, size_t size, const char *prefix, uint64_t num) {
  assert(num > 0);
  return make_filename(buf, size, prefix, num, "ldb");
}

int
rdb_sstable_filename(char *buf, size_t size, const char *prefix, uint64_t num) {
  assert(num > 0);
  return make_filename(buf, size, prefix, num, "sst");
}

int
rdb_desc_filename(char *buf, size_t size, const char *prefix, uint64_t num) {
  char tmp[128];
  char id[32];

  assert(num > 0);

  encode_int(id, num, 6);

  sprintf(tmp, "MANIFEST-%s", id);

  return rdb_path_join(buf, size, prefix, tmp);
}

int
rdb_current_filename(char *buf, size_t size, const char *prefix) {
  return rdb_path_join(buf, size, prefix, "CURRENT");
}

int
rdb_lock_filename(char *buf, size_t size, const char *prefix) {
  return rdb_path_join(buf, size, prefix, "LOCK");
}

int
rdb_temp_filename(char *buf, size_t size, const char *prefix, uint64_t num) {
  assert(num > 0);
  return make_filename(buf, size, prefix, num, "dbtmp");
}

int
rdb_info_filename(char *buf, size_t size, const char *prefix) {
  return rdb_path_join(buf, size, prefix, "LOG");
}

int
rdb_oldinfo_filename(char *buf, size_t size, const char *prefix) {
  return rdb_path_join(buf, size, prefix, "LOG.old");
}

/* Owned filenames have the form:
 *    dbname/CURRENT
 *    dbname/LOCK
 *    dbname/LOG
 *    dbname/LOG.old
 *    dbname/MANIFEST-[0-9]+
 *    dbname/[0-9]+.(log|sst|ldb)
 */
int
rdb_parse_filename(rdb_filetype_t *type, uint64_t *num, const char *name) {
  uint64_t x;

  if (strcmp(name, "CURRENT") == 0) {
    *type = RDB_FILE_CURRENT;
    *num = 0;
  } else if (strcmp(name, "LOCK") == 0) {
    *type = RDB_FILE_LOCK;
    *num = 0;
  } else if (strcmp(name, "LOG") == 0 || strcmp(name, "LOG.old") == 0) {
    *type = RDB_FILE_INFO;
    *num = 0;
  } else if (starts_with(name, "MANIFEST-")) {
    name += 9;

    if (!decode_int(&x, &name))
      return 0;

    if (*name != '\0')
      return 0;

    *type = RDB_FILE_DESC;
    *num = x;
  } else if (decode_int(&x, &name)) {
    if (strcmp(name, ".log") == 0)
      *type = RDB_FILE_LOG;
    else if (strcmp(name, ".sst") == 0 || strcmp(name, ".ldb") == 0)
      *type = RDB_FILE_TABLE;
    else if (strcmp(name, ".dbtmp") == 0)
      *type = RDB_FILE_TEMP;
    else
      return 0;

    *num = x;
  } else {
    return 0;
  }

  return 1;
}

int
rdb_set_current_file(const char *prefix, uint64_t desc_number) {
  rdb_slice_t data;
  char cur[1024];
  char tmp[1024];
  char man[128];
  char id[32];
  int rc;

  assert(desc_number > 0);

  if (!rdb_temp_filename(tmp, 1024, prefix, desc_number))
    return RDB_INVALID;

  if (!rdb_current_filename(cur, 1024, prefix))
    return RDB_INVALID;

  encode_int(id, desc_number, 6);

  sprintf(man, "MANIFEST-%s\n", id);

  rdb_slice_set_str(&data, man);

  rc = rdb_write_file(tmp, &data, 1);

  if (rc == RDB_OK)
    rc = rdb_rename_file(tmp, cur);

  if (rc != RDB_OK)
    rdb_remove_file(tmp);

  return rc;
}
