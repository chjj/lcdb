/*!
 * tar.c - tar implementation for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "env.h"
#include "internal.h"
#include "slice.h"
#include "status.h"
#include "strutil.h"
#include "tar.h"

/*
 * Constants
 */

#define SCRATCH_SIZE (3 << 20)

/*
 * Helpers
 */

static unsigned int
ldb_tar_checksum(const char *hdr) {
  const unsigned char *blk = (const unsigned char *)hdr;
  unsigned int chk = 256;
  int i;

  for (i = 0; i < 148; i++)
    chk += blk[i];

  for (i = 156; i < 512; i++)
    chk += blk[i];

  return chk;
}

/*
 * TarWriter
 */

void
ldb_tar_init(ldb_tar_t *tar, ldb_wfile_t *dst) {
  tar->dst = dst;
  tar->mtime = time(NULL);
  tar->scratch = ldb_malloc(SCRATCH_SIZE);
}

void
ldb_tar_clear(ldb_tar_t *tar) {
  ldb_free(tar->scratch);
}

static int
ldb_tar_write_header(ldb_tar_t *tar, const char *name, uint64_t size) {
  ldb_slice_t chunk;
  char hdr[512];

  if (strlen(name) >= 100)
    return LDB_INVALID;

  if (size > UINT_MAX - 511)
    return LDB_INVALID;

  memset(hdr, 0, sizeof(hdr));

  /* V7 header:
   *
   *   struct v7_header {
   *     char name[100];
   *     char mode[8];
   *     char uid[8];
   *     char gid[8];
   *     char size[12];
   *     char mtime[12];
   *     char chksum[8];
   *     char linkflag;
   *     char linkname[100];
   *   }
   */
  sprintf(hdr +   0, "%s", name); /* name */
  sprintf(hdr + 100, "%07o", 0644u); /* mode */
  sprintf(hdr + 108, "%07o", 0u); /* uid */
  sprintf(hdr + 116, "%07o", 0u); /* gid */
  sprintf(hdr + 124, "%011o", (unsigned int)size); /* size */
  sprintf(hdr + 136, "%011o", tar->mtime); /* mtime */
  sprintf(hdr + 148, "%8s", ""); /* chksum */
  sprintf(hdr + 156, "%c", '0'); /* linkflag */
  sprintf(hdr + 157, "%s", ""); /* linkname */

  /* UStar header:
   *
   *   struct ustar_header {
   *     struct v7_header;
   *     char magic[6];
   *     char version[2];
   *     char uname[32];
   *     char gname[32];
   *     char devmajor[8];
   *     char devminor[8];
   *     char prefix[155];
   *   }
   */
  sprintf(hdr + 257, "ustar"); /* magic */
  sprintf(hdr + 263, "00"); /* version */
  sprintf(hdr + 265, "%s", "root"); /* uname */
  sprintf(hdr + 297, "%s", "root"); /* gname */
  sprintf(hdr + 329, "%07o", 0u); /* devmajor */
  sprintf(hdr + 337, "%07o", 0u); /* devminor */
  sprintf(hdr + 345, "%s", ""); /* prefix */

  /* Compute checksum. */
  sprintf(hdr + 148, "%06o", ldb_tar_checksum(hdr));

  assert(hdr[155] == ' ');

  ldb_slice_set(&chunk, (unsigned char *)hdr, sizeof(hdr));

  return ldb_wfile_append(tar->dst, &chunk);
}

static int
ldb_tar_write_padding(ldb_tar_t *tar, size_t size) {
  unsigned char blocks[1024];
  ldb_slice_t chunk;

  assert(size <= sizeof(blocks));

  memset(blocks, 0, size);

  ldb_slice_set(&chunk, blocks, size);

  return ldb_wfile_append(tar->dst, &chunk);
}

int
ldb_tar_append(ldb_tar_t *tar, const char *name, const char *path) {
  uint64_t total = 0;
  ldb_slice_t chunk;
  ldb_rfile_t *src;
  uint64_t size;
  int rc;

  rc = ldb_file_size(path, &size);

  if (rc != LDB_OK)
    return rc;

  rc = ldb_tar_write_header(tar, name, size);

  if (rc != LDB_OK)
    return rc;

  rc = ldb_seqfile_create(path, &src);

  if (rc != LDB_OK)
    return rc;

  for (;;) {
    rc = ldb_rfile_read(src, &chunk, tar->scratch, SCRATCH_SIZE);

    if (rc != LDB_OK)
      break;

    if (chunk.size == 0)
      break;

    rc = ldb_wfile_append(tar->dst, &chunk);

    if (rc != LDB_OK)
      break;

    total += chunk.size;
  }

  ldb_rfile_destroy(src);

  if (rc != LDB_OK)
    return rc;

  if (total != size)
    return LDB_IOERR;

  if (size % 512)
    rc = ldb_tar_write_padding(tar, 512 - (size % 512));

  return rc;
}

int
ldb_tar_finish(ldb_tar_t *tar) {
  return ldb_tar_write_padding(tar, 1024);
}

/*
 * TarReader
 */

static int
ldb_tar_parse_header(unsigned int *size, int *flag, char *hdr) {
  unsigned int chk = ldb_tar_checksum(hdr);
  unsigned int num;

  *size = 0;
  *flag = '0';

  hdr[511] = '\0';

  if (sscanf(hdr + 148, "%o", &num) != 1)
    return 0;

  if (chk != num)
    return 0;

  if (strlen(hdr) >= 100)
    return 0;

  if (hdr[124] != '\0') {
    if (sscanf(hdr + 124, "%o", size) != 1)
      return 0;

    if (*size > UINT_MAX - 511)
      return 0;
  }

  if (hdr[156] != '\0')
    *flag = hdr[156];

  return 1;
}

int
ldb_tar_extract(const char *from,
                const char *to,
                int should_sync,
                const char *required) {
  char *scratch, *hdr, *name;
  char path[LDB_PATH_MAX];
  unsigned int size;
  ldb_slice_t chunk;
  ldb_rfile_t *src;
  ldb_wfile_t *dst;
  size_t len, max;
  int rc, flag;

  rc = ldb_seqfile_create(from, &src);

  if (rc != LDB_OK)
    return rc;

  ldb_remove_dir(to);

  rc = ldb_create_dir(to);

  if (rc != LDB_OK) {
    ldb_rfile_destroy(src);
    return rc;
  }

  scratch = ldb_malloc(SCRATCH_SIZE);

  while (rc == LDB_OK) {
    rc = ldb_rfile_read(src, &chunk, scratch, 512);

    if (rc != LDB_OK)
      break;

    if (chunk.size == 0)
      break;

    if (chunk.size != 512) {
      rc = LDB_IOERR;
      break;
    }

    if (chunk.data[0] == 0)
      continue;

    hdr = (char *)chunk.data;

    if (!ldb_tar_parse_header(&size, &flag, hdr)) {
      rc = LDB_IOERR;
      break;
    }

    if (flag != '0') {
      size = (size + 511u) & ~511u;
      rc = ldb_rfile_skip(src, size);
      continue;
    }

    name = ldb_basename(hdr);

    if (required != NULL && strcmp(name, required) == 0)
      required = NULL;

    if (!ldb_join(path, sizeof(path), to, name)) {
      rc = LDB_INVALID;
      break;
    }

    rc = ldb_truncfile_create(path, &dst);

    if (rc != LDB_OK)
      break;

    len = size;

    while (len > 0) {
      max = LDB_MIN(len, SCRATCH_SIZE);
      rc = ldb_rfile_read(src, &chunk, scratch, max);

      if (rc != LDB_OK)
        break;

      if (chunk.size == 0)
        break;

      rc = ldb_wfile_append(dst, &chunk);

      if (rc != LDB_OK)
        break;

      len -= chunk.size;
    }

    if (rc == LDB_OK && len > 0)
      rc = LDB_IOERR;

    if (rc == LDB_OK && should_sync)
      rc = ldb_wfile_sync(dst);

    if (rc == LDB_OK)
      rc = ldb_wfile_close(dst);

    ldb_wfile_destroy(dst);

    if (rc != LDB_OK)
      ldb_remove_file(path);

    if (rc == LDB_OK && (size % 512) != 0)
      rc = ldb_rfile_skip(src, 512 - (size % 512));
  }

  ldb_free(scratch);
  ldb_rfile_destroy(src);

  if (rc == LDB_OK && required != NULL)
    rc = LDB_INVALID;

  if (rc == LDB_OK && should_sync)
    rc = ldb_sync_dir(to);

  if (rc != LDB_OK) {
    char **files = NULL;
    int nfiles = ldb_get_children(to, &files);
    int i;

    for (i = 0; i < nfiles; i++) {
      if (ldb_join(path, sizeof(path), to, files[i]))
        ldb_remove_file(path);
    }

    ldb_free_children(files, nfiles);
    ldb_remove_dir(to);
  }

  return rc;
}
