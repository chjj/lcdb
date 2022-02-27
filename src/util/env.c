/*!
 * env.c - platform-specific functions for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#if defined(_WIN32)
#  include "env_win_impl.h"
#else
#  include "env_unix_impl.h"
#endif

#include "buffer.h"

int
rdb_write_file(const char *fname, const rdb_slice_t *data, int should_sync) {
  rdb_wfile_t *file;
  int rc;

  if ((rc = rdb_truncfile_create(fname, &file)))
    return rc;

  rc = rdb_wfile_append(file, data);

  if (rc == RDB_OK && should_sync)
    rc = rdb_wfile_sync(file);

  if (rc == RDB_OK)
    rc = rdb_wfile_close(file);

  rdb_wfile_destroy(file);

  if (rc != RDB_OK)
    rdb_remove_file(fname);

  return rc;
}

int
rdb_read_file(const char *fname, rdb_buffer_t *data) {
  rdb_rfile_t *file;
  rdb_slice_t chunk;
  char space[8192];
  int rc;

  if ((rc = rdb_seqfile_create(fname, &file)))
    return rc;

  rdb_buffer_reset(data);

  for (;;) {
    rc = rdb_rfile_read(file, &chunk, space, sizeof(space));

    if (rc != RDB_OK)
      break;

    if (chunk.size == 0)
      break;

    rdb_buffer_append(data, chunk.data, chunk.size);
  }

  rdb_rfile_destroy(file);

  return rc;
}
