/*!
 * env_mem_impl.h - memory environment for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(_WIN32)
#  include <windows.h>
#else /* !_WIN32 */
#  include <sys/types.h>
#  include <sys/time.h>
#  if !defined(FD_SETSIZE) && !defined(FD_SET)
#    include <sys/select.h>
#  endif
#endif /* !_WIN32 */

#include "env.h"
#include "internal.h"
#include "port.h"
#include "rbt.h"
#include "slice.h"
#include "status.h"
#include "vector.h"

/*
 * Types
 */

typedef struct rdb_fstate_s rdb_fstate_t;

struct rdb_filelock_s {
  void *unused;
};

/*
 * Globals
 */

static rdb_mutex_t file_mutex = RDB_MUTEX_INITIALIZER;
static rb_map_t file_map;

/*
 * Helpers
 */

static char *
rdb_strdup(const char *xp) {
  size_t xn = strlen(xp);
  return memcpy(rdb_malloc(xn + 1), xp, xn + 1);
}

/*
 * File State
 */

static const int k_block_size = 8 * 1024;

struct rdb_fstate_s {
  rdb_mutex_t refs_mutex;
  rdb_mutex_t blocks_mutex;
  rdb_vector_t blocks;
  uint64_t size;
  int refs;
};

static rdb_fstate_t *
rdb_fstate_create(void) {
  rdb_fstate_t *state = rdb_malloc(sizeof(rdb_fstate_t));

  rdb_mutex_init(&state->refs_mutex);
  rdb_mutex_init(&state->blocks_mutex);
  rdb_vector_init(&state->blocks);

  state->size = 0;
  state->refs = 0;

  return state;
}

static void
rdb_fstate_truncate(rdb_fstate_t *state) {
  size_t i;

  rdb_mutex_lock(&state->blocks_mutex);

  for (i = 0; i < state->blocks.length; i++)
    rdb_free(state->blocks.items[i]);

  state->blocks.length = 0;
  state->size = 0;

  rdb_mutex_unlock(&state->blocks_mutex);
}

static void
rdb_fstate_destroy(rdb_fstate_t *state) {
  rdb_fstate_truncate(state);
  rdb_mutex_destroy(&state->refs_mutex);
  rdb_mutex_destroy(&state->blocks_mutex);
  rdb_vector_clear(&state->blocks);
  rdb_free(state);
}

static rdb_fstate_t *
rdb_fstate_ref(rdb_fstate_t *state) {
  rdb_mutex_lock(&state->refs_mutex);
  ++state->refs;
  rdb_mutex_unlock(&state->refs_mutex);
  return state;
}

static void
rdb_fstate_unref(rdb_fstate_t *state) {
  int do_delete = 0;

  rdb_mutex_lock(&state->refs_mutex);

  --state->refs;

  assert(state->refs >= 0);

  if (state->refs <= 0)
    do_delete = 1;

  rdb_mutex_unlock(&state->refs_mutex);

  if (do_delete)
    rdb_fstate_destroy(state);
}

static uint64_t
rdb_fstate_size(const rdb_fstate_t *state) {
  rdb_mutex_t *mutex = (rdb_mutex_t *)&state->blocks_mutex;
  uint64_t size;

  rdb_mutex_lock(mutex);

  size = state->size;

  rdb_mutex_unlock(mutex);

  return size;
}

static int
rdb_fstate_pread(const rdb_fstate_t *state,
                 rdb_slice_t *result,
                 void *buf,
                 size_t count,
                 uint64_t offset) {
  rdb_mutex_t *mutex = (rdb_mutex_t *)&state->blocks_mutex;
  size_t block, block_offset, bytes_to_copy;
  uint64_t available;
  unsigned char *dst;

  rdb_mutex_lock(mutex);

  if (offset > state->size) {
    rdb_mutex_unlock(mutex);
    return RDB_IOERR; /* "Offset greater than file size." */
  }

  available = state->size - offset;

  if (count > available)
    count = (size_t)available;

  if (count == 0) {
    rdb_mutex_unlock(mutex);
    *result = rdb_slice(0, 0);
    return RDB_OK;
  }

  assert(offset / k_block_size <= (size_t)-1);

  block = (size_t)(offset / k_block_size);
  block_offset = offset % k_block_size;
  bytes_to_copy = count;
  dst = buf;

  while (bytes_to_copy > 0) {
    size_t avail = k_block_size - block_offset;

    if (avail > bytes_to_copy)
      avail = bytes_to_copy;

    memcpy(dst, (char *)state->blocks.items[block] + block_offset, avail);

    bytes_to_copy -= avail;
    dst += avail;
    block++;
    block_offset = 0;
  }

  rdb_mutex_unlock(mutex);

  *result = rdb_slice(buf, count);

  return RDB_OK;
}

static int
rdb_fstate_append(rdb_fstate_t *state, const rdb_slice_t *data) {
  unsigned const char *src = data->data;
  size_t src_len = data->size;

  rdb_mutex_lock(&state->blocks_mutex);

  while (src_len > 0) {
    size_t offset = state->size % k_block_size;
    size_t avail;

    if (offset != 0) {
      /* There is some room in the last block. */
      avail = k_block_size - offset;
    } else {
      /* No room in the last block; push new one. */
      rdb_vector_push(&state->blocks, rdb_malloc(k_block_size));
      avail = k_block_size;
    }

    if (avail > src_len)
      avail = src_len;

    memcpy((char *)rdb_vector_top(&state->blocks) + offset, src, avail);

    src_len -= avail;
    src += avail;
    state->size += avail;
  }

  rdb_mutex_unlock(&state->blocks_mutex);

  return RDB_OK;
}

/*
 * Environment
 */

static int
by_string(rb_val_t x, rb_val_t y, void *arg) {
  (void)arg;
  return strcmp(x.p, y.p);
}

static void
cleanup_node(rb_node_t *node) {
  rdb_free(node->key.p);
  rdb_fstate_unref(node->value.p);
}

void
rdb_env_init(void) {
  rdb_mutex_lock(&file_mutex);

  if (file_map.root == NULL)
    rb_map_init(&file_map, by_string, NULL);

  rdb_mutex_unlock(&file_mutex);
}

void
rdb_env_clear(void) {
  rdb_mutex_lock(&file_mutex);
  rb_map_clear(&file_map, cleanup_node);
  rdb_mutex_unlock(&file_mutex);
}

/*
 * Filesystem
 */

int
rdb_path_absolute(char *buf, size_t size, const char *name) {
  size_t len = strlen(name);

  if (len == 0 || len + 1 > size)
    return 0;

  memcpy(buf, name, len + 1);

#ifdef _WIN32
  {
    size_t i;

    for (i = 0; i < len; i++) {
      if (buf[i] == '/')
        buf[i] = '\\';
    }
  }
#endif

  return 1;
}

int
rdb_file_exists(const char *filename) {
  int result;
  rdb_mutex_lock(&file_mutex);
  result = rb_map_has(&file_map, filename);
  rdb_mutex_unlock(&file_mutex);
  return result;
}

int
rdb_get_children(const char *path, char ***out) {
  size_t plen = strlen(path);
  rdb_vector_t names;
  void *key;

#if defined(_WIN32)
  while (plen > 0 && (path[plen - 1] == '/' || path[plen - 1] == '\\')
    plen -= 1;
#else
  while (plen > 0 && path[plen - 1] == '/')
    plen -= 1;
#endif

  rdb_vector_init(&names);

  rdb_mutex_lock(&file_mutex);

  rb_map_keys(&file_map, key) {
    const char *name = key;
    size_t nlen = strlen(name);

#if defined(_WIN32)
    if (nlen > plen + 1 && (name[plen] == '/' || name[plen] == '\\'))
#else
    if (nlen > plen + 1 && name[plen] == '/')
#endif
    {
      if (memcmp(name, path, plen) == 0)
        rdb_vector_push(&names, rdb_strdup(name + plen + 1));
    }
  }

  rdb_mutex_unlock(&file_mutex);

  *out = (char **)names.items;

  return names.length;
}

void
rdb_free_children(char **list, int len) {
  int i;

  for (i = 0; i < len; i++)
    rdb_free(list[i]);

  rdb_free(list);
}

static int
rdb_delete_file(const char *filename) {
  rb_node_t *node = rb_map_del(&file_map, filename);

  if (node != NULL) {
    rdb_free(node->key.p);
    rdb_fstate_unref(node->value.p);
    rb_node_destroy(node);
    return 1;
  }

  return 0;
}

int
rdb_remove_file(const char *filename) {
  int result;

  rdb_mutex_lock(&file_mutex);

  result = rdb_delete_file(filename);

  rdb_mutex_unlock(&file_mutex);

  return result ? RDB_OK : RDB_IOERR; /* "File not found" */
}

int
rdb_create_dir(const char *dirname) {
  (void)dirname;
  return RDB_OK;
}

int
rdb_remove_dir(const char *dirname) {
  (void)dirname;
  return RDB_OK;
}

int
rdb_get_file_size(const char *filename, uint64_t *size) {
  rdb_fstate_t *state;

  rdb_mutex_lock(&file_mutex);

  state = rb_map_get(&file_map, filename);

  if (state != NULL)
    *size = rdb_fstate_size(state);

  rdb_mutex_unlock(&file_mutex);

  return state ? RDB_OK : RDB_IOERR; /* "File not found" */
}

int
rdb_rename_file(const char *from, const char *to) {
  rb_node_t *node;

  rdb_mutex_lock(&file_mutex);

  node = rb_map_del(&file_map, from);

  if (node != NULL) {
    rdb_delete_file(to);
    rb_map_put(&file_map, rdb_strdup(to), node->value.p);
    rdb_free(node->key.p);
    rb_node_destroy(node);
  }

  rdb_mutex_unlock(&file_mutex);

  return node ? RDB_OK : RDB_IOERR; /* "File not found" */
}

int
rdb_lock_file(const char *filename, rdb_filelock_t **lock) {
  (void)filename;
  *lock = rdb_malloc(sizeof(rdb_filelock_t));
  return RDB_OK;
}

int
rdb_unlock_file(rdb_filelock_t *lock) {
  rdb_free(lock);
  return RDB_OK;
}

/*
 * Readable File
 */

struct rdb_rfile_s {
  rdb_fstate_t *state;
  size_t pos;
};

static void
rdb_rfile_init(rdb_rfile_t *file, rdb_fstate_t *state) {
  file->state = rdb_fstate_ref(state);
  file->pos = 0;
}

int
rdb_rfile_mapped(rdb_rfile_t *file) {
  (void)file;
  return 0;
}

int
rdb_rfile_read(rdb_rfile_t *file,
               rdb_slice_t *result,
               void *buf,
               size_t count) {
  int rc = rdb_fstate_pread(file->state, result, buf, count, file->pos);

  if (rc == RDB_OK)
    file->pos += result->size;

  return rc;
}

int
rdb_rfile_skip(rdb_rfile_t *file, uint64_t offset) {
  uint64_t available;

  if (file->pos > rdb_fstate_size(file->state))
    return RDB_IOERR;

  available = rdb_fstate_size(file->state) - file->pos;

  if (offset > available)
    offset = available;

  file->pos += offset;

  return RDB_OK;
}

int
rdb_rfile_pread(rdb_rfile_t *file,
                rdb_slice_t *result,
                void *buf,
                size_t count,
                uint64_t offset) {
  return rdb_fstate_pread(file->state, result, buf, count, offset);
}

/*
 * Readable File Instantiation
 */

int
rdb_seqfile_create(const char *filename, rdb_rfile_t **file) {
  rdb_fstate_t *state;

  rdb_mutex_lock(&file_mutex);

  state = rb_map_get(&file_map, filename);

  if (state != NULL) {
    *file = rdb_malloc(sizeof(rdb_rfile_t));

    rdb_rfile_init(*file, state);
  }

  rdb_mutex_unlock(&file_mutex);

  return state ? RDB_OK : RDB_IOERR; /* "File not found" */
}

int
rdb_randfile_create(const char *filename, rdb_rfile_t **file) {
  return rdb_seqfile_create(filename, file);
}

void
rdb_rfile_destroy(rdb_rfile_t *file) {
  rdb_fstate_unref(file->state);
  rdb_free(file);
}

/*
 * Writable File
 */

struct rdb_wfile_s {
  rdb_fstate_t *state;
};

static void
rdb_wfile_init(rdb_wfile_t *file, rdb_fstate_t *state) {
  file->state = rdb_fstate_ref(state);
}

int
rdb_wfile_close(rdb_wfile_t *file) {
  (void)file;
  return RDB_OK;
}

int
rdb_wfile_append(rdb_wfile_t *file, const rdb_slice_t *data) {
  return rdb_fstate_append(file->state, data);
}

int
rdb_wfile_flush(rdb_wfile_t *file) {
  (void)file;
  return RDB_OK;
}

int
rdb_wfile_sync(rdb_wfile_t *file) {
  (void)file;
  return RDB_OK;
}

/*
 * Writable File Instantiation
 */

int
rdb_truncfile_create(const char *filename, rdb_wfile_t **file) {
  rdb_fstate_t *state;

  rdb_mutex_lock(&file_mutex);

  state = rb_map_get(&file_map, filename);

  if (state == NULL) {
    state = rdb_fstate_ref(rdb_fstate_create());
    rb_map_put(&file_map, rdb_strdup(filename), state);
  } else {
    rdb_fstate_truncate(state);
  }

  *file = rdb_malloc(sizeof(rdb_wfile_t));

  rdb_wfile_init(*file, state);

  rdb_mutex_unlock(&file_mutex);

  return RDB_OK;
}

int
rdb_appendfile_create(const char *filename, rdb_wfile_t **file) {
  rdb_fstate_t *state;

  rdb_mutex_lock(&file_mutex);

  state = rb_map_get(&file_map, filename);

  if (state == NULL) {
    state = rdb_fstate_ref(rdb_fstate_create());
    rb_map_put(&file_map, rdb_strdup(filename), state);
  }

  *file = rdb_malloc(sizeof(rdb_wfile_t));

  rdb_wfile_init(*file, state);

  rdb_mutex_unlock(&file_mutex);

  return RDB_OK;
}

void
rdb_wfile_destroy(rdb_wfile_t *file) {
  rdb_fstate_unref(file->state);
  rdb_free(file);
}

/*
 * Time
 */

int64_t
rdb_now_usec(void) {
#if defined(_WIN32)
  uint64_t ticks;
  FILETIME ft;

  GetSystemTimeAsFileTime(&ft);

  ticks = ((uint64_t)ft.dwHighDateTime << 32) + ft.dwLowDateTime;

  return ticks / 10;
#else /* !_WIN32 */
  struct timeval tv;

  if (gettimeofday(&tv, NULL) != 0)
    abort(); /* LCOV_EXCL_LINE */

  return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
#endif /* !_WIN32 */
}

void
rdb_sleep_usec(int64_t usec) {
#if defined(_WIN32)
  if (usec < 0)
    usec = 0;

  Sleep(usec / 1000);
#else /* !_WIN32 */
  struct timeval tv;

  memset(&tv, 0, sizeof(tv));

  if (usec <= 0) {
    tv.tv_usec = 1;
  } else {
    tv.tv_sec = usec / 1000000;
    tv.tv_usec = usec % 1000000;
  }

  select(0, NULL, NULL, NULL, &tv);
#endif /* !_WIN32 */
}
