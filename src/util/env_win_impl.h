/*!
 * env_unix_impl.h - unix environment for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <windows.h>

#include "env.h"
#include "internal.h"
#include "slice.h"
#include "status.h"

/*
 * Constants
 */

#define RDB_WRITE_BUFFER 65536
#define RDB_MMAP_LIMIT (sizeof(void *) >= 8 ? 1000 : 0)
#define RDB_WIN32_ERROR rdb_convert_error

/*
 * Types
 */

typedef struct rdb_limiter_s {
  volatile long acquires_allowed;
  int max_acquires;
} rdb_limiter_t;

struct rdb_filelock_s {
  HANDLE handle;
};

/*
 * Globals
 */

static rdb_limiter_t rdb_mmap_limiter = {RDB_MMAP_LIMIT, RDB_MMAP_LIMIT};

/*
 * Limiter
 */

static int
rdb_limiter_acquire(rdb_limiter_t *lim) {
  int old;

  old = InterlockedExchangeAdd(&lim->acquires_allowed, -1);

  if (old > 0)
    return 1;

  old = InterlockedExchangeAdd(&lim->acquires_allowed, 1);

  assert(old < lim->max_acquires);

  (void)old;

  return 0;
}

static void
rdb_limiter_release(rdb_limiter_t *lim) {
  int old = InterlockedExchangeAdd(&lim->acquires_allowed, 1);

  assert(old < lim->max_acquires);

  (void)old;
}

/*
 * File Helpers
 */

static int
rdb_convert_error(DWORD code) {
  if (code == ERROR_FILE_NOT_FOUND || code == ERROR_PATH_NOT_FOUND)
    return RDB_NOTFOUND;

  return RDB_IOERR;
}

static int
rdb_lock_or_unlock(HANDLE handle, int lock) {
  if (lock)
    return LockFile(handle, 0, 0, MAXDWORD, MAXDWORD);

  return UnlockFile(handle, 0, 0, MAXDWORD, MAXDWORD);
}

/*
 * Environment
 */

void
rdb_env_init(void) {
  return;
}

void
rdb_env_clear(void) {
  return;
}

/*
 * Filesystem
 */

int
rdb_path_absolute(char *buf, size_t size, const char *name) {
  DWORD len = GetFullPathNameA(name, size, buf, NULL);

  if (len < 1 || len >= size)
    return 0;

  return 1;
}

int
rdb_file_exists(const char *filename) {
  return GetFileAttributesA(filename) != INVALID_FILE_ATTRIBUTES;
}

int
rdb_get_children(const char *path, char ***out) {
  HANDLE handle = INVALID_HANDLE_VALUE;
  size_t len = strlen(path);
  WIN32_FIND_DATAA fdata;
  char buf[RDB_PATH_MAX];
  char **list = NULL;
  char *name = NULL;
  size_t size = 8;
  size_t i = 0;
  size_t j;

  if (len + 4 > sizeof(buf))
    goto fail;

  if (!(GetFileAttributesA(path) & FILE_ATTRIBUTE_DIRECTORY))
    goto fail;

  list = (char **)malloc(size * sizeof(char *));

  if (list == NULL)
    goto fail;

  memcpy(buf, path, len);

  if (len == 0) {
    buf[len++] = '.';
    buf[len++] = '/';
    buf[len++] = '*';
    buf[len++] = '\0';
  } else if (path[len - 1] == '\\' || path[len - 1] == '/') {
    buf[len++] = '*';
    buf[len++] = '\0';
  } else {
    buf[len++] = '\\';
    buf[len++] = '*';
    buf[len++] = '\0';
  }

  handle = FindFirstFileA(buf, &fdata);

  if (handle == INVALID_HANDLE_VALUE) {
    if (GetLastError() != ERROR_FILE_NOT_FOUND)
      goto fail;

    goto succeed;
  }

  do {
    if (strcmp(fdata.cFileName, ".") == 0
        || strcmp(fdata.cFileName, "..") == 0) {
      continue;
    }

#if 0
    char base[_MAX_FNAME];
    char ext[_MAX_EXT];

    if (_splitpath_s(fdata.cFileName,
                     NULL, 0,
                     NULL, 0,
                     base, sizeof(base),
                     ext, sizeof(ext))) {
      continue;
    }

    len = strlen(base) + strlen(ext);
    name = (char *)malloc(len + 1);

    if (name == NULL)
      goto fail;

    sprintf(name, "%s%s", base, ext);
#endif

    len = strlen(fdata.cFileName);
    name = (char *)malloc(len + 1);

    if (name == NULL)
      goto fail;

    memcpy(name, fdata.cFileName, len + 1);

    if (i == size) {
      size = (size * 3) / 2;
      list = (char **)realloc(list, size * sizeof(char *));

      if (list == NULL)
        goto fail;
    }

    list[i++] = name;
    name = NULL;
  } while (FindNextFileA(handle, &fdata));

  if (GetLastError() != ERROR_NO_MORE_FILES)
    goto fail;

  FindClose(handle);

succeed:
  *out = list;
  *count = i;

  return 1;
fail:
  for (j = 0; j < i; j++)
    rdb_free(list[j]);

  if (list != NULL)
    rdb_free(list);

  if (name != NULL)
    rdb_free(name);

  if (handle != INVALID_HANDLE_VALUE)
    FindClose(handle);

  *out = NULL;
  *count = 0;

  return 0;
}

void
rdb_free_children(char **list, int len) {
  int i;

  for (i = 0; i < len; i++)
    rdb_free(list[i]);

  rdb_free(list);
}

int
rdb_remove_file(const char *filename) {
  if (!DeleteFileA(filename))
    return RDB_WIN32_ERROR(GetLastError());

  return RDB_OK;
}

int
rdb_create_dir(const char *dirname) {
  if (!CreateDirectoryA(dirname, NULL))
    return RDB_WIN32_ERROR(GetLastError());

  return RDB_OK;
}

int
rdb_remove_dir(const char *dirname) {
  if (!RemoveDirectoryA(dirname))
    return RDB_WIN32_ERROR(GetLastError());

  return RDB_OK;
}

int
rdb_get_file_size(const char *filename, uint64_t *size) {
  WIN32_FILE_ATTRIBUTE_DATA attrs;
  ULARGE_INTEGER ul;

  if (!GetFileAttributesExA(filename, GetFileExInfoStandard, &attrs))
    return RDB_WIN32_ERROR(GetLastError());

  ul.HighPart = attrs.nFileSizeHigh;
  ul.LowPart = attrs.nFileSizeLow;

  *size = ul.QuadPart;

  return RDB_OK;
}

int
rdb_rename_file(const char *from, const char *to) {
  DWORD move_error, replace_error;

  if (MoveFileA(from, to))
    return RDB_OK;

  move_error = GetLastError();

  if (ReplaceFileA(to, from, NULL, REPLACEFILE_IGNORE_MERGE_ERRORS, NULL, NULL))
    return RDB_OK;

  replace_error = GetLastError();

  if (replace_error == ERROR_FILE_NOT_FOUND
      || replace_error == ERROR_PATH_NOT_FOUND) {
    return RDB_WIN32_ERROR(move_error);
  }

  return RDB_WIN32_ERROR(replace_error);
}

int
rdb_lock_file(const char *filename, rdb_filelock_t **lock) {
  HANDLE handle = CreateFileA(filename,
                              GENERIC_READ | GENERIC_WRITE,
                              FILE_SHARE_READ,
                              NULL,
                              OPEN_ALWAYS,
                              FILE_ATTRIBUTE_NORMAL,
                              NULL);

  if (handle == INVALID_HANDLE_VALUE)
    return RDB_WIN32_ERROR(GetLastError());

  if (!rdb_lock_or_unlock(handle, 1)) {
    CloseHandle(handle);
    return RDB_IOERR;
  }

  *lock = rdb_malloc(sizeof(rdb_filelock_t));

  (*lock)->handle = handle;

  return RDB_OK;
}

int
rdb_unlock_file(rdb_filelock_t *lock) {
  if (!rdb_lock_or_unlock(lock->handle, 0))
    return RDB_IOERR;

  CloseHandle(lock->handle);

  rdb_free(lock);

  return RDB_OK;
}

/*
 * Readable File
 */

struct rdb_rfile_s {
  char filename[RDB_PATH_MAX];
  HANDLE file;
  rdb_limiter_t *limiter;
  int mapped;
  unsigned char *base;
  size_t length;
};

static void
rdb_rfile_init(rdb_rfile_t *file, const char *filename, HANDLE file) {
  strcpy(file->filename, filename);

  file->handle = file;
  file->limiter = NULL;
  file->mapped = 0;
  file->base = NULL;
  file->length = 0;
}

static void
rdb_mapfile_init(rdb_rfile_t *file,
                 const char *filename,
                 unsigned char *base,
                 size_t length,
                 rdb_limiter_t *limiter) {
  strcpy(file->filename, filename);

  file->handle = INVALID_HANDLE_VALUE;
  file->limiter = limiter;
  file->mapped = 1;
  file->base = base;
  file->length = length;
}

int
rdb_rfile_mapped(rdb_rfile_t *file) {
  return file->mapped;
}

int
rdb_rfile_read(rdb_rfile_t *file,
               rdb_slice_t *result,
               void *buf,
               size_t count) {
  DWORD nread = 0;

  if (!ReadFile(file->handle, buf, count, &nread, NULL))
    return RDB_IOERR;

  rdb_slice_set(result, buf, nread);

  return rc;
}

int
rdb_rfile_skip(rdb_rfile_t *file, uint64_t offset) {
  LARGE_INTEGER dist;

  dist.QuadPart = offset;

  if (!SetFilePointerEx(file->handle, dist, NULL, FILE_CURRENT))
    return RDB_IOERR;

  return RDB_OK;
}

int
rdb_rfile_pread(rdb_rfile_t *file,
                rdb_slice_t *result,
                void *buf,
                size_t count,
                uint64_t offset) {
  DWORD nread = 0;
  OVERLAPPED ol;

  if (file->mapped) {
    if (offset + count > file->length)
      return RDB_INVALID;

    rdb_slice_set(result, file->base + offset, count);

    return RDB_OK;
  }

  memset(&ol, 0, sizeof(ol));

  ol.OffsetHigh = (DWORD)(offset >> 32);
  ol.Offset = (DWORD)offset;

  if (!ReadFile(file->handle, buf, (DWORD)count, &nread, &ol))
    return RDB_IOERR;

  rdb_slice_set(result, buf, nread);

  return RDB_OK;
}

static int
rdb_rfile_close(rdb_rfile_t *file) {
  int rc = RDB_OK;

  if (file->handle != INVALID_HANDLE_VALUE) {
    if (!CloseHandle(file->handle))
      rc = RDB_IOERR;
  }

  if (file->limiter != NULL)
    rdb_limiter_release(file->limiter);

  if (file->mapped)
    UnmapViewOfFile((void *)file->base);

  file->handle = INVALID_HANDLE_VALUE;
  file->limiter = NULL;
  file->mapped = 0;
  file->base = NULL;
  file->length = 0;

  return rc;
}

/*
 * Readable File Instantiation
 */

int
rdb_seqfile_create(const char *filename, rdb_rfile_t **file) {
  HANDLE handle;

  if (strlen(filename) + 1 > RDB_PATH_MAX)
    return RDB_INVALID;

  handle = CreateFileA(filename,
                       GENERIC_READ,
                       FILE_SHARE_READ,
                       NULL,
                       OPEN_EXISTING,
                       FILE_ATTRIBUTE_NORMAL,
                       NULL);

  if (handle == INVALID_HANDLE_VALUE)
    return RDB_WIN32_ERROR(GetLastError());

  *file = rdb_malloc(sizeof(rdb_rfile_t));

  rdb_rfile_init(*file, filename, handle);

  return RDB_OK;
}

int
rdb_randfile_create(const char *filename, rdb_rfile_t **file) {
  HANDLE mapping = NULL;
  LARGE_INTEGER size;
  int rc = RDB_OK;
  HANDLE handle;

  if (strlen(filename) + 1 > RDB_PATH_MAX)
    return RDB_INVALID;

  handle = CreateFileA(filename,
                       GENERIC_READ,
                       FILE_SHARE_READ,
                       NULL,
                       OPEN_EXISTING,
                       FILE_ATTRIBUTE_READONLY,
                       NULL);

  if (handle == INVALID_HANDLE_VALUE)
    return RDB_WIN32_ERROR(GetLastError());

  if (!rdb_limiter_acquire(&rdb_mmap_limiter)) {
    *file = rdb_malloc(sizeof(rdb_rfile_t));

    rdb_rfile_init(*file, filename, handle);

    return RDB_OK;
  }

  if (!GetFileSizeEx(handle, &size))
    rc = RDB_WIN32_ERROR(GetLastError());

  if (rc == RDB_OK && size.QuadPart > (((size_t)-1) / 2))
    rc = RDB_IOERR;

  if (rc == RDB_OK) {
    mapping = CreateFileMappingA(handle, NULL, PAGE_READONLY, 0, 0, NULL);

    if (mapping != NULL) {
      void *base = MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, 0);

      if (base != NULL) {
        *file = rdb_malloc(sizeof(rdb_rfile_t));

        rdb_mapfile_init(*file,
                         filename,
                         base,
                         size.QuadPart,
                         &rdb_mmap_limiter);
      } else {
        rc = RDB_IOERR;
      }
    }
  }

  if (mapping != NULL)
    CloseHandle(mapping);

  CloseHandle(handle);

  if (rc != RDB_OK)
    rdb_limiter_release(&rdb_mmap_limiter);

  return rc;
}

void
rdb_rfile_destroy(rdb_rfile_t *file) {
  rdb_rfile_close(file);
  rdb_free(file);
}

/*
 * Writable File
 */

struct rdb_wfile_s {
  char filename[RDB_PATH_MAX];
  HANDLE handle;
  unsigned char buf[RDB_WRITE_BUFFER];
  size_t pos;
};

static void
rdb_wfile_init(rdb_wfile_t *file, const char *filename, HANDLE file) {
  strcpy(file->filename, filename);

  file->handle = handle;
  file->pos = 0;
}

int
rdb_wfile_close(rdb_wfile_t *file) {
  int rc = rdb_wfile_flush(file);

  if (!CloseHandle(file->handle) && rc == RDB_OK)
    rc = RDB_IOERR;

  file->handle = INVALID_HANDLE_VALUE;

  return rc;
}

static int
rdb_wfile_write(rdb_wfile_t *file, const unsigned char *data, size_t size) {
  DWORD nwrite = 0;

  if (!WriteFile(file->handle, data, (DWORD)size, &nwrite, NULL))
    return RDB_IOERR;

  return RDB_OK;
}

int
rdb_wfile_append(rdb_wfile_t *file, const rdb_slice_t *data) {
  const unsigned char *write_data = data->data;
  size_t write_size = data->size;
  size_t copy_size;
  int rc;

  copy_size = RDB_MIN(write_size, RDB_WRITE_BUFFER - file->pos);

  memcpy(file->buf + file->pos, write_data, copy_size);

  write_data += copy_size;
  write_size -= copy_size;
  file->pos += copy_size;

  if (write_size == 0)
    return RDB_OK;

  if ((rc = rdb_wfile_flush(file)))
    return rc;

  if (write_size < RDB_WRITE_BUFFER) {
    memcpy(file->buf, write_data, write_size);
    file->pos = write_size;
    return RDB_OK;
  }

  return rdb_wfile_write(file, write_data, write_size);
}

int
rdb_wfile_flush(rdb_wfile_t *file) {
  int rc = rdb_wfile_write(file, file->buf, file->pos);
  file->pos = 0;
  return rc;
}

int
rdb_wfile_sync(rdb_wfile_t *file) {
  int rc;

  if ((rc = rdb_wfile_flush(file)))
    return rc;

  if (!FlushFileBuffers(file->handle))
    return RDB_IOERR;

  return RDB_OK;
}

/*
 * Writable File Instantiation
 */

int
rdb_truncfile_create(const char *filename, rdb_wfile_t **file) {
  HANDLE handle;

  if (strlen(filename) + 1 > RDB_PATH_MAX)
    return RDB_INVALID;

  handle = CreateFileA(filename,
                       GENERIC_WRITE,
                       0,
                       NULL,
                       CREATE_ALWAYS,
                       FILE_ATTRIBUTE_NORMAL,
                       NULL);

  if (handle == INVALID_HANDLE_VALUE)
    return RDB_WIN32_ERROR(GetLastError());

  *file = rdb_malloc(sizeof(rdb_wfile_t));

  rdb_wfile_init(*file, filename, handle);

  return RDB_OK;
}

int
rdb_appendfile_create(const char *filename, rdb_wfile_t **file) {
  HANDLE handle;

  if (strlen(filename) + 1 > RDB_PATH_MAX)
    return RDB_INVALID;

  handle = CreateFileA(filename,
                       FILE_APPEND_DATA,
                       0,
                       NULL,
                       OPEN_ALWAYS,
                       FILE_ATTRIBUTE_NORMAL,
                       NULL);

  if (handle == INVALID_HANDLE_VALUE)
    return RDB_WIN32_ERROR(GetLastError());

  *file = rdb_malloc(sizeof(rdb_wfile_t));

  rdb_wfile_init(*file, filename, handle);

  return RDB_OK;
}

void
rdb_wfile_destroy(rdb_wfile_t *file) {
  if (file->handle != INVALID_HANDLE_VALUE)
    CloseHandle(file->handle);

  rdb_free(file);
}

/*
 * Time
 */

uint64_t
rdb_now_usec(void) {
  uint64_t ticks;
  FILETIME ft;

  GetSystemTimeAsFileTime(&ft);

  ticks = ((uint64_t)ft.dwHighDateTime << 32) + ft.dwLowDateTime;

  return ticks / 10;
}

void
rdb_sleep_usec(int64_t usec) {
  if (usec < 0)
    usec = 0;

  Sleep(usec / 1000);
}
