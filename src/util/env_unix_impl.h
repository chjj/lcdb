/*!
 * env_unix_impl.h - unix environment for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#undef HAVE_FCNTL
#undef HAVE_MMAP
#undef HAVE_FDATASYNC
#undef HAVE_PREAD
#undef HAVE_FDOPEN

#if !defined(__EMSCRIPTEN__) && !defined(__wasi__)
#  define HAVE_FCNTL
#  define HAVE_MMAP
#endif

#ifndef __ANDROID__
#  define HAVE_FDATASYNC
#endif

#define HAVE_PREAD
#define HAVE_FDOPEN

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/resource.h>
#ifdef HAVE_MMAP
#include <sys/mman.h>
#endif

#include <dirent.h>
#include <fcntl.h>
#ifdef RDB_PTHREAD
#include <pthread.h>
#endif
#include <unistd.h>

#if !defined(FD_SETSIZE) && !defined(FD_SET)
#  include <sys/select.h>
#endif

#ifndef MAP_FAILED
#  define MAP_FAILED ((void *)-1)
#endif

#ifdef __wasi__
/* lseek(3) is statement expression in wasi-libc. */
#  pragma GCC diagnostic ignored "-Wgnu-statement-expression"
#endif

#include "atomic.h"
#include "env.h"
#include "internal.h"
#include "port.h"
#include "rbt.h"
#include "slice.h"
#include "status.h"
#include "strutil.h"

/*
 * Constants
 */

#define RDB_WRITE_BUFFER 65536
#define RDB_MMAP_LIMIT (sizeof(void *) >= 8 ? 1000 : 0)
#define RDB_POSIX_ERROR(rc) ((rc) == ENOENT ? RDB_NOTFOUND : RDB_IOERR)

/*
 * Types
 */

typedef struct rdb_limiter_s {
  rdb_atomic(int) acquires_allowed;
  int max_acquires;
} rdb_limiter_t;

struct rdb_filelock_s {
  char path[RDB_PATH_MAX];
  int fd;
};

/*
 * Globals
 */

static rdb_limiter_t rdb_fd_limiter = {8192, 8192};
#ifdef HAVE_MMAP
static rdb_limiter_t rdb_mmap_limiter = {RDB_MMAP_LIMIT, RDB_MMAP_LIMIT};
#endif
static rdb_mutex_t file_mutex = RDB_MUTEX_INITIALIZER;
static rb_set_t file_set;

/*
 * Limiter
 */

static void
rdb_limiter_init(rdb_limiter_t *lim, int max_acquires) {
  assert(max_acquires >= 0);

  lim->acquires_allowed = max_acquires;
  lim->max_acquires = max_acquires;
}

static int
rdb_limiter_acquire(rdb_limiter_t *lim) {
  int old;

  old = rdb_atomic_fetch_sub(&lim->acquires_allowed, 1, rdb_order_relaxed);

  if (old > 0)
    return 1;

  old = rdb_atomic_fetch_add(&lim->acquires_allowed, 1, rdb_order_relaxed);

  assert(old < lim->max_acquires);

  (void)old;

  return 0;
}

static void
rdb_limiter_release(rdb_limiter_t *lim) {
  int old = rdb_atomic_fetch_add(&lim->acquires_allowed, 1, rdb_order_relaxed);

  assert(old < lim->max_acquires);

  (void)old;
}

/*
 * Comparator
 */

static int
by_string(rb_val_t x, rb_val_t y, void *arg) {
  (void)arg;
  return strcmp(x.p, y.p);
}

/*
 * Path Helpers
 */

static int
rdb_is_manifest(const char *filename) {
  const char *base = strrchr(filename, '/');

  if (base == NULL)
    base = filename;
  else
    base += 1;

  return rdb_starts_with(base, "MANIFEST");
}

/*
 * File Helpers
 */

static int
rdb_try_open(const char *name, int flags, uint32_t mode) {
  int fd;

#ifdef O_CLOEXEC
  if (flags & O_CREAT)
    fd = open(name, flags | O_CLOEXEC, mode);
  else
    fd = open(name, flags | O_CLOEXEC);

  if (fd >= 0 || errno != EINVAL)
    return fd;
#endif

  if (flags & O_CREAT)
    fd = open(name, flags, mode);
  else
    fd = open(name, flags);

#if defined(HAVE_FCNTL) && defined(FD_CLOEXEC)
  if (fd >= 0) {
    int r = fcntl(fd, F_GETFD);

    if (r != -1)
      fcntl(fd, F_SETFD, r | FD_CLOEXEC);
  }
#endif

  return fd;
}

static int
rdb_open(const char *name, int flags, uint32_t mode) {
  int fd;

  do {
    fd = rdb_try_open(name, flags, mode);
  } while (fd < 0 && errno == EINTR);

  return fd;
}

static int
rdb_sync_fd(int fd, const char *fd_path) {
  int sync_success;

  (void)fd_path;

#ifdef F_FULLFSYNC
#ifdef HAVE_FCNTL
  if (fcntl(fd, F_FULLFSYNC) == 0)
    return RDB_OK;
#endif
#endif

#ifdef HAVE_FDATASYNC
  sync_success = fdatasync(fd) == 0;
#else
  sync_success = fsync(fd) == 0;
#endif

  if (sync_success)
    return RDB_OK;

  return RDB_IOERR;
}

static int
rdb_lock_or_unlock(int fd, int lock) {
#ifdef HAVE_FCNTL
  struct flock info;

  errno = 0;

  memset(&info, 0, sizeof(info));

  info.l_type = (lock ? F_WRLCK : F_UNLCK);
  info.l_whence = SEEK_SET;

  return fcntl(fd, F_SETLK, &info) == 0;
#else
  (void)fd;
  (void)lock;
  return 1;
#endif
}

static int
rdb_max_open_files(void) {
#ifdef __Fuchsia__
  return 50;
#else
  struct rlimit rlim;

  if (getrlimit(RLIMIT_NOFILE, &rlim) != 0)
    return 50;

  if (rlim.rlim_cur == RLIM_INFINITY)
    return INT_MAX / 2;

  return rlim.rlim_cur / 5;
#endif
}

/*
 * Environment
 */

static void
env_init(void) {
  rdb_limiter_init(&rdb_fd_limiter, rdb_max_open_files());
}

static void
rdb_env_init(void) {
#if defined(RDB_PTHREAD)
  static pthread_once_t guard = PTHREAD_ONCE_INIT;
  pthread_once(&guard, env_init);
#else
  static int guard = 0;
  if (guard == 0) {
    env_init();
    guard = 1;
  }
#endif
}

/*
 * Filesystem
 */

int
rdb_path_absolute(char *buf, size_t size, const char *name) {
#if defined(__wasi__)
  size_t len = strlen(name);

  if (name[0] != '/')
    return 0;

  if (len + 1 > size)
    return 0;

  memcpy(buf, name, len + 1);

  return 1;
#else
  char cwd[RDB_PATH_MAX];

  if (name[0] == '/') {
    size_t len = strlen(name);

    if (len + 1 > size)
      return 0;

    memcpy(buf, name, len + 1);

    return 1;
  }

  if (getcwd(cwd, sizeof(cwd)) == NULL)
    return 0;

  cwd[sizeof(cwd) - 1] = '\0';

  return rdb_join(buf, size, cwd, name);
#endif
}

int
rdb_file_exists(const char *filename) {
  return access(filename, F_OK) == 0;
}

int
rdb_get_children(const char *path, char ***out) {
  struct dirent *entry;
  char **list = NULL;
  char *name = NULL;
  DIR *dir = NULL;
  size_t size = 8;
  size_t i = 0;
  size_t j, len;

  list = (char **)malloc(size * sizeof(char *));

  if (list == NULL)
    goto fail;

  dir = opendir(path);

  if (dir == NULL)
    goto fail;

  for (;;) {
    errno = 0;
    entry = readdir(dir);

    if (entry == NULL) {
      if (errno != 0)
        goto fail;
      break;
    }

    if (strcmp(entry->d_name, ".") == 0
        || strcmp(entry->d_name, "..") == 0) {
      continue;
    }

    len = strlen(entry->d_name);
    name = (char *)malloc(len + 1);

    if (name == NULL)
      goto fail;

    memcpy(name, entry->d_name, len + 1);

    if (i == size) {
      size = (size * 3) / 2;
      list = (char **)realloc(list, size * sizeof(char *));

      if (list == NULL)
        goto fail;
    }

    list[i++] = name;
    name = NULL;
  }

  closedir(dir);

  *out = list;

  return i;
fail:
  for (j = 0; j < i; j++)
    rdb_free(list[j]);

  if (list != NULL)
    rdb_free(list);

  if (name != NULL)
    rdb_free(name);

  if (dir != NULL)
    closedir(dir);

  *out = NULL;

  return -1;
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
  if (unlink(filename) != 0)
    return RDB_POSIX_ERROR(errno);

  return RDB_OK;
}

int
rdb_create_dir(const char *dirname) {
  if (mkdir(dirname, 0755) != 0)
    return RDB_POSIX_ERROR(errno);

  return RDB_OK;
}

int
rdb_remove_dir(const char *dirname) {
  if (rmdir(dirname) != 0)
    return RDB_POSIX_ERROR(errno);

  return RDB_OK;
}

int
rdb_get_file_size(const char *filename, uint64_t *size) {
  struct stat st;

  if (stat(filename, &st) != 0)
    return RDB_POSIX_ERROR(errno);

  *size = st.st_size;

  return RDB_OK;
}

int
rdb_rename_file(const char *from, const char *to) {
  if (rename(from, to) != 0)
    return RDB_POSIX_ERROR(errno);

  return RDB_OK;
}

int
rdb_lock_file(const char *filename, rdb_filelock_t **lock) {
  size_t len = strlen(filename);
  int fd;

  if (len + 1 > RDB_PATH_MAX)
    return RDB_INVALID;

  rdb_mutex_lock(&file_mutex);

  if (file_set.root == NULL)
    rb_set_init(&file_set, by_string, NULL);

  if (rb_set_has(&file_set, filename)) {
    rdb_mutex_unlock(&file_mutex);
    return RDB_IOERR;
  }

  fd = rdb_open(filename, O_RDWR | O_CREAT, 0644);

  if (fd < 0) {
    rdb_mutex_unlock(&file_mutex);
    return RDB_POSIX_ERROR(errno);
  }

  if (!rdb_lock_or_unlock(fd, 1)) {
    rdb_mutex_unlock(&file_mutex);
    close(fd);
    return RDB_IOERR;
  }

  *lock = rdb_malloc(sizeof(rdb_filelock_t));

  (*lock)->fd = fd;

  memcpy((*lock)->path, filename, len + 1);

  rb_set_put(&file_set, (*lock)->path);

  rdb_mutex_unlock(&file_mutex);

  return RDB_OK;
}

int
rdb_unlock_file(rdb_filelock_t *lock) {
  int ok = 0;

  rdb_mutex_lock(&file_mutex);

  if (file_set.root && rb_set_has(&file_set, lock->path)) {
    rb_set_del(&file_set, lock->path);
    ok = 1;
  }

  ok &= rdb_lock_or_unlock(lock->fd, 0);

  close(lock->fd);

  rdb_free(lock);

  rdb_mutex_unlock(&file_mutex);

  return ok ? RDB_OK : RDB_IOERR;
}

int
rdb_test_directory(char *result, size_t size) {
  const char *dir = getenv("TEST_TMPDIR");
  char tmp[100];
  size_t len;

  if (dir && dir[0] != '\0') {
    len = strlen(dir);
  } else {
    len = sprintf(tmp, "/tmp/leveldbtest-%d", (int)geteuid());
    dir = tmp;
  }

  if (len + 1 > size)
    return 0;

  memcpy(result, dir, len + 1);

  mkdir(result, 0755);

  return 1;
}

/*
 * Readable File
 */

struct rdb_rfile_s {
  char filename[RDB_PATH_MAX];
  int fd;
  rdb_limiter_t *limiter;
  int mapped;
  unsigned char *base;
  size_t length;
#ifndef HAVE_PREAD
  rdb_mutex_t mutex;
  int has_mutex;
#endif
};

static void
rdb_seqfile_init(rdb_rfile_t *file, const char *filename, int fd) {
  strcpy(file->filename, filename);

  file->fd = fd;
  file->limiter = NULL;
  file->mapped = 0;
  file->base = NULL;
  file->length = 0;
#ifndef HAVE_PREAD
  file->has_mutex = 0;
#endif
}

static void
rdb_randfile_init(rdb_rfile_t *file,
                  const char *filename,
                  int fd,
                  rdb_limiter_t *limiter) {
  int acquired = rdb_limiter_acquire(limiter);

  strcpy(file->filename, filename);

  file->fd = acquired ? fd : -1;
  file->limiter = acquired ? limiter : NULL;
  file->mapped = 0;
  file->base = NULL;
  file->length = 0;

#ifndef HAVE_PREAD
  rdb_mutex_init(&file->mutex);
  file->has_mutex = 1;
#endif

  if (!acquired)
    close(fd);
}

#ifdef HAVE_MMAP
static void
rdb_mapfile_init(rdb_rfile_t *file,
                 const char *filename,
                 unsigned char *base,
                 size_t length,
                 rdb_limiter_t *limiter) {
  strcpy(file->filename, filename);

  file->fd = -1;
  file->limiter = limiter;
  file->mapped = 1;
  file->base = base;
  file->length = length;
#ifndef HAVE_PREAD
  file->has_mutex = 0;
#endif
}
#endif

int
rdb_rfile_mapped(rdb_rfile_t *file) {
  return file->mapped;
}

int
rdb_rfile_read(rdb_rfile_t *file,
               rdb_slice_t *result,
               void *buf,
               size_t count) {
  ssize_t nread;

  do {
    nread = read(file->fd, buf, count);
  } while (nread < 0 && errno == EINTR);

  if (nread < 0)
    return RDB_IOERR;

  rdb_slice_set(result, buf, nread);

  return RDB_OK;
}

int
rdb_rfile_skip(rdb_rfile_t *file, uint64_t offset) {
  if (lseek(file->fd, offset, SEEK_CUR) == -1)
    return RDB_IOERR;

  return RDB_OK;
}

int
rdb_rfile_pread(rdb_rfile_t *file,
                rdb_slice_t *result,
                void *buf,
                size_t count,
                uint64_t offset) {
  int fd = file->fd;
  ssize_t nread;

  if (file->mapped) {
    if (offset + count > file->length)
      return RDB_IOERR;

    rdb_slice_set(result, file->base + offset, count);

    return RDB_OK;
  }

  if (buf == NULL)
    return RDB_INVALID;

  if (file->fd == -1) {
    fd = rdb_open(file->filename, O_RDONLY, 0);

    if (fd < 0)
      return RDB_POSIX_ERROR(errno);
  }

#ifdef HAVE_PREAD
  do {
    nread = pread(fd, buf, count, offset);
  } while (nread < 0 && errno == EINTR);
#else
  rdb_mutex_lock(&file->mutex);

  if ((uint64_t)lseek(fd, offset, SEEK_SET) == offset) {
    do {
      nread = read(fd, buf, count);
    } while (nread < 0 && errno == EINTR);
  } else {
    nread = -1;
  }

  rdb_mutex_unlock(&file->mutex);
#endif

  if (nread >= 0)
    rdb_slice_set(result, buf, nread);

  if (file->fd == -1)
    close(fd);

  return nread < 0 ? RDB_IOERR : RDB_OK;
}

static int
rdb_rfile_close(rdb_rfile_t *file) {
  int rc = RDB_OK;

  if (file->fd != -1) {
    if (close(file->fd) < 0)
      rc = RDB_IOERR;
  }

  if (file->limiter != NULL)
    rdb_limiter_release(file->limiter);

#ifdef HAVE_MMAP
  if (file->mapped)
    munmap((void *)file->base, file->length);
#endif

  file->fd = -1;
  file->limiter = NULL;
  file->mapped = 0;
  file->base = NULL;
  file->length = 0;

#ifndef HAVE_PREAD
  if (file->has_mutex) {
    rdb_mutex_destroy(&file->mutex);
    file->has_mutex = 0;
  }
#endif

  return rc;
}

/*
 * Readable File Instantiation
 */

int
rdb_seqfile_create(const char *filename, rdb_rfile_t **file) {
  int fd;

  if (strlen(filename) + 1 > RDB_PATH_MAX)
    return RDB_INVALID;

  fd = rdb_open(filename, O_RDONLY, 0);

  if (fd < 0)
    return RDB_POSIX_ERROR(errno);

  *file = rdb_malloc(sizeof(rdb_rfile_t));

  rdb_seqfile_init(*file, filename, fd);

  return RDB_OK;
}

int
rdb_randfile_create(const char *filename, rdb_rfile_t **file, int use_mmap) {
#ifdef HAVE_MMAP
  uint64_t size = 0;
  int rc = RDB_OK;
  struct stat st;
#endif
  int fd;

#ifndef HAVE_MMAP
  (void)use_mmap;
#endif

  if (strlen(filename) + 1 > RDB_PATH_MAX)
    return RDB_INVALID;

  fd = rdb_open(filename, O_RDONLY, 0);

  if (fd < 0)
    return RDB_POSIX_ERROR(errno);

#ifdef HAVE_MMAP
  if (!use_mmap || !rdb_limiter_acquire(&rdb_mmap_limiter))
#endif
  {
    *file = rdb_malloc(sizeof(rdb_rfile_t));

    rdb_env_init();
    rdb_randfile_init(*file, filename, fd, &rdb_fd_limiter);

    return RDB_OK;
  }

#ifdef HAVE_MMAP
  if (fstat(fd, &st) != 0)
    rc = RDB_POSIX_ERROR(errno);
  else
    size = st.st_size;

  if (rc == RDB_OK && size > (((size_t)-1) / 2))
    rc = RDB_IOERR;

  if (rc == RDB_OK) {
    void *base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);

    if (base != MAP_FAILED) {
      *file = rdb_malloc(sizeof(rdb_rfile_t));

      rdb_mapfile_init(*file, filename, base, size, &rdb_mmap_limiter);
    } else {
      rc = RDB_IOERR;
    }
  }

  close(fd);

  if (rc != RDB_OK)
    rdb_limiter_release(&rdb_mmap_limiter);

  return rc;
#endif
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
  char dirname[RDB_PATH_MAX];
  int fd, manifest;
  unsigned char buf[RDB_WRITE_BUFFER];
  size_t pos;
};

static void
rdb_wfile_init(rdb_wfile_t *file, const char *filename, int fd) {
  strcpy(file->filename, filename);

  if (!rdb_dirname(file->dirname, RDB_PATH_MAX, filename))
    abort(); /* LCOV_EXCL_LINE */

  file->fd = fd;
  file->manifest = rdb_is_manifest(filename);
  file->pos = 0;
}

int
rdb_wfile_close(rdb_wfile_t *file) {
  int rc = rdb_wfile_flush(file);

  if (close(file->fd) < 0 && rc == RDB_OK)
    rc = RDB_IOERR;

  file->fd = -1;

  return rc;
}

static int
rdb_wfile_write(rdb_wfile_t *file, const unsigned char *data, size_t size) {
  while (size > 0) {
    ssize_t nwrite = write(file->fd, data, size);

    if (nwrite < 0) {
      if (errno == EINTR)
        continue;

      return RDB_IOERR;
    }

    data += nwrite;
    size -= nwrite;
  }

  return RDB_OK;
}

static int
rdb_wfile_sync_dir(rdb_wfile_t *file) {
  int fd, rc;

  if (!file->manifest)
    return RDB_OK;

  fd = rdb_open(file->dirname, O_RDONLY, 0);

  if (fd < 0) {
    rc = RDB_POSIX_ERROR(errno);
  } else {
    rc = rdb_sync_fd(fd, file->dirname);
    close(fd);
  }

  return rc;
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

  if ((rc = rdb_wfile_sync_dir(file)))
    return rc;

  if ((rc = rdb_wfile_flush(file)))
    return rc;

  return rdb_sync_fd(file->fd, file->filename);
}

/*
 * Writable File Instantiation
 */

static int
rdb_wfile_create(const char *filename, int flags, rdb_wfile_t **file) {
  int fd;

  if (strlen(filename) + 1 > RDB_PATH_MAX)
    return RDB_INVALID;

  fd = rdb_open(filename, flags, 0644);

  if (fd < 0)
    return RDB_POSIX_ERROR(errno);

  *file = rdb_malloc(sizeof(rdb_wfile_t));

  rdb_wfile_init(*file, filename, fd);

  return RDB_OK;
}

int
rdb_truncfile_create(const char *filename, rdb_wfile_t **file) {
  int flags = O_TRUNC | O_WRONLY | O_CREAT;
  return rdb_wfile_create(filename, flags, file);
}

int
rdb_appendfile_create(const char *filename, rdb_wfile_t **file) {
  int flags = O_APPEND | O_WRONLY | O_CREAT;
  return rdb_wfile_create(filename, flags, file);
}

void
rdb_wfile_destroy(rdb_wfile_t *file) {
  if (file->fd >= 0)
    close(file->fd);

  rdb_free(file);
}

/*
 * Logging
 */

rdb_logger_t *
rdb_logger_create(FILE *stream);

int
rdb_logger_open(const char *filename, rdb_logger_t **result) {
#ifdef HAVE_FDOPEN
  int fd = rdb_open(filename, O_APPEND | O_WRONLY | O_CREAT, 0644);
  FILE *stream;

  if (fd < 0)
    return RDB_POSIX_ERROR(errno);

  stream = fdopen(fd, "w");
#else
  FILE *stream = fopen(filename, "a");
  int fd = -1;
#endif

  if (stream == NULL) {
    close(fd);
    return RDB_POSIX_ERROR(errno);
  }

  *result = rdb_logger_create(stream);

  return RDB_OK;
}

/*
 * Time
 */

int64_t
rdb_now_usec(void) {
  struct timeval tv;

  if (gettimeofday(&tv, NULL) != 0)
    abort(); /* LCOV_EXCL_LINE */

  return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

void
rdb_sleep_usec(int64_t usec) {
  struct timeval tv;

  memset(&tv, 0, sizeof(tv));

  if (usec <= 0) {
    tv.tv_usec = 1;
  } else {
    tv.tv_sec = usec / 1000000;
    tv.tv_usec = usec % 1000000;
  }

  select(0, NULL, NULL, NULL, &tv);
}
