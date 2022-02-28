/*!
 * port.h - ported functions for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_PORT_H
#define RDB_PORT_H

#include <stddef.h>
#include "internal.h"

/*
 * Compat
 */

#if defined(RDB_NEED_WINDOWS_H)
#  include <windows.h>
#  define RDB_HANDLE HANDLE
#  define RDB_CRITICAL_SECTION CRITICAL_SECTION
#elif defined(_WIN32)
typedef void *RDB_HANDLE;
#  pragma pack(push, 8)
typedef struct RDB_RTL_CRITICAL_SECTION {
  void *DebugInfo;
  long LockCount;
  long RecursionCount;
  void *OwningThread;
  void *LockSemaphore;
#ifdef _WIN64
  unsigned __int64 SpinCount;
#else
  unsigned long SpinCount;
#endif
} RDB_CRITICAL_SECTION;
#  pragma pack(pop)
#elif defined(RDB_HAVE_PTHREAD)
#  include <pthread.h>
#endif

/*
 * Types
 */

#if defined(_WIN32)

typedef struct rdb_mutex_s {
  int initialized;
  RDB_HANDLE event;
  RDB_CRITICAL_SECTION handle;
} rdb_mutex_t;

typedef struct rdb_cond_s {
  int waiters;
  RDB_HANDLE signal;
  RDB_HANDLE broadcast;
  RDB_CRITICAL_SECTION lock;
} rdb_cond_t;

typedef struct rdb_thread_s {
  RDB_HANDLE handle;
} rdb_thread_t;

#define RDB_MUTEX_INITIALIZER {0, 0, {0, 0, 0, 0, 0, 0}}

#elif defined(RDB_HAVE_PTHREAD)

typedef struct rdb_mutex_s {
  pthread_mutex_t handle;
} rdb_mutex_t;

typedef struct rdb_cond_s {
  pthread_cond_t handle;
} rdb_cond_t;

typedef struct rdb_thread_s {
  pthread_t handle;
} rdb_thread_t;

#define RDB_MUTEX_INITIALIZER { PTHREAD_MUTEX_INITIALIZER }

#else /* !RDB_HAVE_PTHREAD */

typedef struct rdb_mutex_s {
  void *handle;
} rdb_mutex_t;

typedef struct rdb_cond_s {
  void *handle;
} rdb_cond_t;

typedef struct rdb_thread_s {
  void *handle;
} rdb_thread_t;

#define RDB_MUTEX_INITIALIZER {0}

#endif /* !RDB_HAVE_PTHREAD */

/*
 * Mutex
 */

void
rdb_mutex_init(rdb_mutex_t *mtx);

void
rdb_mutex_destroy(rdb_mutex_t *mtx);

void
rdb_mutex_lock(rdb_mutex_t *mtx);

void
rdb_mutex_unlock(rdb_mutex_t *mtx);

/*
 * Conditional
 */

void
rdb_cond_init(rdb_cond_t *cond);

void
rdb_cond_destroy(rdb_cond_t *cond);

void
rdb_cond_signal(rdb_cond_t *cond);

void
rdb_cond_broadcast(rdb_cond_t *cond);

void
rdb_cond_wait(rdb_cond_t *cond, rdb_mutex_t *mtx);

/*
 * Thread
 */

void
rdb_thread_create(rdb_thread_t *thread, void (*start)(void *), void *arg);

void
rdb_thread_detach(rdb_thread_t *thread);

void
rdb_thread_join(rdb_thread_t *thread);

#endif /* RDB_PORT_H */
