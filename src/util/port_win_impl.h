/*!
 * port_win_impl.h - windows port for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 *
 * Parts of this software are based on libuv/libuv:
 *   Copyright (c) 2015-2020, libuv project contributors (MIT License).
 *   https://github.com/libuv/libuv
 */

#define RDB_NEED_WINDOWS_H

#include <assert.h>
#include <stdlib.h>
#include <string.h>
/* #include <windows.h> */
#include "internal.h"
#include "port.h"

/*
 * Types
 */

typedef struct rdb_args_s {
  void (*start)(void *);
  void *arg;
} rdb_args_t;

/*
 * Mutex
 */

static void
rdb_mutex_tryinit(rdb_mutex_t *mtx) {
  /* Logic from libsodium/core.c */
  long state;

  while ((state = InterlockedCompareExchange(&mtx->state, 1, 0)) == 1)
    Sleep(0);

  if (state == 0) {
    InitializeCriticalSection(&mtx->handle);

    if (InterlockedExchange(&mtx->state, 2) != 1)
      abort(); /* LCOV_EXCL_LINE */
  } else {
    assert(state == 2);
  }
}

void
rdb_mutex_init(rdb_mutex_t *mtx) {
  mtx->state = 2;
  InitializeCriticalSection(&mtx->handle);
}

void
rdb_mutex_destroy(rdb_mutex_t *mtx) {
  DeleteCriticalSection(&mtx->handle);
}

void
rdb_mutex_lock(rdb_mutex_t *mtx) {
  rdb_mutex_tryinit(mtx);
  EnterCriticalSection(&mtx->handle);
}

void
rdb_mutex_unlock(rdb_mutex_t *mtx) {
  LeaveCriticalSection(&mtx->handle);
}

/*
 * Conditional
 */

void
rdb_cond_init(rdb_cond_t *cond) {
  cond->waiters = 0;

  InitializeCriticalSection(&cond->lock);

  cond->signal = CreateEvent(NULL, FALSE, FALSE, NULL);
  cond->broadcast = CreateEvent(NULL, TRUE, FALSE, NULL);

  if (!cond->signal || !cond->broadcast)
    abort(); /* LCOV_EXCL_LINE */
}

void
rdb_cond_destroy(rdb_cond_t *cond) {
  if (!CloseHandle(cond->broadcast))
    abort(); /* LCOV_EXCL_LINE */

  if (!CloseHandle(cond->signal))
    abort(); /* LCOV_EXCL_LINE */

  DeleteCriticalSection(&cond->lock);
}

void
rdb_cond_signal(rdb_cond_t *cond) {
  int have_waiters;

  EnterCriticalSection(&cond->lock);
  have_waiters = (cond->waiters > 0);
  LeaveCriticalSection(&cond->lock);

  if (have_waiters)
    SetEvent(cond->signal);
}

void
rdb_cond_broadcast(rdb_cond_t *cond) {
  int have_waiters;

  EnterCriticalSection(&cond->lock);
  have_waiters = (cond->waiters > 0);
  LeaveCriticalSection(&cond->lock);

  if (have_waiters)
    SetEvent(cond->broadcast);
}

void
rdb_cond_wait(rdb_cond_t *cond, rdb_mutex_t *mtx) {
  HANDLE handles[2];
  int last_waiter;
  DWORD result;

  handles[0] = cond->signal;
  handles[1] = cond->broadcast;

  EnterCriticalSection(&cond->lock);
  cond->waiters++;
  LeaveCriticalSection(&cond->lock);

  LeaveCriticalSection(&mtx->handle);

  result = WaitForMultipleObjects(2, handles, FALSE, INFINITE);

  if (result != WAIT_OBJECT_0 && result != WAIT_OBJECT_0 + 1)
    abort(); /* LCOV_EXCL_LINE */

  EnterCriticalSection(&cond->lock);
  cond->waiters--;
  last_waiter = (result == WAIT_OBJECT_0 + 1 && cond->waiters == 0);
  LeaveCriticalSection(&cond->lock);

  if (last_waiter)
    ResetEvent(cond->broadcast);

  EnterCriticalSection(&mtx->handle);
}

/*
 * Thread
 */

static DWORD WINAPI /* __stdcall */
rdb_thread_run(void *ptr) {
  rdb_args_t args = *((rdb_args_t *)ptr);

  rdb_free(ptr);

  args.start(args.arg);

  return ERROR_SUCCESS;
}

void
rdb_thread_create(rdb_thread_t *thread, void (*start)(void *), void *arg) {
  rdb_args_t *args = rdb_malloc(sizeof(rdb_args_t));

  args->start = start;
  args->arg = arg;

  thread->handle = CreateThread(NULL, 0, rdb_thread_run, args, 0, NULL);

  if (thread->handle == NULL)
    abort(); /* LCOV_EXCL_LINE */
}

void
rdb_thread_detach(rdb_thread_t *thread) {
  if (CloseHandle(thread->handle) == FALSE)
    abort(); /* LCOV_EXCL_LINE */
}

void
rdb_thread_join(rdb_thread_t *thread) {
  WaitForSingleObject(thread->handle, INFINITE);

  if (CloseHandle(thread->handle) == FALSE)
    abort(); /* LCOV_EXCL_LINE */
}
