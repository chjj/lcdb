/*!
 * port_none_impl.h - no threads port for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdlib.h>
#include "port.h"

/*
 * Mutex
 */

void
rdb_mutex_init(rdb_mutex_t *mtx) {
  (void)mtx;
}

void
rdb_mutex_destroy(rdb_mutex_t *mtx) {
  (void)mtx;
}

void
rdb_mutex_lock(rdb_mutex_t *mtx) {
  (void)mtx;
}

void
rdb_mutex_unlock(rdb_mutex_t *mtx) {
  (void)mtx;
}

/*
 * Conditional
 */

void
rdb_cond_init(rdb_cond_t *cond) {
  (void)cond;
}

void
rdb_cond_destroy(rdb_cond_t *cond) {
  (void)cond;
}

void
rdb_cond_signal(rdb_cond_t *cond) {
  (void)cond;
}

void
rdb_cond_broadcast(rdb_cond_t *cond) {
  (void)cond;
}

void
rdb_cond_wait(rdb_cond_t *cond, rdb_mutex_t *mtx) {
  (void)cond;
  (void)mtx;
  abort(); /* LCOV_EXCL_LINE */
}

/*
 * Thread
 */

void
rdb_thread_create(rdb_thread_t *thread, void (*start)(void *), void *arg) {
  (void)thread;
  (void)start;
  (void)arg;
  abort(); /* LCOV_EXCL_LINE */
}

void
rdb_thread_detach(rdb_thread_t *thread) {
  (void)thread;
}

void
rdb_thread_join(rdb_thread_t *thread) {
  (void)thread;
}
