/*!
 * port.c - ported functions for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 */

#if defined(_WIN32)
#  include "port_win_impl.h"
#elif defined(LDB_PTHREAD)
#  include "port_unix_impl.h"
#else
#  include "port_none_impl.h"
#endif

/*
 * RW Lock
 */

void
ldb_rwlock_init(ldb_rwlock_t *lock) {
  lock->readers = 0;
  lock->waiters = 0;
  lock->writing = 0;

  ldb_mutex_init(&lock->mutex);
  ldb_cond_init(&lock->cond);
}

void
ldb_rwlock_destroy(ldb_rwlock_t *lock) {
  ldb_cond_destroy(&lock->cond);
  ldb_mutex_destroy(&lock->mutex);
}

void
ldb_rwlock_rdlock(ldb_rwlock_t *lock) {
  ldb_mutex_lock(&lock->mutex);

  while (lock->writing || lock->waiters > 0)
    ldb_cond_wait(&lock->cond, &lock->mutex);

  lock->readers++;

  ldb_mutex_unlock(&lock->mutex);
}

void
ldb_rwlock_rdunlock(ldb_rwlock_t *lock) {
  ldb_mutex_lock(&lock->mutex);

  if (--lock->readers == 0)
    ldb_cond_broadcast(&lock->cond);

  ldb_mutex_unlock(&lock->mutex);
}

void
ldb_rwlock_wrlock(ldb_rwlock_t *lock) {
  ldb_mutex_lock(&lock->mutex);

  lock->waiters++;

  while (lock->writing || lock->readers > 0)
    ldb_cond_wait(&lock->cond, &lock->mutex);

  lock->waiters--;
  lock->writing = 1;

  ldb_mutex_unlock(&lock->mutex);
}

void
ldb_rwlock_wrunlock(ldb_rwlock_t *lock) {
  ldb_mutex_lock(&lock->mutex);
  lock->writing = 0;
  ldb_cond_broadcast(&lock->cond);
  ldb_mutex_unlock(&lock->mutex);
}
