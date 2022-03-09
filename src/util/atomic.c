/*!
 * atomic.c - atomics for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include "atomic.h"

#if defined(RDB_CLANG_ATOMICS) || defined(RDB_GNUC_ATOMICS)

int
rdb_atomic_no_empty_translation_unit(void);

int
rdb_atomic_no_empty_translation_unit(void) {
  return 0;
}

#elif defined(RDB_MSVC_ATOMICS)

#include <windows.h>

long
rdb_atomic__fetch_add(volatile long *object, long operand) {
  return InterlockedExchangeAdd(object, operand);
}

long
rdb_atomic__load(volatile long *object) {
  return InterlockedCompareExchange(object, 0, 0);
}

void
rdb_atomic__store(volatile long *object, long desired) {
  (void)InterlockedExchange(object, desired);
}

void *
rdb_atomic__load_ptr(void *volatile *object) {
  return InterlockedCompareExchangePointer(object, NULL, NULL);
}

void
rdb_atomic__store_ptr(void *volatile *object, void *desired) {
  (void)InterlockedExchangePointer(object, desired);
}

#else /* !RDB_MSVC_ATOMICS */

#include "port.h"

static rdb_mutex_t rdb_atomic_lock = RDB_MUTEX_INITIALIZER;

long
rdb_atomic__fetch_add(long *object, long operand) {
  long result;
  rdb_mutex_lock(&rdb_atomic_lock);
  result = *object;
  *object += operand;
  rdb_mutex_unlock(&rdb_atomic_lock);
  return result;
}

long
rdb_atomic__load(long *object) {
  long result;
  rdb_mutex_lock(&rdb_atomic_lock);
  result = *object;
  rdb_mutex_unlock(&rdb_atomic_lock);
  return result;
}

void
rdb_atomic__store(long *object, long desired) {
  rdb_mutex_lock(&rdb_atomic_lock);
  *object = desired;
  rdb_mutex_unlock(&rdb_atomic_lock);
}

void *
rdb_atomic__load_ptr(void **object) {
  void *result;
  rdb_mutex_lock(&rdb_atomic_lock);
  result = *object;
  rdb_mutex_unlock(&rdb_atomic_lock);
  return result;
}

void
rdb_atomic__store_ptr(void **object, void *desired) {
  rdb_mutex_lock(&rdb_atomic_lock);
  *object = desired;
  rdb_mutex_unlock(&rdb_atomic_lock);
}

#endif /* !RDB_MSVC_ATOMICS */
