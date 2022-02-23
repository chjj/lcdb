/*!
 * atomic.c - atomics for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include "atomic.h"

#if defined(RDB_GNUC_ATOMICS) || defined(RDB_LEGACY_ATOMICS)

int
rdb_atomic_no_empty_translation_unit(void);

int
rdb_atomic_no_empty_translation_unit(void) {
  return 0;
}

#elif defined(RDB_MSVC_ATOMICS)

#include <windows.h>

long
rdb_atomic_fetch_add(volatile long *object, long operand, int order) {
  (void)order;
  return InterlockedExchangeAdd(object, operand);
}

long
rdb_atomic_fetch_sub(volatile long *object, long operand, int order) {
  (void)order;
  return InterlockedExchangeAdd(object, -operand);
}

long
rdb_atomic_load(volatile long *object, int order) {
  (void)order;
  MemoryBarrier();
  return *object;
}

void
rdb_atomic_store(volatile long *object, long desired, int order) {
  (void)order;
  *object = desired;
  MemoryBarrier();
}

#else /* !RDB_MSVC_ATOMICS */

#include "port.h"

static rdb_mutex_t rdb_atomic_lock = RDB_MUTEX_INITIALIZER;

long
rdb_atomic_fetch_add(long *object, long operand, int order) {
  long value;
  (void)order;
  rdb_mutex_lock(&rdb_atomic_lock);
  value = *object;
  *object += operand;
  rdb_mutex_unlock(&rdb_atomic_lock);
  return value;
}

long
rdb_atomic_fetch_sub(long *object, long operand, int order) {
  return rdb_atomic_fetch_add(object, -operand, order);
}

long
rdb_atomic_load(long *object, int order) {
  long value;
  (void)order;
  rdb_mutex_lock(&rdb_atomic_lock);
  value = *object;
  rdb_mutex_unlock(&rdb_atomic_lock);
  return value;
}

void
rdb_atomic_store(long *object, long desired, int order) {
  (void)order;
  rdb_mutex_lock(&rdb_atomic_lock);
  *object = desired;
  rdb_mutex_unlock(&rdb_atomic_lock);
}

#endif /* !RDB_MSVC_ATOMICS */
