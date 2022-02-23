/*!
 * atomic.h - atomics for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_ATOMICS_H
#define RDB_ATOMICS_H

#include <limits.h>
#include "internal.h"

/*
 * Compiler Compat
 */

#if RDB_GNUC_PREREQ(4, 7)
#  define RDB_GNUC_ATOMICS
#  define RDB_HAVE_ATOMICS
#elif RDB_GNUC_PREREQ(3, 0) /* XXX */
#  define RDB_LEGACY_ATOMICS
#  define RDB_HAVE_ATOMICS
#elif defined(_MSC_VER) && defined(_WIN32)
#  define RDB_MSVC_ATOMICS
#  define RDB_HAVE_ATOMICS
#endif

/*
 * Backend Selection
 */

#if defined(RDB_MSVC_ATOMICS)
#  define rdb_atomic(type) volatile long
#elif defined(RDB_HAVE_ATOMICS)
#  define rdb_atomic(type) volatile type
#else /* !RDB_HAVE_ATOMICS */
#  define rdb_atomic(type) long
#endif /* !RDB_HAVE_ATOMICS */

/*
 * Memory Order
 */

#define rdb_order_relaxed 0
#define rdb_order_acquire 2
#define rdb_order_release 3

#if defined(__ATOMIC_RELAXED)
#  undef rdb_order_relaxed
#  define rdb_order_relaxed __ATOMIC_RELAXED
#endif

#if defined(__ATOMIC_ACQUIRE)
#  undef rdb_order_acquire
#  define rdb_order_acquire __ATOMIC_ACQUIRE
#endif

#if defined(__ATOMIC_RELEASE)
#  undef rdb_order_release
#  define rdb_order_release __ATOMIC_RELEASE
#endif

/*
 * Builtins
 */

#if defined(RDB_GNUC_ATOMICS)

#define rdb_atomic_fetch_add(object, operand, order) \
  __atomic_fetch_add(object, operand, order)
#define rdb_atomic_fetch_sub(object, operand, order) \
  __atomic_fetch_sub(object, operand, order)
#define rdb_atomic_load(object, order) \
  __atomic_load_n(object, order)
#define rdb_atomic_store(object, desired, order) \
  __atomic_store_n(object, desired, order)

#elif defined(RDB_LEGACY_ATOMICS)

#define rdb_atomic_fetch_add(object, operand, order) \
  __sync_fetch_and_add(object, operand)
#define rdb_atomic_fetch_sub(object, operand, order) \
  __sync_fetch_and_sub(object, operand)
#define rdb_atomic_load(object, order) \
  __sync_fetch_and_add(object, 0)
#define rdb_atomic_store(object, desired, order) do { \
  __sync_synchronize();                               \
  *(object) = (desired);                              \
  __sync_synchronize();                               \
} while (0)

#elif defined(RDB_MSVC_ATOMICS)

long
rdb_atomic_fetch_add(volatile long *object, long operand, int order);

long
rdb_atomic_fetch_sub(volatile long *object, long operand, int order);

long
rdb_atomic_load(volatile long *object, int order);

void
rdb_atomic_store(volatile long *object, long desired, int order);

#else /* !RDB_MSVC_ATOMICS */

long
rdb_atomic_fetch_add(long *object, long operand, int order);

long
rdb_atomic_fetch_sub(long *object, long operand, int order);

long
rdb_atomic_load(long *object, int order);

void
rdb_atomic_store(long *object, long desired, int order);

#endif /* !RDB_MSVC_ATOMICS */

#endif /* RDB_ATOMICS_H */
