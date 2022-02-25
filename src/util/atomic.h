/*!
 * atomic.h - atomics for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_ATOMICS_H
#define RDB_ATOMICS_H

#include <stddef.h>
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
#  define rdb_atomic_ptr(type) void *volatile
#elif defined(RDB_HAVE_ATOMICS)
#  define rdb_atomic(type) volatile type
#  define rdb_atomic_ptr(type) type *volatile
#else /* !RDB_HAVE_ATOMICS */
#  define rdb_atomic(type) long
#  define rdb_atomic_ptr(type) void *
#endif /* !RDB_HAVE_ATOMICS */

/*
 * Memory Order
 */

#if defined(__ATOMIC_RELAXED)
#  define rdb_order_relaxed __ATOMIC_RELAXED
#else
#  define rdb_order_relaxed 0
#endif

#if defined(__ATOMIC_CONSUME)
#  define rdb_order_consume __ATOMIC_CONSUME
#else
#  define rdb_order_consume 1
#endif

#if defined(__ATOMIC_ACQUIRE)
#  define rdb_order_acquire __ATOMIC_ACQUIRE
#else
#  define rdb_order_acquire 2
#endif

#if defined(__ATOMIC_RELEASE)
#  define rdb_order_release __ATOMIC_RELEASE
#else
#  define rdb_order_release 3
#endif

#if defined(__ATOMIC_ACQ_REL)
#  define rdb_order_acq_rel __ATOMIC_ACQ_REL
#else
#  define rdb_order_acq_rel 4
#endif

#if defined(__ATOMIC_SEQ_CST)
#  define rdb_order_seq_cst __ATOMIC_SEQ_CST
#else
#  define rdb_order_seq_cst 5
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

#define rdb_atomic_load_ptr rdb_atomic_load
#define rdb_atomic_store_ptr rdb_atomic_store

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

#define rdb_atomic_load_ptr(object, order) \
  __sync_val_compare_and_swap(object, NULL, NULL)

#define rdb_atomic_store_ptr rdb_atomic_store

#elif defined(RDB_MSVC_ATOMICS)

long
rdb_atomic__fetch_add(volatile long *object, long operand);

long
rdb_atomic__load(volatile long *object);

void
rdb_atomic__store(volatile long *object, long desired);

void *
rdb_atomic__load_ptr(void *volatile *object);

void
rdb_atomic__store_ptr(void *volatile *object, void *desired);

#define rdb_atomic_fetch_add(object, operand, order) \
  rdb_atomic__fetch_add(object, operand)

#define rdb_atomic_fetch_sub(object, operand, order) \
  rdb_atomic__fetch_add(object, -operand)

#define rdb_atomic_load(object, order) \
  rdb_atomic__load(object)

#define rdb_atomic_store(object, desired, order) \
  rdb_atomic__store(object, desired)

#define rdb_atomic_load_ptr(object, order) \
  rdb_atomic__load_ptr((void *volatile *)object)

#define rdb_atomic_store_ptr(object, desired, order) \
  rdb_atomic__store_ptr((void *volatile *)object, (void *)desired)

#else /* !RDB_MSVC_ATOMICS */

long
rdb_atomic__fetch_add(long *object, long operand);

long
rdb_atomic__load(long *object);

void
rdb_atomic__store(long *object, long desired);

void *
rdb_atomic__load_ptr(void **object);

void
rdb_atomic__store_ptr(void **object, void *desired);

#define rdb_atomic_fetch_add(object, operand, order) \
  rdb_atomic__fetch_add(object, operand)

#define rdb_atomic_fetch_sub(object, operand, order) \
  rdb_atomic__fetch_add(object, -operand)

#define rdb_atomic_load(object, order) \
  rdb_atomic__load(object)

#define rdb_atomic_store(object, desired, order) \
  rdb_atomic__store(object, desired)

#define rdb_atomic_load_ptr(object, order) \
  rdb_atomic__load_ptr((void **)object)

#define rdb_atomic_store_ptr(object, desired, order) \
  rdb_atomic__store_ptr((void **)object, (void *)desired)

#endif /* !RDB_MSVC_ATOMICS */

#endif /* RDB_ATOMICS_H */
