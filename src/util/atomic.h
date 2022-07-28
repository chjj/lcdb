/*!
 * atomic.h - atomics for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 */

#ifndef LDB_ATOMICS_H
#define LDB_ATOMICS_H

#include <stddef.h>
#include <limits.h>
#include "internal.h"

/*
 * Compiler Compat
 */

#if defined(__clang__)
#  ifdef __has_extension
#    if __has_extension(c_atomic)
#      define LDB_GNUC_ATOMICS
#    endif
#  endif
#  ifndef LDB_GNUC_ATOMICS
#    if __clang_major__ >= 3
#      define LDB_SYNC_ATOMICS
#    endif
#  endif
#elif defined(__INTEL_COMPILER) || defined(__ICC)
#  if __INTEL_COMPILER >= 1100 /* 11.0 */
#    define LDB_SYNC_ATOMICS
#  endif
#elif defined(__CC_ARM)
/* Unknown. */
#elif defined(__TINYC__)
#  if (__TINYC__ + 0) > 927 /* 0.9.27 */
#    define LDB_TINYC_ATOMICS
#  elif defined(__i386__) || defined(__x86_64__)
#    define LDB_ASM_ATOMICS
#  endif
#elif defined(__PCC__)
#  if defined(__i386__) || defined(__x86_64__)
#    define LDB_ASM_ATOMICS
#  endif
#elif defined(__NWCC__)
/* Nothing. */
#elif LDB_GNUC_PREREQ(4, 7)
#  define LDB_GNUC_ATOMICS
#elif LDB_GNUC_PREREQ(4, 6) && defined(__arm__)
#  define LDB_SYNC_ATOMICS
#elif LDB_GNUC_PREREQ(4, 5) && defined(__BFIN__)
#  define LDB_SYNC_ATOMICS
#elif LDB_GNUC_PREREQ(4, 3) && (defined(__mips__) || defined(__xtensa__))
#  define LDB_SYNC_ATOMICS
#elif LDB_GNUC_PREREQ(4, 2) && (defined(__sh__) || defined(__sparc__))
#  define LDB_SYNC_ATOMICS
#elif LDB_GNUC_PREREQ(4, 1) && (defined(__alpha__)  \
                             || defined(__i386__)   \
                             || defined(__x86_64__) \
                             || defined(__amd64__)  \
                             || defined(_IBMR2)     \
                             || defined(__s390__)   \
                             || defined(__s390x__))
#  define LDB_SYNC_ATOMICS
#elif LDB_GNUC_PREREQ(3, 0) && defined(__ia64__)
#  define LDB_SYNC_ATOMICS
#elif defined(__sun) && defined(__SVR4)
#  if defined(__SUNPRO_C) && __SUNPRO_C >= 0x5110 /* 12.2 */
#    include <atomic.h>
#    include <mbarrier.h>
#    define LDB_SUN_ATOMICS
#  endif
#elif defined(__chibicc__)
#  define LDB_CHIBICC_ATOMICS
#elif defined(_WIN32)
#  define LDB_MSVC_ATOMICS
#endif

#if (defined(LDB_GNUC_ATOMICS)    \
  || defined(LDB_SYNC_ATOMICS)    \
  || defined(LDB_SUN_ATOMICS)     \
  || defined(LDB_ASM_ATOMICS)     \
  || defined(LDB_TINYC_ATOMICS)   \
  || defined(LDB_CHIBICC_ATOMICS) \
  || defined(LDB_MSVC_ATOMICS))
#  define LDB_HAVE_ATOMICS
#endif

/*
 * Backend Selection
 */

#if defined(LDB_GNUC_ATOMICS) || defined(LDB_SYNC_ATOMICS)
#  define ldb_atomic(type) volatile type
#  define ldb_atomic_ptr(type) type *volatile
#elif defined(LDB_SUN_ATOMICS) || defined(LDB_MSVC_ATOMICS)
#  define ldb_atomic(type) volatile long
#  define ldb_atomic_ptr(type) void *volatile
#elif defined(LDB_ASM_ATOMICS)
#  define ldb_atomic(type) volatile long
#  define ldb_atomic_ptr(type) void *volatile
#elif defined(LDB_TINYC_ATOMICS) || defined(LDB_CHIBICC_ATOMICS)
#  define ldb_atomic(type) _Atomic(long)
#  define ldb_atomic_ptr(type) _Atomic(type *)
#else
#  define ldb_atomic(type) long
#  define ldb_atomic_ptr(type) void *
#endif

/*
 * Memory Order
 */

#if defined(__ATOMIC_RELAXED)
#  define ldb_order_relaxed __ATOMIC_RELAXED
#else
#  define ldb_order_relaxed 0
#endif

#if defined(__ATOMIC_CONSUME)
#  define ldb_order_consume __ATOMIC_CONSUME
#else
#  define ldb_order_consume 1
#endif

#if defined(__ATOMIC_ACQUIRE)
#  define ldb_order_acquire __ATOMIC_ACQUIRE
#else
#  define ldb_order_acquire 2
#endif

#if defined(__ATOMIC_RELEASE)
#  define ldb_order_release __ATOMIC_RELEASE
#else
#  define ldb_order_release 3
#endif

#if defined(__ATOMIC_ACQ_REL)
#  define ldb_order_acq_rel __ATOMIC_ACQ_REL
#else
#  define ldb_order_acq_rel 4
#endif

#if defined(__ATOMIC_SEQ_CST)
#  define ldb_order_seq_cst __ATOMIC_SEQ_CST
#else
#  define ldb_order_seq_cst 5
#endif

/*
 * Builtins
 */

#if defined(LDB_GNUC_ATOMICS)

#define ldb_atomic_exchange(object, desired) \
  __atomic_exchange_n(object, desired, 5)

#define ldb_atomic_compare_exchange(object, expected, desired)  \
__extension__ ({                                                \
  __typeof__((void)0, *(object)) _exp = (expected);             \
  __atomic_compare_exchange_n(object, &_exp, desired, 0, 5, 5); \
  _exp;                                                         \
})

#define ldb_atomic_fetch_add __atomic_fetch_add
#define ldb_atomic_fetch_sub __atomic_fetch_sub
#define ldb_atomic_load __atomic_load_n
#define ldb_atomic_store __atomic_store_n
#define ldb_atomic_load_ptr __atomic_load_n
#define ldb_atomic_store_ptr __atomic_store_n

#elif defined(LDB_SYNC_ATOMICS)

#define ldb_atomic_exchange __sync_lock_test_and_set
#define ldb_atomic_compare_exchange __sync_val_compare_and_swap

#define ldb_atomic_fetch_add(object, operand, order) \
  __sync_fetch_and_add(object, operand)

#define ldb_atomic_fetch_sub(object, operand, order) \
  __sync_fetch_and_sub(object, operand)

#define ldb_atomic_load(object, order) \
  (__sync_synchronize(), *(object))

#define ldb_atomic_store(object, desired, order) do { \
  *(object) = (desired);                              \
  __sync_synchronize();                               \
} while (0)

#define ldb_atomic_load_ptr ldb_atomic_load
#define ldb_atomic_store_ptr ldb_atomic_store

#elif defined(LDB_SUN_ATOMICS)

static inline void
ldb_rw_barrier(void) {
  __machine_rw_barrier();
}

#define ldb_atomic_exchange(object, desired) \
  ((long)atomic_swap_ulong((volatile unsigned long *)(object), desired))

#define ldb_atomic_compare_exchange(object, expected, desired) \
  ((long)atomic_cas_ulong((volatile unsigned long *)(object),  \
                          expected, desired))

#define ldb_atomic_fetch_add(object, operand, order) \
  ((long)atomic_add_long_nv((volatile unsigned long *)(object), operand) - \
   (long)(operand))

#define ldb_atomic_fetch_sub(object, operand, order) \
  ldb_atomic_fetch_add(object, -(long)(operand))

#define ldb_atomic_load(object, order) (ldb_rw_barrier(), *(object))

#define ldb_atomic_store(object, desired, order) do { \
  *(object) = (desired);                              \
  ldb_rw_barrier();                                   \
} while (0)

#define ldb_atomic_load_ptr ldb_atomic_load
#define ldb_atomic_store_ptr ldb_atomic_store

#elif defined(LDB_ASM_ATOMICS)

static long
ldb_atomic_exchange(volatile long *object, long desired) {
  __asm__ __volatile__ (
    "xchg %1, %0\n"
    : "+m" (*object),
      "+a" (desired)
  );
  return desired;
}

static long
ldb_atomic_compare_exchange(volatile long *object,
                            long expected,
                            long desired) {
  __asm__ __volatile__ (
    "lock cmpxchg %2, %0\n"
    : "+m" (*object),
      "+a" (expected)
    : "d" (desired)
    : "cc"
  );
  return expected;
}

static long
ldb_atomic__fetch_add(volatile long *object, long operand) {
  __asm__ __volatile__ (
    "lock xadd %1, %0\n"
    : "+m" (*object),
      "+a" (operand)
    :: "cc"
  );
  return operand;
}

#define ldb_atomic_fetch_add(object, operand, order) \
  ldb_atomic__fetch_add(object, operand)

#define ldb_atomic_fetch_sub(object, operand, order) \
  ldb_atomic__fetch_add(object, -(long)(operand))

#define ldb_atomic_load(object, order) \
  ldb_atomic_compare_exchange((volatile long *)(object), 0, 0)

#define ldb_atomic_store(object, desired, order) \
  ((void)ldb_atomic_exchange(object, desired))

#define ldb_atomic_load_ptr(object, order) \
  ((void *)ldb_atomic_compare_exchange((volatile long *)(object), 0, 0))

#define ldb_atomic_store_ptr(object, desired, order) \
  ((void)ldb_atomic_exchange((volatile long *)(object), (long)(desired)))

#elif defined(LDB_TINYC_ATOMICS)

#define ldb_atomic_exchange(object, desired) \
  __atomic_exchange(object, desired, 5)

#define ldb_atomic_compare_exchange(object, expected, desired) ({ \
  long _exp = (expected);                                         \
  __atomic_compare_exchange(object, &_exp, desired, 0, 5, 5);     \
  _exp;                                                           \
})

#define ldb_atomic_fetch_add __atomic_fetch_add
#define ldb_atomic_fetch_sub __atomic_fetch_sub
#define ldb_atomic_load __atomic_load
#define ldb_atomic_store __atomic_store
#define ldb_atomic_load_ptr  __atomic_load
#define ldb_atomic_store_ptr __atomic_store

#elif defined(LDB_CHIBICC_ATOMICS)

#define ldb_atomic_exchange __builtin_atomic_exchange

#define ldb_atomic_compare_exchange(object, expected, desired) ({ \
  long _exp = (expected);                                         \
  __builtin_compare_and_swap(object, &_exp, desired);             \
  _exp;                                                           \
})

#define ldb_atomic_fetch_add(object, operand, order) \
  (((*(object)) += (long)(operand)) - (long)(operand))

#define ldb_atomic_fetch_sub(object, operand, order) \
  (((*(object)) -= (long)(operand)) + (long)(operand))

#define ldb_atomic_load(object, order) (*(object))
#define ldb_atomic_store(object, desired, order) (*(object) = (desired))
#define ldb_atomic_load_ptr ldb_atomic_load
#define ldb_atomic_store_ptr ldb_atomic_store

#elif defined(LDB_MSVC_ATOMICS)

long
ldb_atomic__exchange(volatile long *object, long desired);

long
ldb_atomic__compare_exchange(volatile long *object,
                             long expected,
                             long desired);

long
ldb_atomic__fetch_add(volatile long *object, long operand);

long
ldb_atomic__load(volatile long *object);

void
ldb_atomic__store(volatile long *object, long desired);

void *
ldb_atomic__load_ptr(void *volatile *object);

void
ldb_atomic__store_ptr(void *volatile *object, void *desired);

#define ldb_atomic_exchange ldb_atomic__exchange
#define ldb_atomic_compare_exchange ldb_atomic__compare_exchange

#define ldb_atomic_fetch_add(object, operand, order) \
  ldb_atomic__fetch_add(object, operand)

#define ldb_atomic_fetch_sub(object, operand, order) \
  ldb_atomic__fetch_add(object, -(long)(operand))

#define ldb_atomic_load(object, order) \
  ldb_atomic__load((volatile long *)(object))

#define ldb_atomic_store(object, desired, order) \
  ldb_atomic__store(object, desired)

#define ldb_atomic_load_ptr(object, order) \
  ldb_atomic__load_ptr((void *volatile *)(object))

#define ldb_atomic_store_ptr(object, desired, order) \
  ldb_atomic__store_ptr((void *volatile *)(object), (void *)(desired))

#else /* !LDB_MSVC_ATOMICS */

long
ldb_atomic__exchange(long *object, long desired);

long
ldb_atomic__compare_exchange(long *object, long expected, long desired);

long
ldb_atomic__fetch_add(long *object, long operand);

long
ldb_atomic__load(long *object);

void
ldb_atomic__store(long *object, long desired);

void *
ldb_atomic__load_ptr(void **object);

void
ldb_atomic__store_ptr(void **object, void *desired);

#define ldb_atomic_exchange ldb_atomic__exchange
#define ldb_atomic_compare_exchange ldb_atomic__compare_exchange

#define ldb_atomic_fetch_add(object, operand, order) \
  ldb_atomic__fetch_add(object, operand)

#define ldb_atomic_fetch_sub(object, operand, order) \
  ldb_atomic__fetch_add(object, -(long)(operand))

#define ldb_atomic_load(object, order) \
  ldb_atomic__load((long *)(object))

#define ldb_atomic_store(object, desired, order) \
  ldb_atomic__store(object, desired)

#define ldb_atomic_load_ptr(object, order) \
  ldb_atomic__load_ptr((void **)(object))

#define ldb_atomic_store_ptr(object, desired, order) \
  ldb_atomic__store_ptr((void **)(object), (void *)(desired))

#endif /* !LDB_MSVC_ATOMICS */

#endif /* LDB_ATOMICS_H */
