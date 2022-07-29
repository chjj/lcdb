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
 * Backend Selection
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
#  if __INTEL_COMPILER >= 1300 /* 13.0 */
#    define LDB_GNUC_ATOMICS
#  elif __INTEL_COMPILER >= 1100 /* 11.0 */
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
#elif LDB_GNUC_PREREQ(3, 0)
#  if defined(__i386__) || defined(__x86_64__)
#    define LDB_ASM_ATOMICS
#  endif
#elif defined(__sun) && defined(__SVR4)
#  if defined(__SUNPRO_C) && __SUNPRO_C >= 0x5110 /* 12.2 */
#    define LDB_SUN_ATOMICS
#  endif
#elif defined(_AIX) && defined(__IBMC__) && __IBMC__ >= 800
#  define LDB_AIX_ATOMICS
#elif defined(__chibicc__)
#  define LDB_CHIBICC_ATOMICS
#endif

#if (defined(LDB_GNUC_ATOMICS)    \
  || defined(LDB_SYNC_ATOMICS)    \
  || defined(LDB_ASM_ATOMICS)     \
  || defined(LDB_TINYC_ATOMICS)   \
  || defined(LDB_CHIBICC_ATOMICS) \
  || defined(LDB_SUN_ATOMICS)     \
  || defined(LDB_AIX_ATOMICS))
#  define LDB_HAVE_ATOMICS
#elif defined(_WIN32)
#  define LDB_MSVC_ATOMICS
#  define LDB_HAVE_ATOMICS
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
#  if !defined(__cplusplus) && !defined(__STDC_NO_ATOMICS__)
#    define LDB_STD_ATOMICS
#    define LDB_HAVE_ATOMICS
#  endif
#endif

/*
 * Types
 */

#if defined(LDB_STD_ATOMICS)     \
 || defined(LDB_TINYC_ATOMICS)   \
 || defined(LDB_CHIBICC_ATOMICS)
#  define ldb_atomic(type) _Atomic(long)
#  define ldb_atomic_ptr(type) _Atomic(type *)
#elif defined(LDB_GNUC_ATOMICS) || defined(LDB_SYNC_ATOMICS)
#  define ldb_atomic(type) volatile type
#  define ldb_atomic_ptr(type) type *volatile
#elif defined(LDB_ASM_ATOMICS)  \
   || defined(LDB_SUN_ATOMICS)  \
   || defined(LDB_MSVC_ATOMICS)
#  define ldb_atomic(type) volatile long
#  define ldb_atomic_ptr(type) void *volatile
#elif defined(LDB_AIX_ATOMICS)
#  define ldb_atomic(type) volatile int
#  define ldb_atomic_ptr(type) void *volatile
#else
#  define ldb_atomic(type) long
#  define ldb_atomic_ptr(type) void *
#endif

/*
 * Memory Order
 */

#if defined(LDB_STD_ATOMICS)
#  define ldb_order_relaxed memory_order_relaxed
#  define ldb_order_consume memory_order_consume
#  define ldb_order_acquire memory_order_acquire
#  define ldb_order_release memory_order_release
#  define ldb_order_acq_rel memory_order_acq_rel
#  define ldb_order_seq_cst memory_order_seq_cst
#elif defined(__ATOMIC_RELAXED)
#  define ldb_order_relaxed __ATOMIC_RELAXED
#  define ldb_order_consume __ATOMIC_CONSUME
#  define ldb_order_acquire __ATOMIC_ACQUIRE
#  define ldb_order_release __ATOMIC_RELEASE
#  define ldb_order_acq_rel __ATOMIC_ACQ_REL
#  define ldb_order_seq_cst __ATOMIC_SEQ_CST
#else
#  define ldb_order_relaxed 0
#  define ldb_order_consume 1
#  define ldb_order_acquire 2
#  define ldb_order_release 3
#  define ldb_order_acq_rel 4
#  define ldb_order_seq_cst 5
#endif

/*
 * Initialization
 */

#ifdef LDB_STD_ATOMICS
#  define ldb_atomic_init atomic_init
#  define ldb_atomic_init_ptr atomic_init
#else
#  define ldb_atomic_init(object, desired) \
    ldb_atomic_store(object, desired, ldb_order_relaxed)
#  define ldb_atomic_init_ptr ldb_atomic_init
#endif

/*
 * Builtins
 */

#if defined(LDB_STD_ATOMICS)

/*
 * Standard Atomics
 */

#include <stdatomic.h>

#define ldb_atomic_exchange atomic_exchange

LDB_STATIC long
ldb_atomic_compare_exchange(_Atomic(long) *object,
                            long expected,
                            long desired) {
  atomic_compare_exchange_strong(object, &expected, desired);
  return expected;
}

#define ldb_atomic_fetch_add atomic_fetch_add_explicit
#define ldb_atomic_fetch_sub atomic_fetch_sub_explicit
#define ldb_atomic_load atomic_load_explicit
#define ldb_atomic_store atomic_store_explicit
#define ldb_atomic_load_ptr atomic_load_explicit
#define ldb_atomic_store_ptr atomic_store_explicit

#elif defined(LDB_GNUC_ATOMICS)

/*
 * GNU Atomics
 */

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

/*
 * Sync Atomics
 */

#define ldb_compiler_barrier() __asm__ __volatile__ ("" ::: "memory")

#if defined(__i386__) || defined(__x86_64__)
#  define ldb_hardware_fence ldb_compiler_barrier
#  define ldb_atomic_exchange __sync_lock_test_and_set
#else
#  define ldb_hardware_fence __sync_synchronize
#  define ldb_atomic_exchange(object, desired) \
     (__sync_synchronize(), __sync_lock_test_and_set(object, desired))
#endif

#define ldb_atomic_compare_exchange __sync_val_compare_and_swap

#define ldb_atomic_fetch_add(object, operand, order) \
  __sync_fetch_and_add(object, operand)

#define ldb_atomic_fetch_sub(object, operand, order) \
  __sync_fetch_and_sub(object, operand)

#define ldb_atomic_load(object, order) __extension__ ({ \
  __typeof__((void)0, *(object)) _result;               \
  ldb_compiler_barrier();                               \
  _result = *(object);                                  \
  ldb_hardware_fence();                                 \
  _result;                                              \
})

#define ldb_atomic_store(object, desired, order) do { \
  ldb_hardware_fence();                               \
  *(object) = (desired);                              \
  ldb_compiler_barrier();                             \
} while (0)

#define ldb_atomic_load_ptr ldb_atomic_load
#define ldb_atomic_store_ptr ldb_atomic_store

#elif defined(LDB_ASM_ATOMICS)

/*
 * ASM Atomics
 */

#define ldb_compiler_barrier() __asm__ __volatile__ ("" ::: "memory")

LDB_STATIC long
ldb_atomic_exchange(volatile long *object, long desired) {
  __asm__ __volatile__ (
    "xchg %1, %0\n"
    : "+m" (*object),
      "+a" (desired)
  );
  return desired;
}

LDB_STATIC long
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

LDB_STATIC long
ldb_atomic__fetch_add(volatile long *object, long operand) {
  __asm__ __volatile__ (
    "lock xadd %1, %0\n"
    : "+m" (*object),
      "+a" (operand)
    :: "cc"
  );
  return operand;
}

LDB_STATIC long
ldb_atomic__load(volatile long *object) {
  long result;
  ldb_compiler_barrier();
  result = *object;
  ldb_compiler_barrier();
  return result;
}

LDB_STATIC void *
ldb_atomic__load_ptr(void *volatile *object) {
  void *result;
  ldb_compiler_barrier();
  result = *object;
  ldb_compiler_barrier();
  return result;
}

#define ldb_atomic_fetch_add(object, operand, order) \
  ldb_atomic__fetch_add(object, operand)

#define ldb_atomic_fetch_sub(object, operand, order) \
  ldb_atomic__fetch_add(object, -(long)(operand))

#define ldb_atomic_load(object, order) \
  ldb_atomic__load((volatile long *)(object))

#define ldb_atomic_store(object, desired, order) do { \
  ldb_compiler_barrier();                             \
  *(object) = (desired);                              \
  ldb_compiler_barrier();                             \
} while (0)

#define ldb_atomic_load_ptr(object, order) \
  ldb_atomic__load_ptr((void *volatile *)(object))

#define ldb_atomic_store_ptr ldb_atomic_store

#elif defined(LDB_TINYC_ATOMICS)

/*
 * Tiny Atomics
 */

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

/*
 * Chibi Atomics
 */

#define ldb_atomic_exchange __builtin_atomic_exchange

#define ldb_atomic_compare_exchange(object, expected, desired) ({ \
  long _exp = (expected);                                         \
  __builtin_compare_and_swap(object, &_exp, desired);             \
  _exp;                                                           \
})

/* Atomic additions are inlined by the compiler. */
#define ldb_atomic_fetch_add(object, operand, order) \
  (((*(object)) += (long)(operand)) - (long)(operand))

#define ldb_atomic_fetch_sub(object, operand, order) \
  (((*(object)) -= (long)(operand)) + (long)(operand))

/* We assume chibicc does not reorder volatiles. */
#define ldb_atomic_load(object, order) (*(object))
#define ldb_atomic_store(object, desired, order) (*(object) = (desired))
#define ldb_atomic_load_ptr ldb_atomic_load
#define ldb_atomic_store_ptr ldb_atomic_store

#elif defined(LDB_SUN_ATOMICS)

/*
 * Sun Atomics
 */

#include <atomic.h>
#include <mbarrier.h>

#define ldb_compiler_barrier __compiler_barrier

#if defined(__i386) || defined(__x86_64)
#  define ldb_hardware_fence __compiler_barrier
#else
#  define ldb_hardware_fence __machine_rw_barrier
#endif

static inline long
ldb_atomic__load(volatile long *object) {
  long result;
  ldb_compiler_barrier();
  result = *object;
  ldb_hardware_fence();
  return result;
}

static inline void *
ldb_atomic__load_ptr(void *volatile *object) {
  void *result;
  ldb_compiler_barrier();
  result = *object;
  ldb_hardware_fence();
  return result;
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

#define ldb_atomic_load(object, order) \
  ldb_atomic__load((volatile long *)(object))

#define ldb_atomic_store(object, desired, order) do { \
  ldb_hardware_fence();                               \
  *(object) = (desired);                              \
  ldb_compiler_barrier();                             \
} while (0)

#define ldb_atomic_load_ptr(object, order) \
  ldb_atomic__load_ptr((void *volatile *)(object))

#define ldb_atomic_store_ptr ldb_atomic_store

#elif defined(LDB_AIX_ATOMICS)

/*
 * AIX Atomics
 */

static int
ldb_atomic_exchange(volatile int *object, int desired) {
  int old;
  __sync();
  do {
    old = __lwarx(object)
  } while (__stwcx(object, desired) == 0);
  __isync();
  return old;
}

static int
ldb_atomic_compare_exchange(volatile int *object,
                            int expected,
                            int desired) {
  int old;
  __sync();
  do {
    old = __lwarx(object);
  } while (__stwcx(object, old == expected ? desired : old) == 0);
  __isync();
  return old;
}

static int
ldb_atomic_fetch_add(volatile int *object, int operand, int order) {
  int old;
  __sync();
  do {
    old = __lwarx(object);
  } while (__stwcx(object, old + operand) == 0);
  __isync();
  return old;
}

#define ldb_atomic_fetch_sub(object, operand, order) \
  ldb_atomic_fetch_add(object, -(int)(operand))

static int
ldb_atomic_load(const volatile int *object, int order) {
  int result;
  __fence();
  result = *object;
  __isync();
  return result;
}

#define ldb_atomic_store(object, desired, order) do { \
  __lwsync();                                         \
  *(object) = (desired);                              \
  __fence();                                          \
} while (0)

static void *
ldb_atomic_load_ptr(const void *volatile *object, int order) {
  void *result;
  __fence();
  result = *object;
  __isync();
  return result;
}

#define ldb_atomic_store_ptr ldb_atomic_store

#elif defined(LDB_MSVC_ATOMICS)

/*
 * MSVC Atomics
 */

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

/*
 * Mutex Fallback
 */

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
