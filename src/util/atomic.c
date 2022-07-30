/*!
 * atomic.c - atomics for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 */

#include "atomic.h"

/*
 * Builtins
 */

#if defined(LDB_MSVC_ATOMICS)

/*
 * MSVC Atomics
 */

#ifdef LDB_HAVE_INTRIN
#  include <intrin.h>
#else
#  include <windows.h>
#endif

/*
 * Compat
 */

#undef USE_INTRIN
#undef USE_MEM_BARRIER
#undef USE_INLINE_ASM

#ifdef LDB_HAVE_INTRIN
#  define USE_INTRIN
#  if defined(_M_IX86) || defined(_M_X64)
#    define USE_MEM_BARRIER
#  endif
#endif

#if defined(_MSC_VER) && !defined(__clang__)        \
                      && !defined(__INTEL_COMPILER) \
                      && !defined(__ICL)
#  ifdef _M_IX86
#    define USE_INLINE_ASM
#  endif
#endif

/*
 * Backend
 */

void
ldb_atomic__store(volatile ldb_word_t *object, ldb_word_t desired) {
#if defined(USE_MEM_BARRIER)
  _ReadWriteBarrier();
  *object = desired;
  _ReadWriteBarrier();
#elif defined(USE_INLINE_ASM)
  __asm {
    mov ecx, object
    mov eax, desired
    mov [ecx], eax
  }
#elif defined(USE_INTRIN) && defined(_WIN64)
  (void)_InterlockedExchange64(object, desired);
#elif defined(USE_INTRIN)
  (void)_InterlockedExchange(object, desired);
#else
  /* Windows 95 and above. */
  (void)InterlockedExchange(object, desired);
#endif
}

void
ldb_atomic__store_ptr(void *volatile *object, void *desired) {
#if defined(USE_MEM_BARRIER)
  _ReadWriteBarrier();
  *object = desired;
  _ReadWriteBarrier();
#elif defined(USE_INLINE_ASM)
  __asm {
    mov ecx, object
    mov eax, desired
    mov [ecx], eax
  }
#elif defined(USE_INTRIN) && defined(_WIN64)
  (void)_InterlockedExchangePointer(object, desired);
#elif defined(USE_INTRIN)
  (void)_InterlockedExchange((volatile long *)object, (long)desired);
#elif defined(_WIN64)
  /* Windows XP and above. */
  (void)InterlockedExchangePointer(object, desired);
#else
  /* Windows 95 and above. */
  (void)InterlockedExchange((volatile long *)object, (long)desired);
#endif
}

ldb_word_t
ldb_atomic__load(volatile ldb_word_t *object) {
#if defined(USE_MEM_BARRIER)
  ldb_word_t result;
  _ReadWriteBarrier();
  result = *object;
  _ReadWriteBarrier();
  return result;
#elif defined(USE_INLINE_ASM)
  __asm {
    mov ecx, object
    mov eax, [ecx]
  }
#elif defined(USE_INTRIN) && defined(_WIN64)
  return _InterlockedCompareExchange64(object, 0, 0);
#elif defined(USE_INTRIN)
  return _InterlockedCompareExchange(object, 0, 0);
#else
  /* Windows 98 and above. */
  return InterlockedCompareExchange(object, 0, 0);
#endif
}

void *
ldb_atomic__load_ptr(void *volatile *object) {
#if defined(USE_MEM_BARRIER)
  void *result;
  _ReadWriteBarrier();
  result = *object;
  _ReadWriteBarrier();
  return result;
#elif defined(USE_INLINE_ASM)
  __asm {
    mov ecx, object
    mov eax, [ecx]
  }
#elif defined(USE_INTRIN) && defined(_WIN64)
  return _InterlockedCompareExchangePointer(object, NULL, NULL);
#elif defined(USE_INTRIN)
  return (void *)_InterlockedCompareExchange((volatile long *)object, 0, 0);
#elif defined(_WIN64)
  /* Windows XP and above. */
  return InterlockedCompareExchangePointer(object, NULL, NULL);
#else
  /* Windows 98 and above. */
  return (void *)InterlockedCompareExchange((volatile long *)object, 0, 0);
#endif
}

ldb_word_t
ldb_atomic__exchange(volatile ldb_word_t *object, ldb_word_t desired) {
#if defined(USE_INLINE_ASM)
  __asm {
    mov ecx, object
    mov eax, desired
    xchg [ecx], eax
  }
#elif defined(USE_INTRIN) && defined(_WIN64)
  return _InterlockedExchange64(object, desired);
#elif defined(USE_INTRIN)
  return _InterlockedExchange(object, desired);
#else
  /* Windows 95 and above. */
  return InterlockedExchange(object, desired);
#endif
}

ldb_word_t
ldb_atomic__compare_exchange(volatile ldb_word_t *object,
                             ldb_word_t expected,
                             ldb_word_t desired) {
#if defined(USE_INLINE_ASM)
  __asm {
    mov ecx, object
    mov eax, expected
    mov edx, desired
    lock cmpxchg [ecx], edx
  }
#elif defined(USE_INTRIN) && defined(_WIN64)
  return _InterlockedCompareExchange64(object, desired, expected);
#elif defined(USE_INTRIN)
  return _InterlockedCompareExchange(object, desired, expected);
#else
  /* Windows 98 and above. */
  return InterlockedCompareExchange(object, desired, expected);
#endif
}

ldb_word_t
ldb_atomic__fetch_add(volatile ldb_word_t *object, ldb_word_t operand) {
#if defined(USE_INLINE_ASM)
  __asm {
    mov ecx, object
    mov eax, operand
    lock xadd [ecx], eax
  }
#elif defined(USE_INTRIN) && defined(_WIN64)
  return _InterlockedExchangeAdd64(object, operand);
#elif defined(USE_INTRIN)
  return _InterlockedExchangeAdd(object, operand);
#else
  /* Windows 98 and above. */
  return InterlockedExchangeAdd(object, operand);
#endif
}

#elif defined(LDB_HAVE_ATOMICS)

/*
 * Non-Empty (avoids empty translation unit)
 */

int
ldb_atomic__nonempty(void);

int
ldb_atomic__nonempty(void) {
  return 0;
}

#else /* !LDB_HAVE_ATOMICS */

/*
 * Mutex Fallback
 */

#include "port.h"

/*
 * Globals
 */

static ldb_mutex_t ldb_atomic_lock = LDB_MUTEX_INITIALIZER;

/*
 * Backend
 */

void
ldb_atomic__store(long *object, long desired) {
  ldb_mutex_lock(&ldb_atomic_lock);
  *object = desired;
  ldb_mutex_unlock(&ldb_atomic_lock);
}

void
ldb_atomic__store_ptr(void **object, void *desired) {
  ldb_mutex_lock(&ldb_atomic_lock);
  *object = desired;
  ldb_mutex_unlock(&ldb_atomic_lock);
}

long
ldb_atomic__load(long *object) {
  long result;
  ldb_mutex_lock(&ldb_atomic_lock);
  result = *object;
  ldb_mutex_unlock(&ldb_atomic_lock);
  return result;
}

void *
ldb_atomic__load_ptr(void **object) {
  void *result;
  ldb_mutex_lock(&ldb_atomic_lock);
  result = *object;
  ldb_mutex_unlock(&ldb_atomic_lock);
  return result;
}

long
ldb_atomic__exchange(long *object, long desired) {
  long result;
  ldb_mutex_lock(&ldb_atomic_lock);
  result = *object;
  *object = desired;
  ldb_mutex_unlock(&ldb_atomic_lock);
  return result;
}

long
ldb_atomic__compare_exchange(long *object, long expected, long desired) {
  long result;
  ldb_mutex_lock(&ldb_atomic_lock);
  result = *object;
  if (*object == expected)
    *object = desired;
  ldb_mutex_unlock(&ldb_atomic_lock);
  return result;
}

long
ldb_atomic__fetch_add(long *object, long operand) {
  long result;
  ldb_mutex_lock(&ldb_atomic_lock);
  result = *object;
  *object += operand;
  ldb_mutex_unlock(&ldb_atomic_lock);
  return result;
}

#endif /* !LDB_HAVE_ATOMICS */
