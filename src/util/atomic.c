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

#include <windows.h>

/*
 * Compat
 */

#undef USE_INLINE_ASM
#undef HAVE_VOLATILE_MS

/* With regards to the USE_INLINE_ASM check: we want to
 * make sure we're not targeting Windows 95. Unfortunately,
 * some nerds[1] figured out how to get VS 2008 working
 * for Windows 95.
 *
 * This means we have to check for VS 2010 to be absolutely
 * sure we're not targeting Windows 95.
 *
 * [1] https://msfn.org/board/topic/112283-visual-studio-2008-and-windows-9x/
 */
#if defined(_MSC_VER) && !defined(__clang__)        \
                      && !defined(__INTEL_COMPILER) \
                      && !defined(__ICL)
#  if defined(_M_IX86) && _MSC_VER < 1600 /* VS 2010 */
#    define USE_INLINE_ASM
#  endif
#  if (defined(_M_IX86) || defined(_M_X64)) && _MSC_VER >= 1400 /* VS 2005 */
#    define HAVE_VOLATILE_MS /* Assume /volatile:ms. */
#  elif defined(_M_IX86) && _MSC_VER < 1300 /* VS 2002 */
#    define HAVE_VOLATILE_MS /* Seems to work in practice. */
#  endif
#endif

/*
 * Backend
 */

long
ldb_atomic__exchange(volatile long *object, long desired) {
#ifdef USE_INLINE_ASM
  __asm {
    mov ecx, object
    mov eax, desired
    xchg [ecx], eax
  }
#else
  /* Windows 95 and above. */
  return InterlockedExchange(object, desired);
#endif
}

long
ldb_atomic__compare_exchange(volatile long *object,
                             long expected,
                             long desired) {
#ifdef USE_INLINE_ASM
  __asm {
    mov ecx, object
    mov eax, expected
    mov edx, desired
    lock cmpxchg [ecx], edx
  }
#else
  /* Windows 98 and above. */
  return InterlockedCompareExchange(object, desired, expected);
#endif
}

long
ldb_atomic__fetch_add(volatile long *object, long operand) {
#ifdef USE_INLINE_ASM
  __asm {
    mov ecx, object
    mov eax, operand
    lock xadd [ecx], eax
  }
#else
  /* Windows 98 and above. */
  return InterlockedExchangeAdd(object, operand);
#endif
}

long
ldb_atomic__load(volatile long *object) {
#ifdef HAVE_VOLATILE_MS
  return *object;
#else
  return ldb_atomic__compare_exchange(object, 0, 0);
#endif
}

void
ldb_atomic__store(volatile long *object, long desired) {
#ifdef HAVE_VOLATILE_MS
  *object = desired;
#else
  (void)ldb_atomic__exchange(object, desired);
#endif
}

void *
ldb_atomic__load_ptr(void *volatile *object) {
#if defined(HAVE_VOLATILE_MS)
  return *object;
#elif defined(_WIN64)
  /* Windows XP and above. */
  return InterlockedCompareExchangePointer(object, NULL, NULL);
#else
  return (void *)ldb_atomic__compare_exchange((volatile long *)object, 0, 0);
#endif
}

void
ldb_atomic__store_ptr(void *volatile *object, void *desired) {
#if defined(HAVE_VOLATILE_MS)
  *object = desired;
#elif defined(_WIN64)
  /* Windows XP and above. */
  (void)InterlockedExchangePointer(object, desired);
#else
  (void)ldb_atomic__exchange((volatile long *)object, (long)desired);
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

long
ldb_atomic__load(long *object) {
  long result;
  ldb_mutex_lock(&ldb_atomic_lock);
  result = *object;
  ldb_mutex_unlock(&ldb_atomic_lock);
  return result;
}

void
ldb_atomic__store(long *object, long desired) {
  ldb_mutex_lock(&ldb_atomic_lock);
  *object = desired;
  ldb_mutex_unlock(&ldb_atomic_lock);
}

void *
ldb_atomic__load_ptr(void **object) {
  void *result;
  ldb_mutex_lock(&ldb_atomic_lock);
  result = *object;
  ldb_mutex_unlock(&ldb_atomic_lock);
  return result;
}

void
ldb_atomic__store_ptr(void **object, void *desired) {
  ldb_mutex_lock(&ldb_atomic_lock);
  *object = desired;
  ldb_mutex_unlock(&ldb_atomic_lock);
}

#endif /* !LDB_HAVE_ATOMICS */
