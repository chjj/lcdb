/*!
 * atomic.c - atomics for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 */

#include "atomic.h"

#if defined(LDB_MSVC_ATOMICS)

#include <windows.h>

long
ldb_atomic__exchange(volatile long *object, long desired) {
#if defined(_M_IX86) && defined(_MSC_VER)
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
#if defined(_M_IX86) && defined(_MSC_VER)
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
#if defined(_M_IX86) && defined(_MSC_VER)
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
#if defined(MemoryBarrier) && !defined(_M_IX86)
  /* Modern MSVC. */
  MemoryBarrier();
  return *object;
#else
  return ldb_atomic__compare_exchange(object, 0, 0);
#endif
}

void
ldb_atomic__store(volatile long *object, long desired) {
#if defined(MemoryBarrier) && !defined(_M_IX86)
  /* Modern MSVC. */
  *object = desired;
  MemoryBarrier();
#else
  (void)ldb_atomic__exchange(object, desired);
#endif
}

void *
ldb_atomic__load_ptr(void *volatile *object) {
#if defined(MemoryBarrier) && !defined(_M_IX86)
  /* Modern MSVC. */
  MemoryBarrier();
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
#if defined(MemoryBarrier) && !defined(_M_IX86)
  /* Modern MSVC. */
  *object = desired;
  MemoryBarrier();
#elif defined(_WIN64)
  /* Windows XP and above. */
  (void)InterlockedExchangePointer(object, desired);
#else
  (void)ldb_atomic__exchange((volatile long *)object, (long)desired);
#endif
}

#elif defined(LDB_HAVE_ATOMICS)

int
ldb_atomic__empty(void);

int
ldb_atomic__empty(void) {
  return 0;
}

#else /* !LDB_HAVE_ATOMICS */

#include "port.h"

static ldb_mutex_t ldb_atomic_lock = LDB_MUTEX_INITIALIZER;

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
