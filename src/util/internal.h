/*!
 * internal.h - internal utils for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_INTERNAL_H
#define RDB_INTERNAL_H

#include <stddef.h>
#include "extern.h"

/*
 * Language Standard
 */

#if defined(__cplusplus)
#  define RDB_STDC_VERSION 0L
#  define RDB_CPP_VERSION (__cplusplus + 0L)
#elif defined(__STDC_VERSION__)
#  define RDB_STDC_VERSION __STDC_VERSION__
#  define RDB_CPP_VERSION 0L
#else
#  define RDB_STDC_VERSION 0L
#  define RDB_CPP_VERSION 0L
#endif

/*
 * GNUC Compat
 */

#if defined(__GNUC__) && defined(__GNUC_MINOR__) && !defined(__TINYC__) \
                                                 && !defined(__NWCC__)
#  define RDB_GNUC_PREREQ(maj, min) \
    ((__GNUC__ << 16) + __GNUC_MINOR__ >= ((maj) << 16) + (min))
#else
#  define RDB_GNUC_PREREQ(maj, min) 0
#endif

/*
 * Clang Compat
 */

#if defined(__has_builtin) && !defined(__NWCC__)
#  define RDB_HAS_BUILTIN __has_builtin
#else
#  define RDB_HAS_BUILTIN(x) 0
#endif

/*
 * Builtins
 */

#undef LIKELY
#undef UNLIKELY

#if RDB_GNUC_PREREQ(3, 0) || RDB_HAS_BUILTIN(__builtin_expect)
#  define LIKELY(x) __builtin_expect(x, 1)
#  define UNLIKELY(x) __builtin_expect(x, 0)
#else
#  define LIKELY(x) (x)
#  define UNLIKELY(x) (x)
#endif

/*
 * Static Assertions
 */

#undef STATIC_ASSERT

#if RDB_STDC_VERSION >= 201112L && !defined(__chibicc__)
#  define STATIC_ASSERT(expr) _Static_assert(expr, "check failed")
#elif RDB_CPP_VERSION >= 201703L
#  define STATIC_ASSERT(expr) static_assert(expr)
#elif RDB_CPP_VERSION >= 201103L
#  define STATIC_ASSERT(expr) static_assert(expr, "check failed")
#elif RDB_GNUC_PREREQ(2, 7) || defined(__clang__) || defined(__TINYC__)
#  define STATIC_ASSERT_2(x, y) \
     typedef char rdb__assert_ ## y[(x) ? 1 : -1] __attribute__((unused))
#  define STATIC_ASSERT_1(x, y) STATIC_ASSERT_2(x, y)
#  define STATIC_ASSERT(expr) STATIC_ASSERT_1(expr, __LINE__)
#else
#  define STATIC_ASSERT(expr) struct rdb__assert_empty
#endif

/*
 * Keywords/Attributes
 */

#undef unused

#if RDB_STDC_VERSION >= 199901L
#  define RDB_INLINE inline
#elif RDB_CPP_VERSION >= 199711L
#  define RDB_INLINE inline
#elif RDB_GNUC_PREREQ(2, 7)
#  define RDB_INLINE __inline__
#elif defined(_MSC_VER) && _MSC_VER >= 900
#  define RDB_INLINE __inline
#elif (defined(__SUNPRO_C) && __SUNPRO_C >= 0x560) \
   || (defined(__SUNPRO_CC) && __SUNPRO_CC >= 0x560)
#  define RDB_INLINE inline
#else
#  define RDB_INLINE
#endif

#if RDB_STDC_VERSION > 201710L
#  define RDB_UNUSED [[maybe_unused]]
#elif RDB_CPP_VERSION >= 201703L
#  define RDB_UNUSED [[maybe_unused]]
#elif RDB_GNUC_PREREQ(2, 7) || defined(__clang__) || defined(__TINYC__)
#  define RDB_UNUSED __attribute__((unused))
#else
#  define RDB_UNUSED
#endif

#if RDB_GNUC_PREREQ(3, 0)
#  define RDB_MALLOC __attribute__((__malloc__))
#else
#  define RDB_MALLOC
#endif

#if defined(__GNUC__) && __GNUC__ >= 2
#  define RDB_EXTENSION __extension__
#else
#  define RDB_EXTENSION
#endif

#define RDB_STATIC RDB_UNUSED static RDB_INLINE

/*
 * Macros
 */

#define lengthof(x) (sizeof(x) / sizeof((x)[0]))
#define RDB_MIN(x, y) ((x) < (y) ? (x) : (y))
#define RDB_MAX(x, y) ((x) > (y) ? (x) : (y))

/*
 * Helpers
 */

RDB_MALLOC void *
rdb_malloc(size_t size);

RDB_MALLOC void *
rdb_realloc(void *ptr, size_t size);

RDB_EXTERN void
rdb_free(void *ptr);

#endif /* RDB_INTERNAL_H */
