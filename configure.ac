dnl configure.ac - autoconf script for lcdb
dnl Copyright (c) 2022, Christopher Jeffrey (MIT License).
dnl https://github.com/chjj/lcdb

m4_define(LDB_PKG_VERSION, 0.0.0)
m4_define(LDB_ABI_VERSION, 0:0:0)

#
# Initialization
#

AC_PREREQ([2.64])
AC_INIT([lcdb], [m4_substr(LDB_PKG_VERSION, 0)],
        [https://github.com/chjj/lcdb/issues])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([include/lcdb_c.h])
AC_CANONICAL_HOST
AC_LANG_ASSERT([C])
AC_PROG_CC

AM_INIT_AUTOMAKE([1.11 foreign subdir-objects tar-v7 dist-zip])
AM_PROG_CC_C_O
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])
AM_SILENT_RULES([yes])

LT_PREREQ([2.2.8])
LT_INIT([static disable-shared])

echo > confdefs.h

#
# Sanity Checks
#

AC_CHECK_DEFINE([__wasi__], [
  AS_IF([test x"$WASI" = x''], [
    AC_MSG_ERROR([./configure must be run with ./scripts/wasi-configure])
  ])
], [
  AS_IF([test x"$WASI" != x''], [
    AC_MSG_WARN([WASI env var set but environment is not wasi])
    AS_UNSET([WASI])
  ])
])

AC_CHECK_DEFINE([__EMSCRIPTEN__], [
  AS_IF([test x"$EMSCRIPTEN" = x''], [
    AC_MSG_ERROR([./configure must be run with emconfigure])
  ])
], [
  AS_IF([test x"$EMSCRIPTEN" != x''], [
    AC_MSG_WARN([EMSCRIPTEN env var set but environment is not emscripten])
    AS_UNSET([EMSCRIPTEN])
  ])
])

AC_CHECK_DEFINE([__WATCOMC__], [
  AS_IF([test x"$WATCOM" = x''], [
    AC_MSG_ERROR([./configure must be run with ./scripts/watcom-configure])
  ])
], [
  AC_MSG_WARN([WATCOM env var set but environment is not watcom])
  AS_UNSET([WATCOM])
])

AS_CASE([$host_os], [mingw*], [MINGW="$CC"], [AS_UNSET([MINGW])])

#
# Options
#

AC_ARG_ENABLE(
  bench,
  AS_HELP_STRING([--enable-bench],
                 [build benchmarks [default=yes]]),
  [enable_bench=$enableval],
  [enable_bench=yes]
)

AC_ARG_ENABLE(
  coverage,
  AS_HELP_STRING([--enable-coverage],
                 [enable coverage [default=no]]),
  [enable_coverage=$enableval],
  [enable_coverage=no]
)

AC_ARG_ENABLE(
  debug,
  AS_HELP_STRING([--enable-debug],
                 [enable debug build [default=no]]),
  [enable_debug=$enableval],
  [enable_debug=no]
)

AC_ARG_ENABLE(
  fuzzer,
  AS_HELP_STRING([--enable-fuzzer],
                 [build fuzzer [default=no]]),
  [enable_fuzzer=$enableval],
  [enable_fuzzer=no]
)

AC_ARG_ENABLE(
  lwdb,
  AS_HELP_STRING([--enable-lwdb],
                 [enable lwdb [default=no]]),
  [enable_lwdb=$enableval],
  [enable_lwdb=no]
)

AC_ARG_ENABLE(
  portable,
  AS_HELP_STRING([--enable-portable],
                 [be as portable as possible [default=no]]),
  [enable_portable=$enableval],
  [enable_portable=no]
)

AC_ARG_ENABLE(
  pthread,
  AS_HELP_STRING([--enable-pthread],
                 [use pthread [default=yes]]),
  [enable_pthread=$enableval],
  [enable_pthread=yes]
)

AC_ARG_ENABLE(
  tests,
  AS_HELP_STRING([--enable-tests],
                 [enable tests [default=yes]]),
  [enable_tests=$enableval],
  [enable_tests=yes]
)

#
# Global Flags
#

AX_CHECK_DEFINE([limits.h], [__GLIBC__], [
  AX_CHECK_COMPILE_FLAG([-std=c90], [CFLAGS="$CFLAGS -std=c90"], [
    AX_CHECK_COMPILE_FLAG([-std=c89], [CFLAGS="$CFLAGS -std=c89"])
  ])
])

AC_COMPILE_IFELSE([
  AC_LANG_PROGRAM([[]], [[
#   if !defined(__GNUC__) || __GNUC__ < 4
      choke me
#   endif
  ]])
], [
  AX_CHECK_COMPILE_FLAG([-fvisibility=hidden],
                        [CFLAGS="$CFLAGS -fvisibility=hidden"])
])

AS_CASE([$host_os], [darwin*], [
  AX_CHECK_COMPILE_FLAG([-mmacosx-version-min=10.7],
                        [CFLAGS="$CFLAGS -mmacosx-version-min=10.7"])
])

#
# Feature Test Macros
#

AS_IF([test x"$MINGW" != x''], [
  AC_DEFINE([_WIN32_WINNT], [0x501])
])

AX_CHECK_DEFINE([limits.h], [__GLIBC__], [
  AC_DEFINE([_GNU_SOURCE])
])

AS_CASE([$host_os], [solaris*], [
  AC_DEFINE([_TS_ERRNO])
])

AS_CASE([$host_os], [aix*], [
  AC_DEFINE([_THREAD_SAFE_ERRNO])
])

#
# System Libraries
#

AS_IF([test x"$MINGW" != x''], [
  LIBS="$LIBS -lkernel32"
], [
  AS_IF([test x"$enable_pthread$WATCOM$WASI$EMSCRIPTEN" = x'yes'], [
    AX_PTHREAD([
      CC="$PTHREAD_CC"
      CFLAGS="$CFLAGS $PTHREAD_CFLAGS"
      LIBS="$PTHREAD_LIBS $LIBS"

      AC_DEFINE([LDB_PTHREAD])
    ])
  ])
])

#
# Feature Testing
#

has_fdatasync=no
has_pread=no

AS_IF([test x"$enable_portable" != x'yes'], [
  AC_MSG_CHECKING(for fdatasync support)
  AC_LINK_IFELSE([
    AC_LANG_SOURCE([[
#     include <unistd.h>
      int main(void) {
        return fdatasync(1);
      }
    ]])
  ], [
    has_fdatasync=yes
  ])
  AC_MSG_RESULT([$has_fdatasync])

  AC_MSG_CHECKING(for pread support)
  AC_LINK_IFELSE([
    AC_LANG_SOURCE([[
#     include <unistd.h>
      int main(void) {
        char buf[32];
        return pread(0, buf, sizeof(buf), 0);
      }
    ]])
  ], [
    has_pread=yes
  ])
  AC_MSG_RESULT([$has_pread])
])

#
# Flags
#

AX_CHECK_COMPILE_FLAG([-Werror=unknown-warning-option],
                      [CFLAGS="$CFLAGS -Werror=unknown-warning-option"])
AX_CHECK_COMPILE_FLAG([-pedantic], [CFLAGS="$CFLAGS -pedantic"])
AX_CHECK_COMPILE_FLAG([-Wall], [CFLAGS="$CFLAGS -Wall"])
AX_CHECK_COMPILE_FLAG([-Wextra], [CFLAGS="$CFLAGS -Wextra"])
AX_CHECK_COMPILE_FLAG([-Wcast-align], [CFLAGS="$CFLAGS -Wcast-align"])
AX_CHECK_COMPILE_FLAG([-Wcast-align=strict],
                      [CFLAGS="$CFLAGS -Wcast-align=strict"])
AX_CHECK_COMPILE_FLAG([-Wconditional-uninitialized],
                      [CFLAGS="$CFLAGS -Wconditional-uninitialized"])
AX_CHECK_COMPILE_FLAG([-Wmissing-prototypes],
                      [CFLAGS="$CFLAGS -Wmissing-prototypes"])
AX_CHECK_COMPILE_FLAG([-Wno-implicit-fallthrough],
                      [CFLAGS="$CFLAGS -Wno-implicit-fallthrough"])
AX_CHECK_COMPILE_FLAG([-Wno-long-long], [CFLAGS="$CFLAGS -Wno-long-long"])
AX_CHECK_COMPILE_FLAG([-Wno-overlength-strings],
                      [CFLAGS="$CFLAGS -Wno-overlength-strings"])
AX_CHECK_COMPILE_FLAG([-Wshadow], [CFLAGS="$CFLAGS -Wshadow"])
AX_CHECK_COMPILE_FLAG([-Wstrict-prototypes],
                      [CFLAGS="$CFLAGS -Wstrict-prototypes"])
AX_CHECK_COMPILE_FLAG([-Wundef], [CFLAGS="$CFLAGS -Wundef"])

AS_IF([test x"$WATCOM" != x''], [
  AX_CHECK_COMPILE_FLAG([-Wno-n201], [CFLAGS="$CFLAGS -Wno-n201"])
  AX_CHECK_COMPILE_FLAG([-Wno-n202], [CFLAGS="$CFLAGS -Wno-n202"])
])

AS_IF([test x"$enable_coverage" = x'yes'], [
  CFLAGS="$CFLAGS -O0 --coverage"
])

AS_IF([test x"$enable_debug" = x'yes'], [
  AX_CHECK_COMPILE_FLAG([-g], [CFLAGS="$CFLAGS -g"])
])

AS_IF([test x"$MINGW" != x''], [
  # Ensure we are redistributable on windows.
  LDFLAGS="$LDFLAGS -static-libgcc"
  # Much compatibility.
  AS_IF([test x"$enable_portable $MINGW_ARCH" = x'yes i686'], [
    CFLAGS="$CFLAGS -march=i486 -mtune=i686"
  ])
])

#
# Defines
#

AS_IF([test x"$enable_coverage" = x'yes'], [
  AC_DEFINE([LDB_COVERAGE])
])

AS_IF([test x"$enable_debug" = x'yes'], [
  AC_DEFINE([LDB_DEBUG])
])

AS_IF([test x"$has_fdatasync" = x'yes'], [
  AC_DEFINE([LDB_HAVE_FDATASYNC])
])

AS_IF([test x"$has_pread" = x'yes'], [
  AC_DEFINE([LDB_HAVE_PREAD])
])

#
# Libraries
#

AS_IF([test x"$enable_bench" = x'yes'], [
  AC_LINK_IFELSE([
    AC_LANG_SOURCE([[
#     include <math.h>
      int main(int argc, char **argv) {
        return (int)sqrt((double)argc);
      }
    ]])
  ], [], [
    AC_SUBST([MATHLIB], ['-lm'])
  ])
])

#
# Configuration
#

AC_SUBST([ABI_VERSION], [LDB_ABI_VERSION])

AS_IF([test x"$MINGW" != x''], [
  AC_SUBST([LOG_COMPILER], [wine])
])

AS_IF([test x"$WASI" != x''], [
  AC_SUBST([LOG_COMPILER], ['$(top_builddir)/scripts/wasi-run'])
])

AS_IF([test x"$EMSCRIPTEN" != x''], [
  AC_SUBST([LOG_COMPILER], [node])
])

AM_CONDITIONAL([ENABLE_BENCH], [test x"$enable_bench" = x'yes'])
AM_CONDITIONAL([ENABLE_FUZZER], [test x"$enable_fuzzer" = x'yes'])
AM_CONDITIONAL([ENABLE_LWDB], [test x"$enable_lwdb" = x'yes'])
AM_CONDITIONAL([ENABLE_SHARED], [test x"$enable_shared" = x'yes'])
AM_CONDITIONAL([ENABLE_TESTS], [test x"$enable_tests" = x'yes'])
AM_CONDITIONAL([MINGW], [test x"$MINGW" != x''])

AC_CONFIG_FILES([Makefile test/Makefile lcdb.pc])

#
# Output
#

AC_OUTPUT

AC_MSG_NOTICE([Build Options:

  coverage   = $enable_coverage
  portable   = $enable_portable
  tests      = $enable_tests

  PREFIX     = $prefix
  HOST       = $host
  CC         = $CC
  CPPFLAGS   = $CPPFLAGS
  CFLAGS     = $CFLAGS
  LDFLAGS    = $LDFLAGS
  DEFS       = $DEFS
  LIBS       = $LIBS
])
