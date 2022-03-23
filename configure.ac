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

AS_CASE([$host_os], [mingw*], [MINGW="$CC"], [AS_UNSET([MINGW])])

#
# Options
#

AC_ARG_ENABLE(
  coverage,
  AS_HELP_STRING([--enable-coverage],
                 [enable coverage [default=no]]),
  [enable_coverage=$enableval],
  [enable_coverage=no]
)

AC_ARG_ENABLE(
  tests,
  AS_HELP_STRING([--enable-tests],
                 [enable tests [default=no]]),
  [enable_tests=$enableval],
  [enable_tests=no]
)

#
# Global Flags
#

AX_CHECK_DEFINE([limits.h], [__GLIBC__], [
  AX_CHECK_COMPILE_FLAG([-std=c90], [CFLAGS="$CFLAGS -std=c90"], [
    AX_CHECK_COMPILE_FLAG([-std=c89], [CFLAGS="$CFLAGS -std=c89"])
  ])
  AC_DEFINE([_GNU_SOURCE])
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
# Compiler Fixes
#

# Encourage the user to build with xlc_r.
AC_COMPILE_IFELSE([
  AC_LANG_PROGRAM([[]], [[
#   if defined(__xlC__) && defined(_AIX) && !defined(_THREAD_SAFE)
      choke me
#   endif
  ]])
], [], [
  AC_MSG_WARN([Please use a thread-safe compiler invocation.])
  AC_MSG_WARN([For example, './configure CC=xlc_r'.])
])

# Hack to get OpenWatcom's owcc working.
AC_CHECK_DEFINE([__WATCOMC__], [
  grep -v '#define PACKAGE_STRING' confdefs.h > confdefs.h.tmp
  mv -f confdefs.h.tmp confdefs.h
])

#
# Feature Testing
#

has_fdatasync=no
has_pread=no
has_pthread=no

AC_MSG_CHECKING(for fdatasync support)
AC_LINK_IFELSE([
  AC_LANG_SOURCE([[
#   include <unistd.h>
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
#   include <unistd.h>
    int main(void) {
      char buf[32];
      return pread(0, buf, sizeof(buf), 0);
    }
  ]])
], [
  has_pread=yes
])
AC_MSG_RESULT([$has_pread])

AS_IF([test x"$MINGW$WASI$EMSCRIPTEN" = x''], [
  AC_CHECK_DEFINE([__WATCOMC__], [], [
    AX_PTHREAD([has_pthread=yes])
  ])
])

#
# Flags
#

ldb_save_CFLAGS="$CFLAGS"

AX_CHECK_COMPILE_FLAG([-Werror=unknown-warning-option],
                      [CFLAGS="$CFLAGS -Werror=unknown-warning-option"])
AX_CHECK_COMPILE_FLAG([-pedantic], [WARN_FLAGS="$WARN_FLAGS -pedantic"])
AX_CHECK_COMPILE_FLAG([-Wall], [WARN_FLAGS="$WARN_FLAGS -Wall"])
AX_CHECK_COMPILE_FLAG([-Wextra], [WARN_FLAGS="$WARN_FLAGS -Wextra"])
AX_CHECK_COMPILE_FLAG([-Wcast-align], [WARN_FLAGS="$WARN_FLAGS -Wcast-align"])
AX_CHECK_COMPILE_FLAG([-Wcast-align=strict],
                      [WARN_FLAGS="$WARN_FLAGS -Wcast-align=strict"])
AX_CHECK_COMPILE_FLAG([-Wconditional-uninitialized],
                      [WARN_FLAGS="$WARN_FLAGS -Wconditional-uninitialized"])
AX_CHECK_COMPILE_FLAG([-Wmissing-prototypes],
                      [WARN_FLAGS="$WARN_FLAGS -Wmissing-prototypes"])
AX_CHECK_COMPILE_FLAG([-Wno-implicit-fallthrough],
                      [WARN_FLAGS="$WARN_FLAGS -Wno-implicit-fallthrough"])
AX_CHECK_COMPILE_FLAG([-Wno-long-long],
                      [WARN_FLAGS="$WARN_FLAGS -Wno-long-long"])
AX_CHECK_COMPILE_FLAG([-Wno-overlength-strings],
                      [WARN_FLAGS="$WARN_FLAGS -Wno-overlength-strings"])
AX_CHECK_COMPILE_FLAG([-Wshadow], [WARN_FLAGS="$WARN_FLAGS -Wshadow"])
AX_CHECK_COMPILE_FLAG([-Wstrict-prototypes],
                      [WARN_FLAGS="$WARN_FLAGS -Wstrict-prototypes"])
AX_CHECK_COMPILE_FLAG([-Wundef], [WARN_FLAGS="$WARN_FLAGS -Wundef"])

CFLAGS="$ldb_save_CFLAGS"

AC_SUBST([WARN_FLAGS])

AC_CHECK_DEFINE([__WATCOMC__], [
  AX_CHECK_COMPILE_FLAG([-Wno-n201], [CFLAGS="$CFLAGS -Wno-n201"])
  AX_CHECK_COMPILE_FLAG([-Wno-n202], [CFLAGS="$CFLAGS -Wno-n202"])
])

AS_IF([test x"$enable_coverage" = x'yes'], [
  CFLAGS="$CFLAGS -O0 --coverage"
])

AS_IF([test x"$MINGW" != x''], [
  # Ensure we are redistributable on windows.
  LDFLAGS="$LDFLAGS -static-libgcc"
])

#
# Defines
#

AS_IF([test x"$has_fdatasync" = x'yes'], [
  AC_DEFINE([LDB_HAVE_FDATASYNC])
])

AS_IF([test x"$has_pread" = x'yes'], [
  AC_DEFINE([LDB_HAVE_PREAD])
])

AS_IF([test x"$has_pthread" = x'yes'], [
  AC_DEFINE([LDB_PTHREAD])
])

#
# Feature Test Macros
#

AS_IF([test x"$MINGW" != x''], [
  AC_DEFINE([_WIN32_WINNT], [0x501])
])

AS_CASE([$host_os], [solaris*], [
  AC_DEFINE([_TS_ERRNO])
])

AS_CASE([$host_os], [aix*], [
  AC_DEFINE([_THREAD_SAFE_ERRNO])
])

#
# Libraries
#

AS_IF([test x"$MINGW" != x''], [
  LIBS="$LIBS -lkernel32"
], [
  AS_IF([test x"$has_pthread" = x'yes'], [
    CC="$PTHREAD_CC"
    CFLAGS="$CFLAGS $PTHREAD_CFLAGS"
    LIBS="$LIBS $PTHREAD_LIBS"
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

AM_CONDITIONAL([ENABLE_SHARED], [test x"$enable_shared" = x'yes'])
AM_CONDITIONAL([ENABLE_TESTS], [test x"$enable_tests" = x'yes'])
AM_CONDITIONAL([HAVE_PTHREAD], [test x"$has_pthread" = x'yes'])
AM_CONDITIONAL([MINGW], [test x"$MINGW" != x''])

AC_CONFIG_FILES([Makefile test/Makefile lcdb.pc])

#
# Output
#

AC_OUTPUT

AC_MSG_NOTICE([Build Options:

  coverage   = $enable_coverage
  emscripten = $EMSCRIPTEN
  fdatasync  = $has_fdatasync
  pread      = $has_pread
  pthread    = $has_pthread
  tests      = $enable_tests
  wasi       = $WASI

  PREFIX     = $prefix
  HOST       = $host
  CC         = $CC
  CPPFLAGS   = $CPPFLAGS
  CFLAGS     = $CFLAGS
  WARN_FLAGS = $WARN_FLAGS
  LDFLAGS    = $LDFLAGS
  DEFS       = $DEFS
  LIBS       = $LIBS
])
