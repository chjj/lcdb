/*!
 * logger.c - logger for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#undef HAVE_GETTID

#if defined(_WIN32)
#  include <windows.h>
#elif defined(__linux__)
#  if !defined(__NEWLIB__) && !defined(__dietlibc__)
#    include <sys/types.h>
#    include <sys/syscall.h>
#    ifdef __NR_gettid
#      define HAVE_GETTID
#    endif
#  endif
#  include <unistd.h>
#else
#  include <unistd.h>
#endif

#include "env.h"
#include "internal.h"

/*
 * Types
 */

struct rdb_logger_s {
  FILE *stream;
};

/*
 * Helpers
 */

static int
rdb_date(char *zp, int64_t x) {
  /* https://stackoverflow.com/a/42936293 */
  /* https://howardhinnant.github.io/date_algorithms.html#civil_from_days */
  int64_t xx = x / 1000000;
  int zz = (xx / 86400) + 719468;
  int era = (zz >= 0 ? zz : zz - 146096) / 146097;
  unsigned int doe = (unsigned int)(zz - era * 146097);
  unsigned int yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  int y = (int)yoe + era * 400;
  unsigned int doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  unsigned int mp = (5 * doy + 2) / 153;
  unsigned int d = doy - (153 * mp + 2) / 5 + 1;
  unsigned int m = mp < 10 ? mp + 3 : mp - 9;
  unsigned int hr = (xx / 3600) % 24;
  unsigned int min = (xx / 60) % 60;
  unsigned int sec = xx % 60;
  unsigned int usec = x % 1000000;

  y += (m <= 2);

  return sprintf(zp, "%.4u/%.2u/%.2u-%.2u:%.2u:%.2u.%.6u",
                     y, m, d, hr, min, sec, usec);
}

/*
 * Logger
 */

rdb_logger_t *
rdb_logger_create(FILE *stream) {
  rdb_logger_t *logger = rdb_malloc(sizeof(rdb_logger_t));
  logger->stream = stream;
  return logger;
}

void
rdb_logger_destroy(rdb_logger_t *logger) {
  if (logger != NULL) {
    if (logger->stream != NULL)
      fclose(logger->stream);

    rdb_free(logger);
  }
}

void
rdb_log(rdb_logger_t *logger, const char *fmt, ...) {
  unsigned long tid;
  char date[64];
  va_list ap;

  va_start(ap, fmt);

  if (logger != NULL && logger->stream != NULL) {
    rdb_date(date, rdb_now_usec());

#if defined(_WIN32)
    tid = GetCurrentThreadId();
#elif defined(HAVE_GETTID)
    tid = syscall(__NR_gettid);
#else
    tid = getpid();
#endif

    fprintf(logger->stream, "%s %lu ", date, tid);

    vfprintf(logger->stream, fmt, ap);

    fputc('\n', logger->stream);

    fflush(logger->stream);
  }

  va_end(ap);
}
