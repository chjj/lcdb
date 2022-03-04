/*!
 * histogram.h - histogram for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef HISTOGRAM_H
#define HISTOGRAM_H

/*
 * Constants
 */

#define NUM_BUCKETS 154
#define MAX_HISTOGRAM (232 + (NUM_BUCKETS * 127) + 1)

/*
 * Types
 */

typedef struct histogram_s {
  double min;
  double max;
  double num;
  double sum;
  double sum_squares;
  double buckets[NUM_BUCKETS];
} histogram_t;

/*
 * Histogram
 */

void
histogram_init(histogram_t *h);

void
histogram_add(histogram_t *h, double value);

void
histogram_merge(histogram_t *z, const histogram_t *x);

char *
histogram_string(const histogram_t *h, char *buf);

#endif /* HISTOGRAM_H */
