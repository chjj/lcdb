/*!
 * log_test.c - log test for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * See LICENSE for more information.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "util/buffer.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/extern.h"
#include "util/internal.h"
#include "util/random.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/testutil.h"
#include "util/vector.h"

#include "log_format.h"
#include "log_reader.h"
#include "log_writer.h"

/*
 * Constants
 */

static const size_t offset_record_sizes[] = {
  10000, /* Two sizable records in first block. */
  10000,
  2 * LDB_BLOCK_SIZE - 1000,        /* Span three blocks. */
  1,
  13716,                            /* Consume all but two bytes of block 3. */
  LDB_BLOCK_SIZE - LDB_HEADER_SIZE, /* Consume the entirety of block 4. */
};

static const uint64_t last_record_offsets[] = {
  0,
  LDB_HEADER_SIZE + 10000,
  2 * (LDB_HEADER_SIZE + 10000),
  2 * (LDB_HEADER_SIZE + 10000)
    + (2 * LDB_BLOCK_SIZE - 1000)
    + 3 * LDB_HEADER_SIZE,
  2 * (LDB_HEADER_SIZE + 10000)
    + (2 * LDB_BLOCK_SIZE - 1000)
    + 3 * LDB_HEADER_SIZE + LDB_HEADER_SIZE + 1,
  3 * LDB_BLOCK_SIZE,
};

static const int num_offset_records = lengthof(last_record_offsets);

/*
 * LogTest
 */

typedef struct ltest_s {
  ldb_buffer_t dst;
  ldb_slice_t src;
  int status;
  ldb_reporter_t report;
  int reading;
  ldb_logwriter_t writer;
  ldb_logreader_t reader;
  ldb_buffer_t scratch;
  ldb_vector_t arena;
} ltest_t;

static void
report_corruption(ldb_reporter_t *report, size_t bytes, int status) {
  report->dropped_bytes += bytes;
  *report->status = status;
}

static void
ltest_init(ltest_t *t) {
  ldb_buffer_init(&t->dst);
  ldb_slice_init(&t->src);

  t->status = LDB_OK;

  t->report.status = &t->status;
  t->report.dropped_bytes = 0;
  t->report.corruption = &report_corruption;

  t->reading = 0;

  ldb_logwriter_init(&t->writer, NULL, 0);
  ldb_logreader_init(&t->reader, NULL, &t->report, 1, 0);

  t->writer.dst = &t->dst;
  t->reader.src = &t->src;

  ldb_buffer_init(&t->scratch);
  ldb_vector_init(&t->arena);
}

static void
ltest_clear(ltest_t *t) {
  size_t i;

  for (i = 0; i < t->arena.length; i++)
    ldb_free(t->arena.items[i]);

  ldb_vector_clear(&t->arena);
  ldb_buffer_clear(&t->scratch);
  ldb_logreader_clear(&t->reader);
  ldb_buffer_clear(&t->dst);
}

static void
ltest_reset(ltest_t *t) {
  size_t i;

  for (i = 0; i < t->arena.length; i++)
    ldb_free(t->arena.items[i]);

  ldb_vector_reset(&t->arena);
}

/* Construct a string of the specified length
   made out of the supplied partial string. */
static const char *
ltest_big_string(ltest_t *t, const char *partial, size_t n) {
  ldb_slice_t chunk = ldb_string(partial);
  ldb_buffer_t result;

  ldb_buffer_init(&result);
  ldb_buffer_grow(&result, n + chunk.size + 1);

  while (result.size < n)
    ldb_buffer_concat(&result, &chunk);

  ldb_buffer_resize(&result, n);
  ldb_buffer_push(&result, 0);

  ldb_vector_push(&t->arena, result.data);

  return (char *)result.data;
}

/* Construct a string from a number. */
static const char *
number_string(int n, char *buf) {
  sprintf(buf, "%d.", n);
  return buf;
}

/* Return a skewed potentially long string. */
static const char *
ltest_rand_string(ltest_t *t, int i, ldb_rand_t *rnd) {
  char buf[50];
  return ltest_big_string(t, number_string(i, buf), ldb_rand_skewed(rnd, 17));
}

static void
ltest_reopen_for_append(ltest_t *t) {
  ldb_logwriter_init(&t->writer, NULL, t->dst.size);
  t->writer.dst = &t->dst;
}

static void
ltest_write(ltest_t *t, const char *msg) {
  ldb_slice_t m = ldb_string(msg);

  ASSERT(!t->reading);

  ldb_logwriter_add_record(&t->writer, &m);
}

static size_t
ltest_written_bytes(const ltest_t *t) {
  return t->dst.size;
}

static const char *
ltest_read(ltest_t *t) {
  ldb_slice_t record;
  char *result;

  if (!t->reading) {
    t->reading = 1;
    t->src = t->dst;
  }

  if (!ldb_logreader_read_record(&t->reader, &record, &t->scratch))
    return "EOF";

  result = ldb_malloc(record.size + 1);

  if (record.size > 0)
    memcpy(result, record.data, record.size);

  result[record.size] = '\0';

  ldb_vector_push(&t->arena, result);

  return result;
}

static void
ltest_increment_byte(ltest_t *t, int offset, int delta) {
  t->dst.data[offset] += delta;
}

static void
ltest_set_byte(ltest_t *t, int offset, int new_byte) {
  t->dst.data[offset] = new_byte;
}

static void
ltest_shrink_size(ltest_t *t, int bytes) {
  ldb_buffer_resize(&t->dst, t->dst.size - bytes);
}

static void
ltest_fix_checksum(ltest_t *t, int header_offset, int len) {
  /* Compute crc of type/len/data. */
  uint32_t crc = ldb_crc32c_value(&t->dst.data[header_offset + 6], 1 + len);

  crc = ldb_crc32c_mask(crc);

  ldb_fixed32_write(&t->dst.data[header_offset], crc);
}

static void
ltest_force_error(ltest_t *t) {
  t->reader.error = LDB_CORRUPTION;
}

static size_t
ltest_dropped_bytes(ltest_t *t) {
  return t->report.dropped_bytes;
}

static void
ltest_write_initial_offset_log(ltest_t *t) {
  int i;

  for (i = 0; i < num_offset_records; i++) {
    size_t len = offset_record_sizes[i];
    char *record = ldb_malloc(len + 1);

    memset(record, 'a' + i, len);

    record[len] = '\0';

    ltest_write(t, record);
    ldb_free(record);
  }
}

static void
ltest_start_reading_at(ltest_t *t, uint64_t initial_offset) {
  ldb_logreader_clear(&t->reader);
  ldb_logreader_init(&t->reader, NULL, &t->report, 1, initial_offset);

  t->reader.src = &t->src;
}

static void
ltest_check_offset_past_end_returns_no_records(ltest_t *t,
                                               uint64_t offset_past_end) {
  ldb_logreader_t reader;
  ldb_slice_t record;

  ltest_write_initial_offset_log(t);

  t->reading = 1;
  t->src = t->dst;

  ldb_logreader_init(&reader, NULL, &t->report, 1,
                     ltest_written_bytes(t) + offset_past_end);

  reader.src = &t->src;

  ASSERT(!ldb_logreader_read_record(&reader, &record, &t->scratch));

  ldb_logreader_clear(&reader);
}

static void
ltest_check_initial_offset_record(ltest_t *t,
                                  uint64_t initial_offset,
                                  int offset_index) {
  ldb_logreader_t reader;
  ldb_slice_t record;

  ltest_write_initial_offset_log(t);

  t->reading = 1;
  t->src = t->dst;

  ldb_logreader_init(&reader, NULL, &t->report, 1, initial_offset);

  reader.src = &t->src;

  /* Read all records from offset_index through the last one. */
  ASSERT(offset_index < num_offset_records);

  for (; offset_index < num_offset_records; offset_index++) {
    ASSERT(ldb_logreader_read_record(&reader, &record, &t->scratch));

    ASSERT(offset_record_sizes[offset_index] == record.size);

    ASSERT(last_record_offsets[offset_index] == reader.last_offset);

    ASSERT('a' + offset_index == (int)record.data[0]);
  }

  ldb_logreader_clear(&reader);
}

/*
 * Tests
 */

static void
test_log_empty(ltest_t *t) {
  ASSERT_EQ("EOF", ltest_read(t));
}

static void
test_log_read_write(ltest_t *t) {
  ltest_write(t, "foo");
  ltest_write(t, "bar");
  ltest_write(t, "");
  ltest_write(t, "xxxx");

  ASSERT_EQ("foo", ltest_read(t));
  ASSERT_EQ("bar", ltest_read(t));
  ASSERT_EQ("", ltest_read(t));
  ASSERT_EQ("xxxx", ltest_read(t));
  ASSERT_EQ("EOF", ltest_read(t));
  ASSERT_EQ("EOF", ltest_read(t)); /* Make sure reads at eof work. */
}

static void
test_log_many_blocks(ltest_t *t) {
  char buf[50];
  int i;

  for (i = 0; i < 100000; i++)
    ltest_write(t, number_string(i, buf));

  for (i = 0; i < 100000; i++)
    ASSERT_EQ(number_string(i, buf), ltest_read(t));

  ASSERT_EQ("EOF", ltest_read(t));
}

static void
test_log_fragmentation(ltest_t *t) {
  ltest_write(t, "small");
  ltest_write(t, ltest_big_string(t, "medium", 50000));
  ltest_write(t, ltest_big_string(t, "large", 100000));

  ASSERT_EQ("small", ltest_read(t));
  ASSERT_EQ(ltest_big_string(t, "medium", 50000), ltest_read(t));
  ASSERT_EQ(ltest_big_string(t, "large", 100000), ltest_read(t));
  ASSERT_EQ("EOF", ltest_read(t));
}

static void
test_log_marginal_trailer(ltest_t *t) {
  /* Make a trailer that is exactly the same length as an empty record. */
  const int n = LDB_BLOCK_SIZE - 2 * LDB_HEADER_SIZE;

  ltest_write(t, ltest_big_string(t, "foo", n));

  ASSERT(LDB_BLOCK_SIZE - LDB_HEADER_SIZE == ltest_written_bytes(t));

  ltest_write(t, "");
  ltest_write(t, "bar");

  ASSERT_EQ(ltest_big_string(t, "foo", n), ltest_read(t));
  ASSERT_EQ("", ltest_read(t));
  ASSERT_EQ("bar", ltest_read(t));
  ASSERT_EQ("EOF", ltest_read(t));
}

static void
test_log_marginal_trailer2(ltest_t *t) {
  /* Make a trailer that is exactly the same length as an empty record. */
  const int n = LDB_BLOCK_SIZE - 2 * LDB_HEADER_SIZE;

  ltest_write(t, ltest_big_string(t, "foo", n));

  ASSERT(LDB_BLOCK_SIZE - LDB_HEADER_SIZE == ltest_written_bytes(t));

  ltest_write(t, "bar");

  ASSERT_EQ(ltest_big_string(t, "foo", n), ltest_read(t));
  ASSERT_EQ("bar", ltest_read(t));
  ASSERT_EQ("EOF", ltest_read(t));
  ASSERT(0 == ltest_dropped_bytes(t));
  ASSERT(t->status == LDB_OK);
}

static void
test_log_short_trailer(ltest_t *t) {
  const int n = LDB_BLOCK_SIZE - 2 * LDB_HEADER_SIZE + 4;

  ltest_write(t, ltest_big_string(t, "foo", n));

  ASSERT(LDB_BLOCK_SIZE - LDB_HEADER_SIZE + 4 == ltest_written_bytes(t));

  ltest_write(t, "");
  ltest_write(t, "bar");

  ASSERT_EQ(ltest_big_string(t, "foo", n), ltest_read(t));
  ASSERT_EQ("", ltest_read(t));
  ASSERT_EQ("bar", ltest_read(t));
  ASSERT_EQ("EOF", ltest_read(t));
}

static void
test_log_aligned_eof(ltest_t *t) {
  const int n = LDB_BLOCK_SIZE - 2 * LDB_HEADER_SIZE + 4;

  ltest_write(t, ltest_big_string(t, "foo", n));

  ASSERT(LDB_BLOCK_SIZE - LDB_HEADER_SIZE + 4 == ltest_written_bytes(t));
  ASSERT_EQ(ltest_big_string(t, "foo", n), ltest_read(t));
  ASSERT_EQ("EOF", ltest_read(t));
}

static void
test_log_open_for_append(ltest_t *t) {
  ltest_write(t, "hello");
  ltest_reopen_for_append(t);
  ltest_write(t, "world");

  ASSERT_EQ("hello", ltest_read(t));
  ASSERT_EQ("world", ltest_read(t));
  ASSERT_EQ("EOF", ltest_read(t));
}

static void
test_log_random_read(ltest_t *t) {
  const int N = 500;
  ldb_rand_t rnd;
  int i;

  ldb_rand_init(&rnd, 301);

  for (i = 0; i < N; i++) {
    ltest_write(t, ltest_rand_string(t, i, &rnd));
    ltest_reset(t);
  }

  ldb_rand_init(&rnd, 301);

  for (i = 0; i < N; i++) {
    ASSERT_EQ(ltest_rand_string(t, i, &rnd), ltest_read(t));
    ltest_reset(t);
  }

  ASSERT_EQ("EOF", ltest_read(t));
}

/* Tests of all the error paths in log_reader.cc follow: */

static void
test_log_read_error(ltest_t *t) {
  ltest_write(t, "foo");
  ltest_force_error(t);

  ASSERT_EQ("EOF", ltest_read(t));
  ASSERT(LDB_BLOCK_SIZE == ltest_dropped_bytes(t));
  ASSERT(t->status == LDB_CORRUPTION); /* "read error" */
}

static void
test_log_bad_record_type(ltest_t *t) {
  ltest_write(t, "foo");

  /* Type is stored in header[6]. */
  ltest_increment_byte(t, 6, 100);
  ltest_fix_checksum(t, 0, 3);

  ASSERT_EQ("EOF", ltest_read(t));
  ASSERT(3 == ltest_dropped_bytes(t));
  ASSERT(t->status == LDB_CORRUPTION); /* "unknown record type" */
}

static void
test_log_truncated_trailing_record_is_ignored(ltest_t *t) {
  ltest_write(t, "foo");
  ltest_shrink_size(t, 4); /* Drop all payload as well as a header byte. */

  ASSERT_EQ("EOF", ltest_read(t));

  /* Truncated last record is ignored, not treated as an error. */
  ASSERT(0 == ltest_dropped_bytes(t));
  ASSERT(t->status == LDB_OK);
}

static void
test_log_bad_length(ltest_t *t) {
  const int payload_size = LDB_BLOCK_SIZE - LDB_HEADER_SIZE;

  ltest_write(t, ltest_big_string(t, "bar", payload_size));
  ltest_write(t, "foo");

  /* Least significant size byte is stored in header[4]. */
  ltest_increment_byte(t, 4, 1);

  ASSERT_EQ("foo", ltest_read(t));
  ASSERT(LDB_BLOCK_SIZE == ltest_dropped_bytes(t));
  ASSERT(t->status == LDB_CORRUPTION); /* "bad record length" */
}

static void
test_log_bad_length_at_end_is_ignored(ltest_t *t) {
  ltest_write(t, "foo");
  ltest_shrink_size(t, 1);

  ASSERT_EQ("EOF", ltest_read(t));
  ASSERT(0 == ltest_dropped_bytes(t));
  ASSERT(t->status == LDB_OK);
}

static void
test_log_checksum_mismatch(ltest_t *t) {
  ltest_write(t, "foo");
  ltest_increment_byte(t, 0, 10);

  ASSERT_EQ("EOF", ltest_read(t));
  ASSERT(10 == ltest_dropped_bytes(t));
  ASSERT(t->status == LDB_CORRUPTION); /* "checksum mismatch" */
}

static void
test_log_unexpected_middle_type(ltest_t *t) {
  ltest_write(t, "foo");
  ltest_set_byte(t, 6, LDB_TYPE_MIDDLE);
  ltest_fix_checksum(t, 0, 3);

  ASSERT_EQ("EOF", ltest_read(t));
  ASSERT(3 == ltest_dropped_bytes(t));
  ASSERT(t->status == LDB_CORRUPTION); /* "missing start" */
}

static void
test_log_unexpected_last_type(ltest_t *t) {
  ltest_write(t, "foo");
  ltest_set_byte(t, 6, LDB_TYPE_LAST);
  ltest_fix_checksum(t, 0, 3);

  ASSERT_EQ("EOF", ltest_read(t));
  ASSERT(3 == ltest_dropped_bytes(t));
  ASSERT(t->status == LDB_CORRUPTION); /* "missing start" */
}

static void
test_log_unexpected_full_type(ltest_t *t) {
  ltest_write(t, "foo");
  ltest_write(t, "bar");
  ltest_set_byte(t, 6, LDB_TYPE_FIRST);
  ltest_fix_checksum(t, 0, 3);

  ASSERT_EQ("bar", ltest_read(t));
  ASSERT_EQ("EOF", ltest_read(t));
  ASSERT(3 == ltest_dropped_bytes(t));
  ASSERT(t->status == LDB_CORRUPTION); /* "partial record without end" */
}

static void
test_log_unexpected_first_type(ltest_t *t) {
  ltest_write(t, "foo");
  ltest_write(t, ltest_big_string(t, "bar", 100000));
  ltest_set_byte(t, 6, LDB_TYPE_FIRST);
  ltest_fix_checksum(t, 0, 3);

  ASSERT_EQ(ltest_big_string(t, "bar", 100000), ltest_read(t));
  ASSERT_EQ("EOF", ltest_read(t));
  ASSERT(3 == ltest_dropped_bytes(t));
  ASSERT(t->status == LDB_CORRUPTION); /* "partial record without end" */
}

static void
test_log_missing_last_is_ignored(ltest_t *t) {
  ltest_write(t, ltest_big_string(t, "bar", LDB_BLOCK_SIZE));

  /* Remove the LAST block, including header. */
  ltest_shrink_size(t, 14);

  ASSERT_EQ("EOF", ltest_read(t));
  ASSERT(t->status == LDB_OK);
  ASSERT(0 == ltest_dropped_bytes(t));
}

static void
test_log_partial_last_is_ignored(ltest_t *t) {
  ltest_write(t, ltest_big_string(t, "bar", LDB_BLOCK_SIZE));

  /* Cause a bad record length in the LAST block. */
  ltest_shrink_size(t, 1);

  ASSERT_EQ("EOF", ltest_read(t));
  ASSERT(t->status == LDB_OK);
  ASSERT(0 == ltest_dropped_bytes(t));
}

static void
test_log_skip_into_multi_record(ltest_t *t) {
  /* Consider a fragmented record:
   *
   *    first(R1), middle(R1), last(R1), first(R2)
   *
   * If initial_offset points to a record after first(R1) but before first(R2)
   * incomplete fragment errors are not actual errors, and must be suppressed
   * until a new first or full record is encountered.
   */
  ltest_write(t, ltest_big_string(t, "foo", 3 * LDB_BLOCK_SIZE));
  ltest_write(t, "correct");
  ltest_start_reading_at(t, LDB_BLOCK_SIZE);

  ASSERT_EQ("correct", ltest_read(t));
  ASSERT(t->status == LDB_OK);
  ASSERT(0 == ltest_dropped_bytes(t));
  ASSERT_EQ("EOF", ltest_read(t));
}

static void
test_log_error_joins_records(ltest_t *t) {
  /* Consider two fragmented records:
   *
   *    first(R1) last(R1) first(R2) last(R2)
   *
   * where the middle two fragments disappear. We do not want
   * first(R1),last(R2) to get joined and returned as a valid record.
   */
  size_t dropped;
  int offset;

  /* Write records that span two blocks. */
  ltest_write(t, ltest_big_string(t, "foo", LDB_BLOCK_SIZE));
  ltest_write(t, ltest_big_string(t, "bar", LDB_BLOCK_SIZE));
  ltest_write(t, "correct");

  /* Wipe the middle block. */
  for (offset = LDB_BLOCK_SIZE; offset < 2 * LDB_BLOCK_SIZE; offset++)
    ltest_set_byte(t, offset, 'x');

  ASSERT_EQ("correct", ltest_read(t));
  ASSERT_EQ("EOF", ltest_read(t));

  dropped = ltest_dropped_bytes(t);

  ASSERT(dropped <= 2 * LDB_BLOCK_SIZE + 100);
  ASSERT(dropped >= 2 * LDB_BLOCK_SIZE);
}

static void
test_log_read_start(ltest_t *t) {
  ltest_check_initial_offset_record(t, 0, 0);
}

static void
test_log_read_second_one_off(ltest_t *t) {
  ltest_check_initial_offset_record(t, 1, 1);
}

static void
test_log_read_second_ten_thousand(ltest_t *t) {
  ltest_check_initial_offset_record(t, 10000, 1);
}

static void
test_log_read_second_start(ltest_t *t) {
  ltest_check_initial_offset_record(t, 10007, 1);
}

static void
test_log_read_third_one_off(ltest_t *t) {
  ltest_check_initial_offset_record(t, 10008, 2);
}

static void
test_log_read_third_start(ltest_t *t) {
  ltest_check_initial_offset_record(t, 20014, 2);
}

static void
test_log_read_fourth_one_off(ltest_t *t) {
  ltest_check_initial_offset_record(t, 20015, 3);
}

static void
test_log_read_fourth_first_block_trailer(ltest_t *t) {
  ltest_check_initial_offset_record(t, LDB_BLOCK_SIZE - 4, 3);
}

static void
test_log_read_fourth_middle_block(ltest_t *t) {
  ltest_check_initial_offset_record(t, LDB_BLOCK_SIZE + 1, 3);
}

static void
test_log_read_fourth_last_block(ltest_t *t) {
  ltest_check_initial_offset_record(t, 2 * LDB_BLOCK_SIZE + 1, 3);
}

static void
test_log_read_fourth_start(ltest_t *t) {
  ltest_check_initial_offset_record(t,
    2 * (LDB_HEADER_SIZE + 1000) +
    (2 * LDB_BLOCK_SIZE - 1000) + 3 * LDB_HEADER_SIZE,
    3);
}

static void
test_log_read_initial_offset_into_block_padding(ltest_t *t) {
  ltest_check_initial_offset_record(t, 3 * LDB_BLOCK_SIZE - 3, 5);
}

static void
test_log_read_end(ltest_t *t) {
  ltest_check_offset_past_end_returns_no_records(t, 0);
}

static void
test_log_read_past_end(ltest_t *t) {
  ltest_check_offset_past_end_returns_no_records(t, 5);
}

/*
 * Execute
 */

LDB_EXTERN int
ldb_test_log(void);

int
ldb_test_log(void) {
  static void (*tests[])(ltest_t *) = {
    test_log_empty,
    test_log_read_write,
    test_log_many_blocks,
    test_log_fragmentation,
    test_log_marginal_trailer,
    test_log_marginal_trailer2,
    test_log_short_trailer,
    test_log_aligned_eof,
    test_log_open_for_append,
    test_log_random_read,
    test_log_read_error,
    test_log_bad_record_type,
    test_log_truncated_trailing_record_is_ignored,
    test_log_bad_length,
    test_log_bad_length_at_end_is_ignored,
    test_log_checksum_mismatch,
    test_log_unexpected_middle_type,
    test_log_unexpected_last_type,
    test_log_unexpected_full_type,
    test_log_unexpected_first_type,
    test_log_missing_last_is_ignored,
    test_log_partial_last_is_ignored,
    test_log_skip_into_multi_record,
    test_log_error_joins_records,
    test_log_read_start,
    test_log_read_second_one_off,
    test_log_read_second_ten_thousand,
    test_log_read_second_start,
    test_log_read_third_one_off,
    test_log_read_third_start,
    test_log_read_fourth_one_off,
    test_log_read_fourth_first_block_trailer,
    test_log_read_fourth_middle_block,
    test_log_read_fourth_last_block,
    test_log_read_fourth_start,
    test_log_read_initial_offset_into_block_padding,
    test_log_read_end,
    test_log_read_past_end
  };

  size_t i;

  for (i = 0; i < lengthof(tests); i++) {
    ltest_t t;

    ltest_init(&t);

    tests[i](&t);

    ltest_clear(&t);
  }

  return 0;
}
