/*!
 * options.h - options for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_OPTIONS_H
#define RDB_OPTIONS_H

#include <stddef.h>

/*
 * Types
 */

struct rdb_bloom_s;
struct rdb_comparator_s;
struct rdb_lru_s;
struct rdb_snapshot_s;

/*
 * Constants
 */

/* DB contents are stored in a set of blocks, each of which holds a
 * sequence of key,value pairs.  Each block may be compressed before
 * being stored in a file.  The following enum describes which
 * compression method (if any) is used to compress a block.
 */
enum rdb_compression {
  /* NOTE: do not change the values of existing entries, as these are
     part of the persistent format on disk. */
  RDB_NO_COMPRESSION = 0x0,
  RDB_SNAPPY_COMPRESSION = 0x1
};

/*
 * DB Options
 */

/* Options to control the behavior of a database (passed to DB::Open) */
typedef struct rdb_dbopt_s {
  /* Parameters that affect behavior */

  /* Comparator used to define the order of keys in the table.
   * Default: a comparator that uses lexicographic byte-wise ordering
   *
   * REQUIRES: The client must ensure that the comparator supplied
   * here has the same name and orders keys *exactly* the same as the
   * comparator provided to previous open calls on the same DB.
   */
  const struct rdb_comparator_s *comparator;

  /* If true, the database will be created if it is missing. */
  int create_if_missing; /* 0 */

  /* If true, an error is raised if the database already exists. */
  int error_if_exists; /* 0 */

  /* If true, the implementation will do aggressive checking of the
   * data it is processing and will stop early if it detects any
   * errors.  This may have unforeseen ramifications: for example, a
   * corruption of one DB entry may cause a large number of entries to
   * become unreadable or for the entire DB to become unopenable.
   */
  int paranoid_checks; /* 0 */

  /* Parameters that affect performance */

  /* Amount of data to build up in memory (backed by an unsorted log
   * on disk) before converting to a sorted on-disk file.
   *
   * Larger values increase performance, especially during bulk loads.
   * Up to two write buffers may be held in memory at the same time,
   * so you may wish to adjust this parameter to control memory usage.
   * Also, a larger write buffer will result in a longer recovery time
   * the next time the database is opened.
   */
  size_t write_buffer_size; /* 4 * 1024 * 1024 */

  /* Number of open files that can be used by the DB.  You may need to
   * increase this if your database has a large working set (budget
   * one open file per 2MB of working set).
   */
  int max_open_files; /* 1000 */

  /* Control over blocks (user data is stored in a set of blocks, and
     a block is the unit of reading from disk). */

  /* If non-null, use the specified cache for blocks. */
  /* If null, leveldb will automatically create and use an 8MB internal cache. */
  struct rdb_lru_s *block_cache; /* NULL */

  /* Approximate size of user data packed per block.  Note that the
   * block size specified here corresponds to uncompressed data.  The
   * actual size of the unit read from disk may be smaller if
   * compression is enabled.  This parameter can be changed dynamically.
   */
  size_t block_size; /* 4 * 1024 */

  /* Number of keys between restart points for delta encoding of keys.
   * This parameter can be changed dynamically.  Most clients should
   * leave this parameter alone.
   */
  int block_restart_interval; /* 16 */

  /* Leveldb will write up to this amount of bytes to a file before
   * switching to a new one.
   * Most clients should leave this parameter alone.  However if your
   * filesystem is more efficient with larger files, you could
   * consider increasing the value.  The downside will be longer
   * compactions and hence longer latency/performance hiccups.
   * Another reason to increase this parameter might be when you are
   * initially populating a large database.
   */
  size_t max_file_size; /* 2 * 1024 * 1024 */

  /* Compress blocks using the specified compression algorithm.  This
   * parameter can be changed dynamically.
   *
   * Default: RDB_NO_COMPRESSION, which gives no compression.
   *
   * Typical speeds of RDB_SNAPPY_COMPRESSION on an Intel(R) Core(TM)2 2.4GHz:
   *    ~200-500MB/s compression
   *    ~400-800MB/s decompression
   * Note that these speeds are significantly faster than most
   * persistent storage speeds, and therefore it is typically never
   * worth switching to kNoCompression.  Even if the input data is
   * incompressible, the RDB_NO_COMPRESSION implementation will
   * efficiently detect that and will switch to uncompressed mode.
   */
  enum rdb_compression compression; /* RDB_NO_COMPRESSION */

  /* EXPERIMENTAL: If true, append to existing MANIFEST and log files
   * when a database is opened.  This can significantly speed up open.
   *
   * Default: currently false, but may become true later.
   */
  int reuse_logs; /* 0 */

  /* If non-null, use the specified filter policy to reduce disk reads.
   * Many applications will benefit from passing the result of
   * NewBloomFilterPolicy() here.
   */
  const struct rdb_bloom_s *filter_policy; /* NULL */
} rdb_dbopt_t;

/*
 * Read Options
 */

/* Options that control read operations */
typedef struct rdb_readopt_s {
  /* If true, all data read from underlying storage will be
   * verified against corresponding checksums.
   */
  int verify_checksums; /* 0 */

  /* Should the data read for this iteration be cached in memory?
   * Callers may wish to set this field to false for bulk scans.
   */
  int fill_cache; /* 1 */

  /* If "snapshot" is non-null, read as of the supplied snapshot
   * (which must belong to the DB that is being read and which must
   * not have been released).  If "snapshot" is null, use an implicit
   * snapshot of the state at the beginning of this read operation.
   */
  const struct rdb_snapshot_s *snapshot; /* NULL */
} rdb_readopt_t;

/*
 * Write Options
 */

/* Options that control write operations */
typedef struct rdb_writeopt_s {
  /* If true, the write will be flushed from the operating system
   * buffer cache (by calling WritableFile::Sync()) before the write
   * is considered complete.  If this flag is true, writes will be
   * slower.
   *
   * If this flag is false, and the machine crashes, some recent
   * writes may be lost.  Note that if it is just the process that
   * crashes (i.e., the machine does not reboot), no writes will be
   * lost even if sync==0.
   *
   * In other words, a DB write with sync==0 has similar
   * crash semantics as the "write()" system call.  A DB write
   * with sync==1 has similar crash semantics to a "write()"
   * system call followed by "fsync()".
   */
  int sync; /* 0 */
} rdb_writeopt_t;

/*
 * Globals
 */

/* RDB_EXTERN */ extern const rdb_dbopt_t *rdb_dbopt_default;
/* RDB_EXTERN */ extern const rdb_readopt_t *rdb_readopt_default;
/* RDB_EXTERN */ extern const rdb_writeopt_t *rdb_writeopt_default;
/* RDB_EXTERN */ extern const rdb_readopt_t *rdb_iteropt_default;

#endif /* RDB_OPTIONS_H */
