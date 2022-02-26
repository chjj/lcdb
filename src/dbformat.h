/*!
 * dbformat.h - db format for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_DBFORMAT_H
#define RDB_DBFORMAT_H

#include <stddef.h>
#include <stdint.h>

#include "util/types.h"

/*
 * Constants
 */

/* Grouping of constants. We may want to make some of these
   parameters set via options. */
#define RDB_NUM_LEVELS 7 /* kNumLevels */

/* Level-0 compaction is started when we hit this many files. */
#define RDB_L0_COMPACTION_TRIGGER 4 /* kL0_CompactionTrigger */

/* Soft limit on number of level-0 files. We slow down writes at this point. */
#define RDB_L0_SLOWDOWN_WRITES_TRIGGER 8 /* kL0_SlowdownWritesTrigger */

/* Maximum number of level-0 files. We stop writes at this point. */
#define RDB_L0_STOP_WRITES_TRIGGER 12 /* kL0_StopWritesTrigger */

/* Maximum level to which a new compacted memtable is pushed if it
   does not create overlap. We try to push to level 2 to avoid the
   relatively expensive level 0=>1 compactions and to avoid some
   expensive manifest file operations. We do not push all the way to
   the largest level since that can generate a lot of wasted disk
   space if the same key space is being repeatedly overwritten. */
#define RDB_MAX_MEM_COMPACT_LEVEL 2 /* kMaxMemCompactLevel */

/* Approximate gap in bytes between samples of data read during iteration. */
#define RDB_READ_BYTES_PERIOD 1048576 /* kReadBytesPeriod */

/* Value types encoded as the last component of internal keys.
   DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
   data structures. */
enum rdb_valtype {
  RDB_TYPE_DELETION = 0x0, /* kTypeDeletion */
  RDB_TYPE_VALUE = 0x1 /* kTypeValue */
};

/* RDB_VALTYPE_SEEK defines the rdb_valtype that should be passed when
 * constructing a rdb_pkey_t object for seeking to a particular
 * sequence number (since we sort sequence numbers in decreasing order
 * and the value type is embedded as the low 8 bits in the sequence
 * number in internal keys, we need to use the highest-numbered
 * rdb_valtype, not the lowest).
 */
#define RDB_VALTYPE_SEEK RDB_TYPE_VALUE /* kValueTypeForSeek */

/* We leave eight bits empty at the bottom so a type and sequence#
   can be packed together into 64-bits. */
#define RDB_MAX_SEQUENCE ((UINT64_C(1) << 56) - 1) /* kMaxSequenceNumber */

/*
 * Types
 */

struct rdb_bloom_s;
struct rdb_comparator_s;

typedef enum rdb_valtype rdb_valtype_t;

typedef uint64_t rdb_seqnum_t;

/* ParsedInternalKey */
typedef struct rdb_pkey_s {
  rdb_slice_t user_key;
  rdb_seqnum_t sequence;
  rdb_valtype_t type;
} rdb_pkey_t;

/* InternalKey */
typedef rdb_buffer_t rdb_ikey_t;

/* LookupKey */
typedef struct rdb_lkey_s {
  /* We construct a char array of the form:
   *
   *    klength  varint32               <-- start
   *    userkey  char[klength]          <-- kstart
   *    tag      uint64
   *                                    <-- end
   *
   * The array is a suitable MemTable key.
   * The suffix starting with "userkey" can be used as an rdb_ikey_t.
   */
  const uint8_t *start;
  const uint8_t *kstart;
  const uint8_t *end;
  uint8_t space[200]; /* Avoid allocation for short keys. */
} rdb_lkey_t;

/*
 * Helpers
 */

rdb_slice_t
rdb_extract_user_key(const rdb_slice_t *key);

/*
 * ParsedInternalKey
 */

/* ParsedInternalKey() */
void
rdb_pkey_init(rdb_pkey_t *key,
              const rdb_slice_t *user_key,
              rdb_seqnum_t sequence,
              rdb_valtype_t type);

/* InternalKeyEncodingLength */
size_t
rdb_pkey_size(const rdb_pkey_t *x);

uint8_t *
rdb_pkey_write(uint8_t *zp, const rdb_pkey_t *x);

/* AppendInternalKey */
void
rdb_pkey_export(rdb_buffer_t *z, const rdb_pkey_t *x);

int
rdb_pkey_read(rdb_pkey_t *z, const uint8_t **xp, size_t *xn);

int
rdb_pkey_slurp(rdb_pkey_t *z, rdb_slice_t *x);

/* ParseInternalKey */
int
rdb_pkey_import(rdb_pkey_t *z, const rdb_slice_t *x);

/*
 * InternalKey
 */

/* InternalKey() */
void
rdb_ikey_init(rdb_ikey_t *ikey,
              const rdb_slice_t *user_key,
              rdb_seqnum_t sequence,
              rdb_valtype_t type);

/* ~InternalKey() */
void
rdb_ikey_clear(rdb_ikey_t *ikey);

/* InternalKey::Clear */
void
rdb_ikey_reset(rdb_ikey_t *ikey);

void
rdb_ikey_copy(rdb_ikey_t *z, const rdb_ikey_t *x);

/* InternalKey::SetFrom */
void
rdb_ikey_set(rdb_ikey_t *ikey, const rdb_pkey_t *pkey);

/* InternalKey::user_key */
rdb_slice_t
rdb_ikey_user_key(const rdb_ikey_t *ikey);

void
rdb_ikey_export(rdb_ikey_t *z, const rdb_ikey_t *x);

#if 0
/* InternalKey::Encode */
rdb_slice_t
rdb_ikey_encode(const rdb_ikey_t *x);
#endif

/* See GetInternalKey in version_edit.cc. */
int
rdb_ikey_slurp(rdb_ikey_t *z, rdb_slice_t *x);

#if 0
/* InternalKey::DecodeFrom */
int
rdb_ikey_import(rdb_ikey_t *z, const rdb_slice_t *x);
#endif

/*
 * LookupKey
 */

/* LookupKey() */
void
rdb_lkey_init(rdb_lkey_t *lkey,
              const rdb_slice_t *user_key,
              rdb_seqnum_t sequence);

/* ~LookupKey() */
void
rdb_lkey_clear(rdb_lkey_t *lkey);

/* LookupKey::memtable_key() */
/* Return a key suitable for lookup in a MemTable. */
rdb_slice_t
rdb_lkey_memtable_key(const rdb_lkey_t *lkey);

/* LookupKey::internal_key() */
/* Return an internal key (suitable for passing to an internal iterator) */
rdb_slice_t
rdb_lkey_internal_key(const rdb_lkey_t *lkey);

/* LookupKey::user_key() */
/* Return the user key */
rdb_slice_t
rdb_lkey_user_key(const rdb_lkey_t *lkey);

/*
 * InternalKeyComparator
 */

void
rdb_ikc_init(struct rdb_comparator_s *ikc,
             const struct rdb_comparator_s *user_comparator);

/*
 * InternalFilterPolicy
 */

void
rdb_ifp_init(struct rdb_bloom_s *ifp, const struct rdb_bloom_s *user_policy);

#endif /* RDB_DBFORMAT_H */
