/*!
 * dbformat.c - db format for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

#include "util/bloom.h"
#include "util/buffer.h"
#include "util/coding.h"
#include "util/comparator.h"
#include "util/internal.h"
#include "util/slice.h"

#include "dbformat.h"

/*
 * Helpers
 */

static uint64_t
pack_seqtype(uint64_t sequence, rdb_valtype_t type) {
  assert(sequence <= RDB_MAX_SEQUENCE);
  assert(type <= RDB_VALTYPE_SEEK);
  return (sequence << 8) | type;
}

rdb_slice_t
rdb_extract_user_key(const rdb_slice_t *key) {
  rdb_slice_t ret;

  assert(key->size >= 8);

  rdb_slice_set(&ret, key->data, key->size - 8);

  return ret;
}

/*
 * ParsedInternalKey
 */

/* ParsedInternalKey() */
void
rdb_pkey_init(rdb_pkey_t *key,
              const rdb_slice_t *user_key,
              rdb_seqnum_t sequence,
              rdb_valtype_t type) {
  key->user_key = *user_key;
  key->sequence = sequence;
  key->type = type;
}

/* InternalKeyEncodingLength */
size_t
rdb_pkey_size(const rdb_pkey_t *x) {
  return x->user_key.size + 8;
}

uint8_t *
rdb_pkey_write(uint8_t *zp, const rdb_pkey_t *x) {
  zp = rdb_raw_write(zp, x->user_key.data, x->user_key.size);
  zp = rdb_fixed64_write(zp, pack_seqtype(x->sequence, x->type));
  return zp;
}

/* AppendInternalKey */
void
rdb_pkey_export(rdb_buffer_t *z, const rdb_pkey_t *x) {
  uint8_t *zp = rdb_buffer_expand(z, x->user_key.size + 8);
  size_t xn = rdb_pkey_write(zp, x) - zp;

  z->size += xn;
}

int
rdb_pkey_read(rdb_pkey_t *z, const uint8_t **xp, size_t *xn) {
  size_t zn = *xn - 8;
  const uint8_t *zp;
  uint64_t num;
  int type;

  if (*xn < 8)
    return 0;

  if (!rdb_zraw_read(&zp, zn, xp, xn))
    return 0;

  if (!rdb_fixed64_read(&num, xp, xn))
    return 0;

  type = num & 0xff;

  if (type > RDB_TYPE_VALUE)
    return 0;

  rdb_slice_set(&z->user_key, zp, zn);

  z->sequence = num >> 8;
  z->type = (rdb_valtype_t)type;

  return 1;
}

int
rdb_pkey_slurp(rdb_pkey_t *z, rdb_slice_t *x) {
  return rdb_pkey_read(z, (const uint8_t **)&x->data, &x->size);
}

/* ParseInternalKey */
int
rdb_pkey_import(rdb_pkey_t *z, const rdb_slice_t *x) {
  rdb_slice_t tmp = *x;
  return rdb_pkey_slurp(z, &tmp);
}

/*
 * InternalKey
 */

/* InternalKey() */
void
rdb_ikey_init(rdb_ikey_t *ikey,
              const rdb_slice_t *user_key,
              rdb_seqnum_t sequence,
              rdb_valtype_t type) {
  rdb_pkey_t pkey;

  rdb_pkey_init(&pkey, user_key, sequence, type);

  rdb_buffer_init(ikey);
  rdb_pkey_export(ikey, &pkey);
}

/* ~InternalKey() */
void
rdb_ikey_clear(rdb_ikey_t *ikey) {
  rdb_buffer_clear(ikey);
}

/* InternalKey::Clear */
void
rdb_ikey_reset(rdb_ikey_t *ikey) {
  rdb_buffer_reset(ikey);
}

/* InternalKey::SetFrom */
void
rdb_ikey_set(rdb_ikey_t *ikey, const rdb_pkey_t *pkey) {
  rdb_buffer_reset(ikey);
  rdb_pkey_export(ikey, pkey);
}

/* InternalKey::user_key */
rdb_slice_t
rdb_ikey_user_key(const rdb_ikey_t *ikey) {
  return rdb_extract_user_key(ikey);
}

/* InternalKey::Encode */
rdb_slice_t
rdb_ikey_encode(const rdb_ikey_t *x) {
  return *x;
}

/* InternalKey::DecodeFrom */
int
rdb_ikey_import(rdb_ikey_t *z, const rdb_slice_t *x) {
  rdb_buffer_copy(z, x);
  return z->size != 0;
}

/*
 * LookupKey
 */

/* LookupKey() */
void
rdb_lkey_init(rdb_lkey_t *lkey,
              const rdb_slice_t *user_key,
              rdb_seqnum_t sequence) {
  size_t usize = user_key->size;
  size_t needed = usize + 13; /* A conservative estimate. */
  uint8_t *zp;

  if (needed <= sizeof(lkey->space)) {
    zp = lkey->space;
  } else {
    zp = rdb_malloc(needed);
  }

  lkey->start = zp;

  zp = rdb_varint32_write(zp, usize + 8);

  lkey->kstart = zp;

  zp = rdb_raw_write(zp, user_key->data, usize);
  zp = rdb_fixed64_write(zp, pack_seqtype(sequence, RDB_VALTYPE_SEEK));

  lkey->end = zp;
}

/* ~LookupKey() */
void
rdb_lkey_clear(rdb_lkey_t *lkey) {
  if (lkey->start != lkey->space)
    rdb_free((void *)lkey->start);
}

/* LookupKey::memtable_key() */
/* Return a key suitable for lookup in a MemTable. */
rdb_slice_t
rdb_lkey_memtable_key(const rdb_lkey_t *lkey) {
  rdb_slice_t ret;
  rdb_slice_set(&ret, lkey->start, lkey->end - lkey->start);
  return ret;
}

/* LookupKey::internal_key() */
/* Return an internal key (suitable for passing to an internal iterator) */
rdb_slice_t
rdb_lkey_internal_key(const rdb_lkey_t *lkey) {
  rdb_slice_t ret;
  rdb_slice_set(&ret, lkey->kstart, lkey->end - lkey->kstart);
  return ret;
}

/* LookupKey::user_key() */
/* Return the user key */
rdb_slice_t
rdb_lkey_user_key(const rdb_lkey_t *lkey) {
  rdb_slice_t ret;
  rdb_slice_set(&ret, lkey->kstart, lkey->end - lkey->kstart - 8);
  return ret;
}

/*
 * InternalKeyComparator
 */

static int
rdb_ikc_compare(const rdb_comparator_t *ikc,
                const rdb_slice_t *x,
                const rdb_slice_t *y) {
  /* Order by:
   *    increasing user key (according to user-supplied comparator)
   *    decreasing sequence number
   *    decreasing type (though sequence# should be enough to disambiguate)
   */
  rdb_slice_t xk = rdb_extract_user_key(x);
  rdb_slice_t yk = rdb_extract_user_key(y);
  int r = rdb_compare(ikc->user_comparator, &xk, &yk);

  if (r == 0) {
    uint64_t xn = rdb_fixed64_decode(x->data + x->size - 8);
    uint64_t yn = rdb_fixed64_decode(y->data + y->size - 8);

    if (xn > yn) {
      r = -1;
    } else if (xn < yn) {
      r = +1;
    }
  }

  return r;
}

static void
rdb_ikc_shortest_separator(const rdb_comparator_t *ikc,
                           rdb_buffer_t *start,
                           const rdb_slice_t *limit) {
  /* Attempt to shorten the user portion of the key. */
  const rdb_comparator_t *uc = ikc->user_comparator;
  rdb_slice_t user_start = rdb_extract_user_key(start);
  rdb_slice_t user_limit = rdb_extract_user_key(limit);
  rdb_buffer_t tmp;

  rdb_buffer_init(&tmp);
  rdb_buffer_copy(&tmp, &user_start);

  rdb_shortest_separator(uc, &tmp, &user_limit);

  if (tmp.size < user_start.size && rdb_compare(uc, &user_start, &tmp) < 0) {
    /* User key has become shorter physically, but larger logically. */
    /* Tack on the earliest possible number to the shortened user key. */
    rdb_buffer_fixed64(&tmp, pack_seqtype(RDB_MAX_SEQUENCE, RDB_VALTYPE_SEEK));

    assert(rdb_compare(ikc, start, &tmp) < 0);
    assert(rdb_compare(ikc, &tmp, limit) < 0);

    rdb_buffer_swap(start, &tmp);
  }

  rdb_buffer_clear(&tmp);
}

static void
rdb_ikc_short_successor(const rdb_comparator_t *ikc, rdb_buffer_t *key) {
  const rdb_comparator_t *uc = ikc->user_comparator;
  rdb_slice_t user_key = rdb_extract_user_key(key);
  rdb_buffer_t tmp;

  rdb_buffer_init(&tmp);
  rdb_buffer_copy(&tmp, &user_key);

  rdb_short_successor(uc, &tmp);

  if (tmp.size < user_key.size && rdb_compare(uc, &user_key, &tmp) < 0) {
    /* User key has become shorter physically, but larger logically. */
    /* Tack on the earliest possible number to the shortened user key. */
    rdb_buffer_fixed64(&tmp, pack_seqtype(RDB_MAX_SEQUENCE, RDB_VALTYPE_SEEK));

    assert(rdb_compare(ikc, key, &tmp) < 0);

    rdb_buffer_swap(key, &tmp);
  }

  rdb_buffer_clear(&tmp);
}

void
rdb_ikc_init(rdb_comparator_t *ikc, const rdb_comparator_t *user_comparator) {
  ikc->name = "leveldb.InternalKeyComparator";
  ikc->compare = rdb_ikc_compare;
  ikc->shortest_separator = rdb_ikc_shortest_separator;
  ikc->short_successor = rdb_ikc_short_successor;
  ikc->user_comparator = user_comparator;
}

/*
 * InternalFilterPolicy
 */

static void
rdb_ifp_add(const rdb_bloom_t *ifp,
            uint8_t *data,
            const rdb_slice_t *key,
            size_t bits) {
  rdb_slice_t k = rdb_extract_user_key(key);

  rdb_bloom_add(ifp->user_policy, data, &k, bits);
}

static int
rdb_ifp_match(const rdb_bloom_t *ifp,
              const rdb_slice_t *filter,
              const rdb_slice_t *key) {
  rdb_slice_t k = rdb_extract_user_key(key);
  return rdb_bloom_match(ifp->user_policy, filter, &k);
}

void
rdb_ifp_init(rdb_bloom_t *ifp, const rdb_bloom_t *user_policy) {
  ifp->name = user_policy->name;
  ifp->add = rdb_ifp_add;
  ifp->match = rdb_ifp_match;
  ifp->bits_per_key = 0;
  ifp->k = 0;
  ifp->user_policy = user_policy;
}
