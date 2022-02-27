/*!
 * version_set.c - version set for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "table/iterator.h"
#include "table/merger.h"
#include "table/table.h"
#include "table/two_level_iterator.h"

#include "util/buffer.h"
#include "util/coding.h"
#include "util/comparator.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/port.h"
#include "util/rbt.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/vector.h"

#include "dbformat.h"
#include "filename.h"
#include "log_format.h"
#include "log_reader.h"
#include "log_writer.h"
#include "table_cache.h"
#include "version_edit.h"
#include "version_set.h"

/*
 * Helpers
 */

static size_t
target_file_size(const rdb_dbopt_t *options) {
  return options->max_file_size;
}

/* Maximum bytes of overlaps in grandparent (i.e., level+2) before we
   stop building a single file in a level->level+1 compaction. */
static int64_t
max_grandparent_overlap_bytes(const rdb_dbopt_t *options) {
  return 10 * target_file_size(options);
}

/* Maximum number of bytes in all compacted files. We avoid expanding
   the lower level file set of a compaction if it would make the
   total compaction cover more than this many bytes. */
static int64_t
expanded_compaction_byte_size_limit(const rdb_dbopt_t *options) {
  return 25 * target_file_size(options);
}

static double
max_bytes_for_level(const rdb_dbopt_t *options, int level) {
  /* Note: the result for level zero is not really used since we set
     the level-0 compaction threshold based on number of files. */
  double result = 10. * 1048576.0;

  (void)options;

  /* Result for both level-0 and level-1. */
  while (level > 1) {
    result *= 10;
    level--;
  }

  return result;
}

static uint64_t
max_file_size_for_level(const rdb_dbopt_t *options, int level) {
  /* We could vary per level to reduce number of files? */
  (void)level;
  return target_file_size(options);
}

static int64_t
total_file_size(const rdb_vector_t *files) {
  int64_t sum = 0;
  size_t i;

  for (i = 0; i < files->length; i++) {
    const rdb_filemeta_t *f = files->items[i];

    sum += f->file_size;
  }

  return sum;
}

int
find_file(const rdb_comparator_t *icmp,
          const rdb_vector_t *files,
          const rdb_slice_t *key) {
  uint32_t left = 0;
  uint32_t right = files->length;

  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const rdb_filemeta_t *f = files->items[mid];

    if (rdb_compare(icmp, &f->largest, key) < 0) {
      /* Key at "mid.largest" is < "target". Therefore all
         files at or before "mid" are uninteresting. */
      left = mid + 1;
    } else {
      /* Key at "mid.largest" is >= "target". Therefore all files
         after "mid" are uninteresting. */
      right = mid;
    }
  }

  return right;
}

static int
after_file(const rdb_comparator_t *ucmp,
           const rdb_slice_t *user_key,
           const rdb_filemeta_t *f) {
  rdb_slice_t largest;

  /* Null user_key occurs before all keys
     and is therefore never after *f. */
  if (user_key == NULL)
    return 0;

  largest = rdb_ikey_user_key(&f->largest);

  return rdb_compare(ucmp, user_key, &largest) > 0;
}

static int
before_file(const rdb_comparator_t *ucmp,
            const rdb_slice_t *user_key,
            const rdb_filemeta_t *f) {
  rdb_slice_t smallest;

  /* Null user_key occurs after all keys
     and is therefore never before *f. */
  if (user_key == NULL)
    return 0;

  smallest = rdb_ikey_user_key(&f->smallest);

  return rdb_compare(ucmp, user_key, &smallest) < 0;
}

int
some_file_overlaps_range(const rdb_comparator_t *icmp,
                         int disjoint_sorted_files,
                         const rdb_vector_t *files,
                         const rdb_slice_t *smallest_user_key,
                         const rdb_slice_t *largest_user_key) {
  const rdb_comparator_t *ucmp = icmp->user_comparator;
  uint32_t index = 0;

  if (!disjoint_sorted_files) {
    /* Need to check against all files. */
    size_t i;

    for (i = 0; i < files->length; i++) {
      const rdb_filemeta_t *f = files->items[i];

      if (after_file(ucmp, smallest_user_key, f) ||
          before_file(ucmp, largest_user_key, f)) {
        /* No overlap. */
      } else {
        return 1; /* Overlap. */
      }
    }

    return 0;
  }

  /* Binary search over file list. */
  if (smallest_user_key != NULL) {
    /* Find the earliest possible internal key for smallest_user_key. */
    rdb_ikey_t small_key;

    rdb_ikey_init(&small_key, smallest_user_key, RDB_MAX_SEQUENCE,
                                                 RDB_VALTYPE_SEEK);

    index = find_file(icmp, files, &small_key);

    rdb_ikey_clear(&small_key);
  }

  if (index >= files->length) {
    /* Beginning of range is after all files, so no overlap. */
    return 0;
  }

  return !before_file(ucmp, largest_user_key, files->items[index]);
}

/*
 * Version::LevelFileNumIterator
 */

/* An internal iterator. For a given version/level pair, yields
   information about the files in the level. For a given entry, key()
   is the largest key that occurs in the file, and value() is an
   16-byte value containing the file number and file size, both
   encoded using rdb_fixed64_write. */
typedef struct rdb_numiter_s {
  rdb_comparator_t icmp; /* not a pointer? */
  const rdb_vector_t *flist; /* rdb_filemeta_t */
  uint32_t index;
  uint8_t value[16];
} rdb_numiter_t;

static void
rdb_numiter_init(rdb_numiter_t *iter,
                 const rdb_comparator_t *icmp,
                 const rdb_vector_t *flist) {
  iter->icmp = *icmp;
  iter->flist = flist;
  iter->index = flist->length; /* Mark as invalid. */
}

static void
rdb_numiter_clear(rdb_numiter_t *iter) {
  (void)iter; /* nothing?? */
}

static int
rdb_numiter_valid(const rdb_numiter_t *iter) {
  return iter->index < iter->flist->length;
}

static void
rdb_numiter_seek(rdb_numiter_t *iter, const rdb_slice_t *target) {
  iter->index = find_file(&iter->icmp, iter->flist, target);
}

static void
rdb_numiter_seek_first(rdb_numiter_t *iter) {
  iter->index = 0;
}

static void
rdb_numiter_seek_last(rdb_numiter_t *iter) {
  iter->index = iter->flist->length == 0 ? 0 : iter->flist->length - 1;
}

static void
rdb_numiter_next(rdb_numiter_t *iter) {
  assert(rdb_numiter_valid(iter));
  iter->index++;
}

static void
rdb_numiter_prev(rdb_numiter_t *iter) {
  assert(rdb_numiter_valid(iter));

  if (iter->index == 0)
    iter->index = iter->flist->length; /* Marks as invalid. */
  else
    iter->index--;
}

static rdb_slice_t
rdb_numiter_key(const rdb_numiter_t *iter) {
  assert(rdb_numiter_valid(iter));
  return ((rdb_filemeta_t *)iter->flist->items[iter->index])->largest;
}

static rdb_slice_t
rdb_numiter_value(const rdb_numiter_t *iter) {
  const rdb_filemeta_t **flist = (const rdb_filemeta_t **)iter->flist->items;
  uint8_t *value = (uint8_t *)iter->value;

  assert(rdb_numiter_valid(iter));

  rdb_fixed64_write(value + 0, flist[iter->index]->number);
  rdb_fixed64_write(value + 8, flist[iter->index]->file_size);

  return rdb_slice(value, sizeof(iter->value));
}

static int
rdb_numiter_status(const rdb_numiter_t *iter) {
  (void)iter;
  return RDB_OK;
}

RDB_ITERATOR_FUNCTIONS(rdb_numiter);

static rdb_iter_t *
rdb_numiter_create(const rdb_comparator_t *icmp, const rdb_vector_t *flist) {
  rdb_numiter_t *iter = rdb_malloc(sizeof(rdb_numiter_t));

  rdb_numiter_init(iter, icmp, flist);

  return rdb_iter_create(iter, &rdb_numiter_table);
}

static rdb_iter_t *
get_file_iterator(void *arg,
                  const rdb_readopt_t *options,
                  const rdb_slice_t *file_value) {
  rdb_tcache_t *cache = (rdb_tcache_t *)arg;

  if (file_value->size != 16) {
    /* "FileReader invoked with unexpected value" */
    return rdb_emptyiter_create(RDB_CORRUPTION);
  }

  return rdb_tcache_iterate(cache, options,
                            rdb_fixed64_decode(file_value->data + 0),
                            rdb_fixed64_decode(file_value->data + 8),
                            NULL);
}

static rdb_iter_t *
rdb_concatiter_create(const rdb_version_t *ver,
                      const rdb_readopt_t *options,
                      int level) {
  rdb_iter_t *iter = rdb_numiter_create(&ver->vset->icmp, &ver->files[level]);

  return rdb_twoiter_create(iter,
                            &get_file_iterator,
                            ver->vset->table_cache,
                            options);
}

/*
 * Saver (for Version::Get)
 */

typedef struct saver_s {
  enum {
    S_NOTFOUND,
    S_FOUND,
    S_DELETED,
    S_CORRUPT
  } state;
  const rdb_comparator_t *ucmp;
  rdb_slice_t user_key;
  rdb_buffer_t *value;
} saver_t;

static void
save_value(void *arg, const rdb_slice_t *ikey, const rdb_slice_t *v) {
  saver_t *s = (saver_t *)arg;
  rdb_pkey_t pkey;

  if (!rdb_pkey_import(&pkey, ikey)) {
    s->state = S_CORRUPT;
    return;
  }

  if (rdb_compare(s->ucmp, &pkey.user_key, &s->user_key) == 0) {
    s->state = (pkey.type == RDB_TYPE_VALUE) ? S_FOUND : S_DELETED;

    if (s->state == S_FOUND)
      rdb_buffer_set(s->value, v->data, v->size);
  }
}

/*
 * GetState (for Version::Get)
 */

typedef struct getstate_s {
  saver_t saver;
  rdb_getstats_t *stats;
  const rdb_readopt_t *options;
  rdb_slice_t ikey;
  rdb_filemeta_t *last_file_read;
  int last_file_read_level;
  rdb_vset_t *vset;
  int status;
  int found;
} getstate_t;

static int
getstate_match(void *arg, int level, rdb_filemeta_t *f) {
  getstate_t *state = (getstate_t *)arg;
  rdb_tcache_t *cache = state->vset->table_cache;

  if (state->stats->seek_file == NULL &&
      state->last_file_read != NULL) {
    /* We have had more than one seek for this read. Charge the 1st file. */
    state->stats->seek_file = state->last_file_read;
    state->stats->seek_file_level = state->last_file_read_level;
  }

  state->last_file_read = f;
  state->last_file_read_level = level;

  state->status = rdb_tcache_get(cache,
                                 state->options,
                                 f->number,
                                 f->file_size,
                                 &state->ikey,
                                 &state->saver,
                                 save_value);

  if (state->status != RDB_OK) {
    state->found = 1;
    return 0;
  }

  switch (state->saver.state) {
    case S_NOTFOUND:
      return 1; /* Keep searching in other files. */
    case S_FOUND:
      state->found = 1;
      return 0;
    case S_DELETED:
      return 0;
    case S_CORRUPT:
      state->status = RDB_CORRUPTION; /* "corrupted key for [saver.user_key]" */
      state->found = 1;
      return 0;
  }

  /* Not reached. Added to avoid false compilation warnings of
     "control reaches end of non-void function". */
  return 0;
}

/*
 * SampleState (for Version::RecordReadSample)
 */

typedef struct samplestate_s {
  rdb_getstats_t stats; /* Holds first matching file. */
  int matches;
} samplestate_t;

static int
samplestate_match(void *arg, int level, rdb_filemeta_t *f) {
  samplestate_t *state = (samplestate_t *)arg;

  state->matches++;

  if (state->matches == 1) {
    /* Remember first match. */
    state->stats.seek_file = f;
    state->stats.seek_file_level = level;
  }

  /* We can stop iterating once we have a second match. */
  return state->matches < 2;
}

/*
 * Version
 */

static void
rdb_version_init(rdb_version_t *ver, rdb_vset_t *vset) {
  int level;

  ver->vset = vset;
  ver->next = ver;
  ver->prev = ver;
  ver->refs = 0;
  ver->file_to_compact = NULL;
  ver->file_to_compact_level = 1;
  ver->compaction_score = 1;
  ver->compaction_level = 1;

  for (level = 0; level < RDB_NUM_LEVELS; level++)
    rdb_vector_init(&ver->files[level]);
}

static void
rdb_version_clear(rdb_version_t *ver) {
  size_t i;
  int level;

  assert(ver->refs == 0);

  /* Remove from linked list. */
  ver->prev->next = ver->next;
  ver->next->prev = ver->prev;

  /* Drop references to files. */
  for (level = 0; level < RDB_NUM_LEVELS; level++) {
    for (i = 0; i < ver->files[level].length; i++) {
      rdb_filemeta_t *f = ver->files[level].items[i];
      rdb_filemeta_unref(f);
    }

    rdb_vector_clear(&ver->files[level]);
  }
}

rdb_version_t *
rdb_version_create(rdb_vset_t *vset) {
  rdb_version_t *ver = rdb_malloc(sizeof(rdb_version_t));
  rdb_version_init(ver, vset);
  return ver;
}

void
rdb_version_destroy(rdb_version_t *ver) {
  rdb_version_clear(ver);
  rdb_free(ver);
}

int
rdb_version_num_files(const rdb_version_t *ver, int level) {
  return ver->files[level].length;
}

void
rdb_version_add_iterators(rdb_version_t *ver,
                          const rdb_readopt_t *options,
                          rdb_vector_t *iters) {
  rdb_tcache_t *table_cache = ver->vset->table_cache;
  int level;
  size_t i;

  /* Merge all level zero files together since they may overlap. */
  for (i = 0; i < ver->files[0].length; i++) {
    rdb_filemeta_t *item = ver->files[0].items[i];
    rdb_iter_t *iter = rdb_tcache_iterate(table_cache,
                                          options,
                                          item->number,
                                          item->file_size,
                                          NULL);

    rdb_vector_push(iters, iter);
  }

  /* For levels > 0, we can use a concatenating iterator that sequentially
     walks through the non-overlapping files in the level, opening them
     lazily. */
  for (level = 1; level < RDB_NUM_LEVELS; level++) {
    if (ver->files[level].length > 0) {
      rdb_iter_t *iter = rdb_concatiter_create(ver, options, level);

      rdb_vector_push(iters, iter);
    }
  }
}

static int
newest_first(void *x, void *y) {
  const rdb_filemeta_t *a = x;
  const rdb_filemeta_t *b = y;

  return a->number < b->number ? 1 : -1;
}

static void
rdb_version_for_each_overlapping(rdb_version_t *ver,
                                 const rdb_slice_t *user_key,
                                 const rdb_slice_t *internal_key,
                                 void *arg,
                                 int (*func)(void *, int, rdb_filemeta_t *)) {
  const rdb_comparator_t *ucmp = ver->vset->icmp.user_comparator;
  rdb_vector_t tmp;
  uint32_t i;
  int level;

  /* Search level-0 in order from newest to oldest. */
  rdb_vector_init(&tmp);
  rdb_vector_grow(&tmp, ver->files[0].length);

  for (i = 0; i < ver->files[0].length; i++) {
    rdb_filemeta_t *f = ver->files[0].items[i];
    rdb_slice_t small_key = rdb_ikey_user_key(&f->smallest);
    rdb_slice_t large_key = rdb_ikey_user_key(&f->largest);

    if (rdb_compare(ucmp, user_key, &small_key) >= 0 &&
        rdb_compare(ucmp, user_key, &large_key) <= 0) {
      rdb_vector_push(&tmp, f);
    }
  }

  if (tmp.length > 0) {
    rdb_vector_sort(&tmp, newest_first);

    for (i = 0; i < tmp.length; i++) {
      if (!func(arg, 0, tmp.items[i])) {
        rdb_vector_clear(&tmp);
        return;
      }
    }
  }

  rdb_vector_clear(&tmp);

  /* Search other levels. */
  for (level = 1; level < RDB_NUM_LEVELS; level++) {
    size_t num_files = ver->files[level].length;
    uint32_t index;

    if (num_files == 0)
      continue;

    /* Binary search to find earliest index whose largest key >= internal_key */
    index = find_file(&ver->vset->icmp, &ver->files[level], internal_key);

    if (index < num_files) {
      rdb_filemeta_t *f = ver->files[level].items[index];
      rdb_slice_t small_key = rdb_ikey_user_key(&f->smallest);

      if (rdb_compare(ucmp, user_key, &small_key) < 0) {
        /* All of "f" is past any data for user_key. */
      } else {
        if (!func(arg, level, f))
          return;
      }
    }
  }
}

int
rdb_version_get(rdb_version_t *ver,
                const rdb_readopt_t *options,
                const rdb_lkey_t *k,
                rdb_buffer_t *value,
                rdb_getstats_t *stats) {
  getstate_t state;

  stats->seek_file = NULL;
  stats->seek_file_level = -1;

  state.status = 0;
  state.found = 0;
  state.stats = stats;
  state.last_file_read = NULL;
  state.last_file_read_level = -1;

  state.options = options;
  state.ikey = rdb_lkey_internal_key(k);
  state.vset = ver->vset;

  state.saver.state = S_NOTFOUND;
  state.saver.ucmp = ver->vset->icmp.user_comparator;
  state.saver.user_key = rdb_lkey_user_key(k);
  state.saver.value = value;

  rdb_version_for_each_overlapping(ver,
                                   &state.saver.user_key,
                                   &state.ikey,
                                   &state,
                                   &getstate_match);

  return state.found ? state.status : RDB_NOTFOUND;
}

int
rdb_version_update_stats(rdb_version_t *ver, const rdb_getstats_t *stats) {
  rdb_filemeta_t *f = stats->seek_file;

  if (f != NULL) {
    f->allowed_seeks--;

    if (f->allowed_seeks <= 0 && ver->file_to_compact == NULL) {
      ver->file_to_compact = f;
      ver->file_to_compact_level = stats->seek_file_level;
      return 1;
    }
  }

  return 0;
}

int
rdb_version_record_read_sample(rdb_version_t *ver, const rdb_slice_t *ikey) {
  samplestate_t state;
  rdb_pkey_t pkey;

  if (!rdb_pkey_import(&pkey, ikey))
    return 0;

  state.stats.seek_file = NULL;
  state.stats.seek_file_level = 0;
  state.matches = 0;

  rdb_version_for_each_overlapping(ver,
                                   &pkey.user_key,
                                   ikey,
                                   &state,
                                   &samplestate_match);

  /* Must have at least two matches since we want to merge across
     files. But what if we have a single file that contains many
     overwrites and deletions? Should we have another mechanism for
     finding such files? */
  if (state.matches >= 2) {
    /* 1MB cost is about 1 seek (see comment in builder_apply). */
    return rdb_version_update_stats(ver, &state.stats);
  }

  return 0;
}

void
rdb_version_ref(rdb_version_t *ver) {
  ++ver->refs;
}

void
rdb_version_unref(rdb_version_t *ver) {
  assert(ver != &ver->vset->dummy_versions);
  assert(ver->refs >= 1);

  --ver->refs;

  if (ver->refs == 0)
    rdb_version_destroy(ver);
}

int
rdb_version_overlap_in_level(rdb_version_t *ver,
                             int level,
                             const rdb_slice_t *smallest_user_key,
                             const rdb_slice_t *largest_user_key) {
  return some_file_overlaps_range(&ver->vset->icmp,
                                  (level > 0),
                                  &ver->files[level],
                                  smallest_user_key,
                                  largest_user_key);
}

int
rdb_version_pick_level_for_memtable_output(rdb_version_t *ver,
                                           const rdb_slice_t *small_key,
                                           const rdb_slice_t *large_key) {
  int level = 0;
  int64_t sum;

  if (!rdb_version_overlap_in_level(ver, 0, small_key, large_key)) {
    /* Push to next level if there is no overlap in next level,
       and the #bytes overlapping in the level after that are limited. */
    rdb_vector_t overlaps; /* rdb_filemeta_t */
    rdb_ikey_t start, limit;

    rdb_vector_init(&overlaps);
    rdb_ikey_init(&start, small_key, RDB_MAX_SEQUENCE, RDB_VALTYPE_SEEK);
    rdb_ikey_init(&limit, large_key, 0, (rdb_valtype_t)0);

    while (level < RDB_MAX_MEM_COMPACT_LEVEL) {
      if (rdb_version_overlap_in_level(ver, level + 1, small_key, large_key))
        break;

      if (level + 2 < RDB_NUM_LEVELS) {
        /* Check that file does not overlap too many grandparent bytes. */
        rdb_version_get_overlapping_inputs(ver, level + 2,
                                           &start, &limit,
                                           &overlaps);

        sum = total_file_size(&overlaps);

        if (sum > max_grandparent_overlap_bytes(ver->vset->options))
          break;
      }

      level++;
    }

    rdb_vector_clear(&overlaps);
    rdb_ikey_clear(&start);
    rdb_ikey_clear(&limit);
  }

  return level;
}

/* Store in "*inputs" all files in "level" that overlap [begin,end]. */
void
rdb_version_get_overlapping_inputs(rdb_version_t *ver,
                                   int level,
                                   const rdb_ikey_t *begin,
                                   const rdb_ikey_t *end,
                                   rdb_vector_t *inputs) {
  const rdb_comparator_t *uc = ver->vset->icmp.user_comparator;
  rdb_slice_t user_begin, user_end;
  size_t i;

  assert(level >= 0);
  assert(level < RDB_NUM_LEVELS);

  rdb_slice_init(&user_begin);
  rdb_slice_init(&user_end);

  rdb_vector_reset(inputs);

  if (begin != NULL)
    user_begin = rdb_ikey_user_key(begin);

  if (end != NULL)
    user_end = rdb_ikey_user_key(end);

  for (i = 0; i < ver->files[level].length;) {
    rdb_filemeta_t *f = ver->files[level].items[i++];
    rdb_slice_t file_start = rdb_ikey_user_key(&f->smallest);
    rdb_slice_t file_limit = rdb_ikey_user_key(&f->largest);

    if (begin != NULL && rdb_compare(uc, &file_limit, &user_begin) < 0) {
      /* "f" is completely before specified range; skip it. */
    } else if (end != NULL && rdb_compare(uc, &file_start, &user_end) > 0) {
      /* "f" is completely after specified range; skip it. */
    } else {
      rdb_vector_push(inputs, f);

      if (level == 0) {
        /* Level-0 files may overlap each other. So check if the newly
           added file has expanded the range. If so, restart search. */
        if (begin != NULL && rdb_compare(uc, &file_start, &user_begin) < 0) {
          user_begin = file_start;
          rdb_vector_reset(inputs);
          i = 0;
        } else if (end != NULL && rdb_compare(uc, &file_limit, &user_end) > 0) {
          user_end = file_limit;
          rdb_vector_reset(inputs);
          i = 0;
        }
      }
    }
  }
}

/*
 * VersionSet::Builder
 */

/* A helper class so we can efficiently apply a whole sequence
   of edits to a particular state without creating intermediate
   versions that contain full copies of the intermediate state. */
typedef rb_tree_t file_set_t; /* void * */

typedef struct level_state_s {
  rb_tree_t deleted_files; /* uint64_t */
  file_set_t added_files; /* rdb_filemeta_t * */
} level_state_t;

typedef struct builder_s {
  rdb_vset_t *vset;
  rdb_version_t *base;
  level_state_t levels[RDB_NUM_LEVELS];
} builder_t;

static int
by_smallest_key(const rdb_comparator_t *cmp,
                const rdb_filemeta_t *f1,
                const rdb_filemeta_t *f2) {
  int r = rdb_compare(cmp, &f1->smallest, &f2->smallest);

  if (r != 0)
    return r;

  /* Break ties by file number. */
  if (f1->number == f2->number)
    return 0;

  return f1->number < f2->number ? -1 : 1;
}

static int
file_set_compare(rb_val_t x, rb_val_t y, void *arg) {
  return by_smallest_key(arg, x.p, y.p);
}

static void
file_set_destruct(rb_node_t *node) {
  rdb_filemeta_unref(node->key.p);
}

/* Initialize a builder with the files from *base and other info from *vset. */
static void
builder_init(builder_t *b, rdb_vset_t *vset, rdb_version_t *base) {
  int level;

  b->vset = vset;
  b->base = base;

  for (level = 0; level < RDB_NUM_LEVELS; level++) {
    level_state_t *state = &b->levels[level];

    /* XXX Should deleted_files be a non-unique set? */
    rb_set64_init(&state->deleted_files);
    rb_set_init(&state->added_files, file_set_compare, &b->vset->icmp);
  }

  rdb_version_ref(b->base);
}

static void
builder_clear(builder_t *b) {
  int level;

  for (level = 0; level < RDB_NUM_LEVELS; level++) {
    level_state_t *state = &b->levels[level];

    rb_set64_clear(&state->deleted_files);
    rb_set_clear(&state->added_files, file_set_destruct);
  }

  rdb_version_unref(b->base);
}

/* Apply all of the edits in *edit to the current state. */
static void
builder_apply(builder_t *b, const rdb_vedit_t *edit) {
  rdb_vset_t *v = b->vset;
  void *item;
  size_t i;

  /* Update compaction pointers. */
  for (i = 0; i < edit->compact_pointers.length; i++) {
    const ikey_entry_t *entry = edit->compact_pointers.items[i];

    rdb_buffer_copy(&v->compact_pointer[entry->level], &entry->key);
  }

  /* Delete files. */
  rb_set_iterate(&edit->deleted_files, item) {
    const file_entry_t *entry = item;
    level_state_t *state = &b->levels[entry->level];

#ifndef NDEBUG
    assert(rb_set64_put(&state->deleted_files, entry->number) == 1);
#else
    rb_set64_put(&state->deleted_files, entry->number);
#endif
  }

  /* Add new files. */
  for (i = 0; i < edit->new_files.length; i++) {
    const meta_entry_t *entry = edit->new_files.items[i];
    level_state_t *state = &b->levels[entry->level];
    rdb_filemeta_t *f = rdb_filemeta_clone(&entry->meta);

    f->refs = 1;

    /* We arrange to automatically compact this file after
     * a certain number of seeks. Let's assume:
     *
     *   (1) One seek costs 10ms
     *   (2) Writing or reading 1MB costs 10ms (100MB/s)
     *   (3) A compaction of 1MB does 25MB of IO:
     *         1MB read from this level
     *         10-12MB read from next level (boundaries may be misaligned)
     *         10-12MB written to next level
     *
     * This implies that 25 seeks cost the same as the compaction
     * of 1MB of data. I.e., one seek costs approximately the
     * same as the compaction of 40KB of data. We are a little
     * conservative and allow approximately one seek for every 16KB
     * of data before triggering a compaction.
     */
    f->allowed_seeks = (int)(f->file_size / 16384U);

    if (f->allowed_seeks < 100)
      f->allowed_seeks = 100;

    rb_set64_del(&state->deleted_files, f->number);
    rb_set_put(&state->added_files, f);
  }
}

static void
builder_maybe_add_file(builder_t *b,
                       rdb_version_t *v,
                       int level,
                       rdb_filemeta_t *f) {
  level_state_t *state = &b->levels[level];

  if (rb_set64_has(&state->deleted_files, f->number)) {
    /* File is deleted: do nothing. */
  } else {
    rdb_vector_t *files = &v->files[level];

#ifndef NDEBUG
    if (level > 0 && files->length > 0) {
      rdb_filemeta_t *item = files->items[files->length - 1];

      /* Must not overlap. */
      assert(rdb_compare(&b->vset->icmp, &item->largest, &f->smallest) < 0);
    }
#endif

    rdb_filemeta_ref(f);
    rdb_vector_push(files, f);
  }
}

/* Save the current state in *v. */
static void
builder_save_to(builder_t *b, rdb_version_t *v) {
  int level;

  for (level = 0; level < RDB_NUM_LEVELS; level++) {
    /* Merge the set of added files with the set of pre-existing files. */
    /* Drop any deleted files. Store the result in *v. */
    const rdb_vector_t *base_files = &b->base->files[level];
    const file_set_t *added_files = &b->levels[level].added_files;
    size_t i = 0;
    void *item;

    rdb_vector_grow(&v->files[level], base_files->length + added_files->size);

    rb_set_iterate(added_files, item) {
      rdb_filemeta_t *added_file = item;

      /* Add all smaller files listed in b->base. */
      /* This code assumes the base files are sorted. */
      for (; i < base_files->length; i++) {
        rdb_filemeta_t *base_file = base_files->items[i];

        if (by_smallest_key(&b->vset->icmp, base_file, added_file) >= 0)
          break;

        builder_maybe_add_file(b, v, level, base_file);
      }

      builder_maybe_add_file(b, v, level, added_file);
    }

    /* Add remaining base files. */
    for (; i < base_files->length; i++) {
      rdb_filemeta_t *base_file = base_files->items[i];

      builder_maybe_add_file(b, v, level, base_file);
    }

#ifndef NDEBUG
    /* Make sure there is no overlap in levels > 0. */
    if (level > 0) {
      for (i = 1; i < v->files[level].length; i++) {
        const rdb_filemeta_t *x = v->files[level].items[i - 1];
        const rdb_filemeta_t *y = v->files[level].items[i];

        if (rdb_compare(&b->vset->icmp, &x->largest, &y->smallest) >= 0) {
          fprintf(stderr, "overlapping ranges in same level\n");
          abort();
        }
      }
    }
#endif
  }
}

/*
 * VersionSet
 */

static void
rdb_vset_append_version(rdb_vset_t *vset, rdb_version_t *v);

static void
rdb_vset_init(rdb_vset_t *vset,
              const char *dbname,
              const rdb_dbopt_t *options,
              rdb_tcache_t *table_cache,
              const rdb_comparator_t *cmp) {
  int level;

  vset->dbname = dbname;
  vset->options = options;
  vset->table_cache = table_cache;
  vset->icmp = *cmp;
  vset->next_file_number = 2;
  vset->manifest_file_number = 0; /* Filled by recover(). */
  vset->last_sequence = 0;
  vset->log_number = 0;
  vset->prev_log_number = 0;
  vset->descriptor_file = NULL;
  vset->descriptor_log = NULL;
  vset->current = NULL;

  rdb_version_init(&vset->dummy_versions, vset);

  for (level = 0; level < RDB_NUM_LEVELS; level++)
    rdb_buffer_init(&vset->compact_pointer[level]);

  rdb_vset_append_version(vset, rdb_version_create(vset));
}

static void
rdb_vset_clear(rdb_vset_t *vset) {
  int level;

  rdb_version_unref(vset->current);

  assert(vset->dummy_versions.next == &vset->dummy_versions); /* List must be empty. */

  rdb_logwriter_destroy(vset->descriptor_log);
  rdb_wfile_destroy(vset->descriptor_file);

  for (level = 0; level < RDB_NUM_LEVELS; level++)
    rdb_buffer_clear(&vset->compact_pointer[level]);
}

rdb_vset_t *
rdb_vset_create(const char *dbname,
                const rdb_dbopt_t *options,
                rdb_tcache_t *table_cache,
                const rdb_comparator_t *cmp) {
  rdb_vset_t *vset = rdb_malloc(sizeof(rdb_vset_t));
  rdb_vset_init(vset, dbname, options, table_cache, cmp);
  return vset;
}

void
rdb_vset_destroy(rdb_vset_t *vset) {
  rdb_vset_clear(vset);
  rdb_free(vset);
}

rdb_version_t *
rdb_vset_current(const rdb_vset_t *vset) {
  return vset->current;
}

uint64_t
rdb_vset_manifest_file_number(const rdb_vset_t *vset) {
  return vset->manifest_file_number;
}

uint64_t
rdb_vset_new_file_number(rdb_vset_t *vset) {
  return vset->next_file_number++;
}

void
rdb_vset_reuse_file_number(rdb_vset_t *vset, uint64_t file_number) {
  if (vset->next_file_number == file_number + 1)
    vset->next_file_number = file_number;
}

uint64_t
rdb_vset_last_sequence(const rdb_vset_t *vset) {
  return vset->last_sequence;
}

void
rdb_vset_set_last_sequence(rdb_vset_t *vset, uint64_t s) {
  assert(s >= vset->last_sequence);
  vset->last_sequence = s;
}

uint64_t
rdb_vset_log_number(const rdb_vset_t *vset) {
  return vset->log_number;
}

uint64_t
rdb_vset_prev_log_number(const rdb_vset_t *vset) {
  return vset->prev_log_number;
}

int
rdb_vset_needs_compaction(const rdb_vset_t *vset) {
  rdb_version_t *v = vset->current;
  return (v->compaction_score >= 1) || (v->file_to_compact != NULL);
}

static void
rdb_vset_append_version(rdb_vset_t *vset, rdb_version_t *v) {
  /* Make "v" current. */
  assert(v->refs == 0);
  assert(v != vset->current);

  if (vset->current != NULL)
    rdb_version_unref(vset->current);

  vset->current = v;

  rdb_version_ref(v);

  /* Append to linked list. */
  v->prev = vset->dummy_versions.prev;
  v->next = &vset->dummy_versions;
  v->prev->next = v;
  v->next->prev = v;
}

static void
rdb_vset_finalize(rdb_vset_t *vset, rdb_version_t *v);

static int
rdb_vset_write_snapshot(rdb_vset_t *vset, rdb_logwriter_t *log);

int
rdb_vset_log_and_apply(rdb_vset_t *vset, rdb_vedit_t *edit, rdb_mutex_t *mu) {
  char fname[RDB_PATH_MAX];
  rdb_version_t *v;
  int rc = RDB_OK;

  fname[0] = '\0';

  if (edit->has_log_number) {
    assert(edit->log_number >= vset->log_number);
    assert(edit->log_number < vset->next_file_number);
  } else {
    rdb_vedit_set_log_number(edit, vset->log_number);
  }

  if (!edit->has_prev_log_number)
    rdb_vedit_set_prev_log_number(edit, vset->prev_log_number);

  rdb_vedit_set_next_file(edit, vset->next_file_number);
  rdb_vedit_set_last_sequence(edit, vset->last_sequence);

  v = rdb_version_create(vset);

  {
    builder_t b;

    builder_init(&b, vset, vset->current);
    builder_apply(&b, edit);
    builder_save_to(&b, v);
    builder_clear(&b);
  }

  rdb_vset_finalize(vset, v);

  /* Initialize new descriptor log file if necessary by creating
     a temporary file that contains a snapshot of the current version. */
  if (vset->descriptor_log == NULL) {
    /* No reason to unlock *mu here since we only hit this path in the
       first call to log_and_apply (when opening the database). */
    assert(vset->descriptor_file == NULL);

    if (rdb_desc_filename(fname, sizeof(fname), vset->dbname,
                                                vset->manifest_file_number)) {
      rc = rdb_truncfile_create(fname, &vset->descriptor_file);
    } else {
      rc = RDB_INVALID;
    }

    if (rc == RDB_OK) {
      vset->descriptor_log = rdb_logwriter_create(vset->descriptor_file, 0);

      rc = rdb_vset_write_snapshot(vset, vset->descriptor_log);
    }
  }

  /* Unlock during expensive MANIFEST log write. */
  {
    rdb_mutex_unlock(mu);

    /* Write new record to MANIFEST log. */
    if (rc == RDB_OK) {
      rdb_buffer_t record;

      rdb_buffer_init(&record);
      rdb_vedit_export(&record, edit);

      rc = rdb_logwriter_add_record(vset->descriptor_log, &record);

      if (rc == RDB_OK)
        rc = rdb_wfile_sync(vset->descriptor_file);

      rdb_buffer_clear(&record);
    }

    /* If we just created a new descriptor file, install it by writing a
       new CURRENT file that points to it. */
    if (rc == RDB_OK && fname[0])
      rc = rdb_set_current_file(vset->dbname, vset->manifest_file_number);

    rdb_mutex_lock(mu);
  }

  /* Install the new version. */
  if (rc == RDB_OK) {
    rdb_vset_append_version(vset, v);

    vset->log_number = edit->log_number;
    vset->prev_log_number = edit->prev_log_number;
  } else {
    rdb_version_destroy(v);

    if (fname[0]) {
      rdb_logwriter_destroy(vset->descriptor_log);
      rdb_wfile_destroy(vset->descriptor_file);

      vset->descriptor_log = NULL;
      vset->descriptor_file = NULL;

      rdb_remove_file(fname);
    }
  }

  return rc;
}

static int
rdb_vset_reuse_manifest(rdb_vset_t *vset, const char *dscname) {
  rdb_filetype_t manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  const char *dscbase;
  int rc;

  if (!vset->options->reuse_logs)
    return 0;

  dscbase = strrchr(dscname, '/');

  if (dscbase == NULL)
    dscbase = dscname;
  else
    dscbase += 1;

  if (!rdb_parse_filename(&manifest_type, &manifest_number, dscbase)
      || manifest_type != RDB_FILE_DESC
      || rdb_get_file_size(dscname, &manifest_size) != RDB_OK
      /* Make new compacted MANIFEST if old one is too big. */
      || manifest_size >= target_file_size(vset->options)) {
    return 0;
  }

  assert(vset->descriptor_file == NULL);
  assert(vset->descriptor_log == NULL);

  rc = rdb_appendfile_create(dscname, &vset->descriptor_file);

  if (rc != RDB_OK) {
    assert(vset->descriptor_file == NULL);
    return 0;
  }

  vset->descriptor_log = rdb_logwriter_create(vset->descriptor_file,
                                              manifest_size);

  vset->manifest_file_number = manifest_number;

  return 1;
}

static void
report_corruption(rdb_reporter_t *reporter, size_t bytes, int status) {
  (void)bytes;

  if (*reporter->status == RDB_OK)
    *reporter->status = status;
}

static int
read_current_filename(char *path, size_t size, const char *prefix) {
  rdb_buffer_t data;
  size_t len;
  char *name;
  int rc;

  if (!rdb_current_filename(path, size, prefix))
    return RDB_INVALID;

  rdb_buffer_init(&data);

  rc = rdb_read_file(path, &data);

  if (rc != RDB_OK)
    goto fail;

  name = (char *)data.data;
  len = data.size;

  if (len == 0 || name[len - 1] != '\n') {
    rc = RDB_CORRUPTION; /* "CURRENT file does not end with newline" */
    goto fail;
  }

  name[len - 1] = '\0';

  if (strlen(prefix) + len > size) {
    rc = RDB_INVALID;
    goto fail;
  }

  /* Could use rdb_path_join. */
  sprintf(path, "%s/%s", prefix, name);

fail:
  rdb_buffer_clear(&data);
  return rc;
}

static int
slice_equal(const rdb_slice_t *x, const char *y) {
  if (x->size != strlen(y))
    return 0;

  return memcmp(y, x->data, x->size) == 0;
}

int
rdb_vset_recover(rdb_vset_t *vset, int *save_manifest) {
  const rdb_comparator_t *ucmp = vset->icmp.user_comparator;
  char fname[RDB_PATH_MAX];
  int have_log_number = 0;
  int have_prev_log_number = 0;
  int have_next_file = 0;
  int have_last_sequence = 0;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  int read_records = 0;
  builder_t builder;
  rdb_rfile_t *file;
  int rc;

  /* Read "CURRENT" file, which contains a
     pointer to the current manifest file. */
  rc = read_current_filename(fname, sizeof(fname), vset->dbname);

  if (rc != RDB_OK)
    return rc;

  rc = rdb_seqfile_create(fname, &file);

  if (rc != RDB_OK) {
    if (rc == RDB_NOTFOUND)
      return RDB_CORRUPTION; /* "CURRENT points to a non-existent file" */

    return rc;
  }

  builder_init(&builder, vset, vset->current);

  {
    rdb_reporter_t reporter;
    rdb_logreader_t reader;
    rdb_slice_t record;
    rdb_buffer_t buf;
    rdb_vedit_t edit;

    reporter.status = &rc;
    reporter.corruption = report_corruption;

    rdb_logreader_init(&reader, file, &reporter, 1, 0);
    rdb_slice_init(&record);
    rdb_buffer_init(&buf);
    rdb_vedit_init(&edit);

    while (rdb_logreader_read_record(&reader, &record, &buf) && rc == RDB_OK) {
      ++read_records;

      /* Calls rdb_vedit_reset() internally. */
      rc = rdb_vedit_import(&edit, &record);

      if (rc == RDB_OK) {
        if (edit.has_comparator && !slice_equal(&edit.comparator, ucmp->name)) {
          rc = RDB_INVALID; /* "[edit.comparator] does not match
                                existing comparator [vset.user_comparator]" */
        }
      }

      if (rc == RDB_OK)
        builder_apply(&builder, &edit);

      if (edit.has_log_number) {
        log_number = edit.log_number;
        have_log_number = 1;
      }

      if (edit.has_prev_log_number) {
        prev_log_number = edit.prev_log_number;
        have_prev_log_number = 1;
      }

      if (edit.has_next_file_number) {
        next_file = edit.next_file_number;
        have_next_file = 1;
      }

      if (edit.has_last_sequence) {
        last_sequence = edit.last_sequence;
        have_last_sequence = 1;
      }
    }

    rdb_vedit_clear(&edit);
    rdb_buffer_clear(&buf);
    rdb_logreader_clear(&reader);
  }

  rdb_rfile_destroy(file);
  file = NULL;

  if (rc == RDB_OK) {
    if (!have_next_file)
      rc = RDB_CORRUPTION; /* "no meta-nextfile entry in descriptor" */
    else if (!have_log_number)
      rc = RDB_CORRUPTION; /* "no meta-lognumber entry in descriptor" */
    else if (!have_last_sequence)
      rc = RDB_CORRUPTION; /* "no last-sequence-number entry in descriptor" */

    if (!have_prev_log_number)
      prev_log_number = 0;

    rdb_vset_mark_file_number_used(vset, prev_log_number);
    rdb_vset_mark_file_number_used(vset, log_number);
  }

  if (rc == RDB_OK) {
    rdb_version_t *v = rdb_version_create(vset);

    builder_save_to(&builder, v);

    /* Install recovered version. */
    rdb_vset_finalize(vset, v);
    rdb_vset_append_version(vset, v);

    vset->manifest_file_number = next_file;
    vset->next_file_number = next_file + 1;
    vset->last_sequence = last_sequence;
    vset->log_number = log_number;
    vset->prev_log_number = prev_log_number;

    /* See if we can reuse the existing MANIFEST file. */
    if (rdb_vset_reuse_manifest(vset, fname)) {
      /* No need to save new manifest. */
    } else {
      *save_manifest = 1;
    }
  }

  builder_clear(&builder);

  return rc;
}

void
rdb_vset_mark_file_number_used(rdb_vset_t *vset, uint64_t number) {
  if (vset->next_file_number <= number)
    vset->next_file_number = number + 1;
}

static void
rdb_vset_finalize(rdb_vset_t *vset, rdb_version_t *v) {
  /* Precomputed best level for next compaction. */
  int best_level = -1;
  double best_score = -1;
  int level;

  for (level = 0; level < RDB_NUM_LEVELS - 1; level++) {
    double score;

    if (level == 0) {
      /* We treat level-0 specially by bounding the number of files
       * instead of number of bytes for two reasons:
       *
       * (1) With larger write-buffer sizes, it is nice not to do too
       * many level-0 compactions.
       *
       * (2) The files in level-0 are merged on every read and
       * therefore we wish to avoid too many files when the individual
       * file size is small (perhaps because of a small write-buffer
       * setting, or very high compression ratios, or lots of
       * overwrites/deletions).
       */
      score = v->files[level].length / (double)(RDB_L0_COMPACTION_TRIGGER);
    } else {
      /* Compute the ratio of current size to size limit. */
      uint64_t level_bytes = total_file_size(&v->files[level]);

      score = (double)level_bytes / max_bytes_for_level(vset->options, level);
    }

    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }

  v->compaction_level = best_level;
  v->compaction_score = best_score;
}

static int
rdb_vset_write_snapshot(rdb_vset_t *vset, rdb_logwriter_t *log) {
  rdb_buffer_t record;
  rdb_vedit_t edit;
  int level, rc;

  /* Save metadata. */
  rdb_vedit_init(&edit);
  rdb_vedit_set_comparator_name(&edit, vset->icmp.user_comparator->name);

  /* Save compaction pointers. */
  for (level = 0; level < RDB_NUM_LEVELS; level++) {
    if (vset->compact_pointer[level].size > 0)
      rdb_vedit_set_compact_pointer(&edit, level,
                                    &vset->compact_pointer[level]);
  }

  /* Save files. */
  for (level = 0; level < RDB_NUM_LEVELS; level++) {
    const rdb_vector_t *files = &vset->current->files[level];
    size_t i;

    for (i = 0; i < files->length; i++) {
      const rdb_filemeta_t *f = files->items[i];

      rdb_vedit_add_file(&edit, level,
                         f->number,
                         f->file_size,
                         &f->smallest,
                         &f->largest);
    }
  }

  rdb_buffer_init(&record);
  rdb_vedit_export(&record, &edit);
  rdb_vedit_clear(&edit);

  rc = rdb_logwriter_add_record(log, &record);

  rdb_buffer_clear(&record);

  return rc;
}

int
rdb_vset_num_level_files(const rdb_vset_t *vset, int level) {
  assert(level >= 0);
  assert(level < RDB_NUM_LEVELS);
  return vset->current->files[level].length;
}

uint64_t
rdb_vset_approximate_offset_of(rdb_vset_t *vset,
                               rdb_version_t *v,
                               const rdb_ikey_t *ikey) {
  uint64_t result = 0;
  int level;

  for (level = 0; level < RDB_NUM_LEVELS; level++) {
    const rdb_vector_t *files = &v->files[level];
    size_t i;

    for (i = 0; i < files->length; i++) {
      const rdb_filemeta_t *file = files->items[i];

      if (rdb_compare(&vset->icmp, &file->largest, ikey) <= 0) {
        /* Entire file is before "ikey", so just add the file size. */
        result += file->file_size;
      } else if (rdb_compare(&vset->icmp, &file->smallest, ikey) > 0) {
        /* Entire file is after "ikey", so ignore. */
        if (level > 0) {
          /* Files other than level 0 are sorted by meta->smallest, so
             no further files in this level will contain data for
             "ikey". */
          break;
        }
      } else {
        /* "ikey" falls in the range for this table. Add the
           approximate offset of "ikey" within the table. */
        rdb_table_t *tableptr;
        rdb_iter_t *iter;

        iter = rdb_tcache_iterate(vset->table_cache,
                                  rdb_readopt_default,
                                  file->number,
                                  file->file_size,
                                  &tableptr);

        if (tableptr != NULL)
          result += rdb_table_approximate_offsetof(tableptr, ikey);

        rdb_iter_destroy(iter);
      }
    }
  }

  return result;
}

void
rdb_vset_add_live_files(rdb_vset_t *vset, rb_tree_t *live) {
  rdb_version_t *list = &vset->dummy_versions;
  rdb_version_t *v;
  int level;
  size_t i;

  for (v = list->next; v != list; v = v->next) {
    for (level = 0; level < RDB_NUM_LEVELS; level++) {
      const rdb_vector_t *files = &v->files[level];

      for (i = 0; i < files->length; i++) {
        const rdb_filemeta_t *file = files->items[i];

        rb_set64_put(live, file->number);
      }
    }
  }
}

int64_t
rdb_vset_num_level_bytes(const rdb_vset_t *vset, int level) {
  assert(level >= 0);
  assert(level < RDB_NUM_LEVELS);
  return total_file_size(&vset->current->files[level]);
}

int64_t
rdb_vset_max_next_level_overlapping_bytes(rdb_vset_t *vset) {
  rdb_vector_t overlaps;
  int64_t result = 0;
  int level;
  size_t i;

  rdb_vector_init(&overlaps);

  for (level = 1; level < RDB_NUM_LEVELS - 1; level++) {
    for (i = 0; i < vset->current->files[level].length; i++) {
      const rdb_filemeta_t *f = vset->current->files[level].items[i];
      int64_t sum;

      rdb_version_get_overlapping_inputs(vset->current,
                                         level + 1,
                                         &f->smallest,
                                         &f->largest,
                                         &overlaps);

      sum = total_file_size(&overlaps);

      if (sum > result)
        result = sum;
    }
  }

  rdb_vector_clear(&overlaps);

  return result;
}

/* Stores the minimal range that covers all entries in inputs in
   *smallest, *largest. */
/* REQUIRES: inputs is not empty */
static void
rdb_vset_get_range(rdb_vset_t *vset,
                   const rdb_vector_t *inputs,
                   rdb_slice_t *smallest,
                   rdb_slice_t *largest) {
  rdb_ikey_t *small = NULL;
  rdb_ikey_t *large = NULL;
  size_t i;

  assert(inputs->length > 0);

  for (i = 0; i < inputs->length; i++) {
    rdb_filemeta_t *f = inputs->items[i];

    if (i == 0) {
      small = &f->smallest;
      large = &f->largest;
    } else {
      if (rdb_compare(&vset->icmp, &f->smallest, small) < 0)
        small = &f->smallest;

      if (rdb_compare(&vset->icmp, &f->largest, large) > 0)
        large = &f->largest;
    }
  }

  *smallest = *small;
  *largest = *large;
}

/* Stores the minimal range that covers all entries in inputs1 and inputs2
   in *smallest, *largest. */
/* REQUIRES: inputs is not empty */
static void
rdb_vset_get_range2(rdb_vset_t *vset,
                    const rdb_vector_t *inputs1,
                    const rdb_vector_t *inputs2,
                    rdb_slice_t *smallest,
                    rdb_slice_t *largest) {
  rdb_vector_t all;
  size_t i;

  rdb_vector_init(&all);
  rdb_vector_grow(&all, inputs1->length + inputs2->length);

  for (i = 0; i < inputs1->length; i++)
    all.items[all.length++] = inputs1->items[i];

  for (i = 0; i < inputs2->length; i++)
    all.items[all.length++] = inputs2->items[i];

  rdb_vset_get_range(vset, &all, smallest, largest);

  rdb_vector_clear(&all);
}

rdb_iter_t *
rdb_inputiter_create(rdb_vset_t *vset, rdb_compaction_t *c) {
  rdb_readopt_t options = *rdb_readopt_default;
  rdb_iter_t *result;
  rdb_iter_t **list;
  int num = 0;
  int space, which;
  size_t i;

  options.verify_checksums = vset->options->paranoid_checks;
  options.fill_cache = 0;

  /* Level-0 files have to be merged together. For other levels,
     we will make a concatenating iterator per level. */
  space = (rdb_compaction_level(c) == 0 ? c->inputs[0].length + 1 : 2);
  list = rdb_malloc(space * sizeof(rdb_iter_t *));

  for (which = 0; which < 2; which++) {
    if (c->inputs[which].length > 0) {
      if (rdb_compaction_level(c) + which == 0) {
        const rdb_vector_t *files = &c->inputs[which];

        for (i = 0; i < files->length; i++) {
          const rdb_filemeta_t *file = files->items[i];

          list[num++] = rdb_tcache_iterate(vset->table_cache,
                                           &options,
                                           file->number,
                                           file->file_size,
                                           NULL);
        }
      } else {
        /* Create concatenating iterator for the files from this level. */
        list[num++] = rdb_twoiter_create(rdb_numiter_create(&vset->icmp,
                                                            &c->inputs[which]),
                                         &get_file_iterator,
                                         vset->table_cache,
                                         &options);
      }
    }
  }

  assert(num <= space);

  result = rdb_mergeiter_create(&vset->icmp, list, num);

  rdb_free(list);

  return result;
}

static void
rdb_vset_setup_other_inputs(rdb_vset_t *vset, rdb_compaction_t *c);

rdb_compaction_t *
rdb_vset_pick_compaction(rdb_vset_t *vset) {
  rdb_compaction_t *c;
  int level;
  size_t i;

  /* We prefer compactions triggered by too much data in a level over
     the compactions triggered by seeks. */
  int size_compaction = (vset->current->compaction_score >= 1);
  int seek_compaction = (vset->current->file_to_compact != NULL);

  if (size_compaction) {
    level = vset->current->compaction_level;

    assert(level >= 0);
    assert(level + 1 < RDB_NUM_LEVELS);

    c = rdb_compaction_create(vset->options, level);

    /* Pick the first file that comes after compact_pointer[level]. */
    for (i = 0; i < vset->current->files[level].length; i++) {
      rdb_filemeta_t *f = vset->current->files[level].items[i];

      if (vset->compact_pointer[level].size == 0 ||
          rdb_compare(&vset->icmp, &f->largest,
                      &vset->compact_pointer[level]) > 0) {
        rdb_vector_push(&c->inputs[0], f);
        break;
      }
    }

    if (c->inputs[0].length == 0) {
      /* Wrap-around to the beginning of the key space. */
      rdb_vector_push(&c->inputs[0], vset->current->files[level].items[0]);
    }
  } else if (seek_compaction) {
    level = vset->current->file_to_compact_level;
    c = rdb_compaction_create(vset->options, level);
    rdb_vector_push(&c->inputs[0], vset->current->file_to_compact);
  } else {
    return NULL;
  }

  c->input_version = vset->current;

  rdb_version_ref(c->input_version);

  /* Files in level 0 may overlap each other,
     so pick up all overlapping ones. */
  if (level == 0) {
    rdb_slice_t smallest, largest;

    rdb_vset_get_range(vset, &c->inputs[0], &smallest, &largest);

    /* Note that the next call will discard the file we placed in
       c->inputs[0] earlier and replace it with an overlapping set
       which will include the picked file. */
    rdb_version_get_overlapping_inputs(vset->current, 0,
                                       &smallest, &largest,
                                       &c->inputs[0]);

    assert(c->inputs[0].length > 0);
  }

  rdb_vset_setup_other_inputs(vset, c);

  return c;
}

/* Finds the largest key in a vector of files. Returns true if files is not
   empty. */
static int
find_largest_key(const rdb_comparator_t *icmp,
                 const rdb_vector_t *files,
                 rdb_slice_t *largest_key) {
  rdb_ikey_t *large;
  size_t i;

  if (files->length == 0)
    return 0;

  for (i = 0; i < files->length; i++) {
    rdb_filemeta_t *f = files->items[i];

    if (i == 0) {
      large = &f->largest;
    } else {
      if (rdb_compare(icmp, &f->largest, large) > 0)
        large = &f->largest;
    }
  }

  *largest_key = *large;

  return 1;
}

/* Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
   user_key(l2) = user_key(u1). */
static rdb_filemeta_t *
find_smallest_boundary_file(const rdb_comparator_t *icmp,
                            const rdb_vector_t *level_files,
                            const rdb_ikey_t *largest_key) {
  const rdb_comparator_t *user_cmp = icmp->user_comparator;
  rdb_slice_t user_key = rdb_ikey_user_key(largest_key);
  rdb_filemeta_t *res = NULL;
  rdb_slice_t file_key;
  size_t i;

  for (i = 0; i < level_files->length; ++i) {
    rdb_filemeta_t *f = level_files->items[i];

    if (rdb_compare(icmp, &f->smallest, largest_key) <= 0)
      continue;

    file_key = rdb_ikey_user_key(&f->smallest);

    if (rdb_compare(user_cmp, &file_key, &user_key) == 0) {
      if (res == NULL || rdb_compare(icmp, &f->smallest, &res->smallest) < 0)
        res = f;
    }
  }

  return res;
}

/* Extracts the largest file b1 from |compaction_files| and then searches for a
 * b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
 * file b2 (known as a boundary file) it adds it to |compaction_files| and then
 * searches again using this new upper bound.
 *
 * If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
 * user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
 * subsequent get operation will yield an incorrect result because it will
 * return the record from b2 in level i rather than from b1 because it searches
 * level by level for records matching the supplied user key.
 *
 * parameters:
 *   in     level_files:      List of files to search for boundary files.
 *   in/out compaction_files: List of files to extend by adding boundary files.
 */
void
add_boundary_inputs(const rdb_comparator_t *icmp,
                    const rdb_vector_t *level_files,
                    rdb_vector_t *compaction_files) {
  rdb_slice_t largest_key;
  int search = 1;

  /* Quick return if compaction_files is empty. */
  if (!find_largest_key(icmp, compaction_files, &largest_key))
    return;

  while (search) {
    rdb_filemeta_t *file = find_smallest_boundary_file(icmp,
                                                       level_files,
                                                       &largest_key);

    /* If a boundary file was found advance
       largest_key, otherwise we're done. */
    if (file != NULL) {
      rdb_vector_push(compaction_files, file);
      largest_key = file->largest;
    } else {
      search = 0;
    }
  }
}

static void
rdb_vset_setup_other_inputs(rdb_vset_t *vset, rdb_compaction_t *c) {
  int level = rdb_compaction_level(c);
  rdb_slice_t smallest, largest;
  rdb_slice_t all_start, all_limit;

  add_boundary_inputs(&vset->icmp,
                      &vset->current->files[level],
                      &c->inputs[0]);

  rdb_vset_get_range(vset, &c->inputs[0], &smallest, &largest);

  rdb_version_get_overlapping_inputs(vset->current, level + 1,
                                     &smallest, &largest,
                                     &c->inputs[1]);

  add_boundary_inputs(&vset->icmp,
                      &vset->current->files[level + 1],
                      &c->inputs[1]);

  /* Get entire range covered by compaction. */
  rdb_vset_get_range2(vset, &c->inputs[0], &c->inputs[1],
                            &all_start, &all_limit);

  /* See if we can grow the number of inputs in "level" without
     changing the number of "level+1" files we pick up. */
  if (c->inputs[1].length > 0) {
    rdb_vector_t expanded0;
    /* int64_t inputs0_size; */
    int64_t inputs1_size;
    int64_t expanded0_size;

    rdb_vector_init(&expanded0);

    rdb_version_get_overlapping_inputs(vset->current, level,
                                       &all_start, &all_limit,
                                       &expanded0);

    add_boundary_inputs(&vset->icmp, &vset->current->files[level], &expanded0);

    /* inputs0_size = total_file_size(&c->inputs[0]); */
    inputs1_size = total_file_size(&c->inputs[1]);
    expanded0_size = total_file_size(&expanded0);

    if (expanded0.length > c->inputs[0].length &&
        inputs1_size + expanded0_size <
            expanded_compaction_byte_size_limit(vset->options)) {
      rdb_slice_t new_start, new_limit;
      rdb_vector_t expanded1;

      rdb_vector_init(&expanded1);

      rdb_vset_get_range(vset, &expanded0, &new_start, &new_limit);

      rdb_version_get_overlapping_inputs(vset->current,
                                         level + 1,
                                         &new_start,
                                         &new_limit,
                                         &expanded1);

      add_boundary_inputs(&vset->icmp,
                          &vset->current->files[level + 1],
                          &expanded1);

      if (expanded1.length == c->inputs[1].length) {
        smallest = new_start;
        largest = new_limit;

        rdb_vector_swap(&c->inputs[0], &expanded0);
        rdb_vector_swap(&c->inputs[1], &expanded1);

        rdb_vset_get_range2(vset, &c->inputs[0], &c->inputs[1],
                                  &all_start, &all_limit);
      }

      rdb_vector_clear(&expanded1);
    }

    rdb_vector_clear(&expanded0);
  }

  /* Compute the set of grandparent files that overlap this compaction
     (parent == level+1; grandparent == level+2). */
  if (level + 2 < RDB_NUM_LEVELS) {
    rdb_version_get_overlapping_inputs(vset->current, level + 2,
                                       &all_start, &all_limit,
                                       &c->grandparents);
  }

  /* Update the place where we will do the next compaction for this level.
     We update this immediately instead of waiting for the VersionEdit
     to be applied so that if the compaction fails, we will try a different
     key range next time. */
  rdb_buffer_copy(&vset->compact_pointer[level], &largest);

  rdb_vedit_set_compact_pointer(&c->edit, level, &largest);
}

rdb_compaction_t *
rdb_vset_compact_range(rdb_vset_t *vset,
                       int level,
                       const rdb_ikey_t *begin,
                       const rdb_ikey_t *end) {
  rdb_vector_t inputs;
  rdb_compaction_t *c;

  rdb_vector_init(&inputs);

  rdb_version_get_overlapping_inputs(vset->current, level, begin, end, &inputs);

  if (inputs.length == 0)
    return NULL;

  /* Avoid compacting too much in one shot in case the range is large.
     But we cannot do this for level-0 since level-0 files can overlap
     and we must not pick one file and drop another older file if the
     two files overlap. */
  if (level > 0) {
    uint64_t limit = max_file_size_for_level(vset->options, level);
    uint64_t total = 0;
    size_t i;

    for (i = 0; i < inputs.length; i++) {
      rdb_filemeta_t *f = inputs.items[i];

      total += f->file_size;

      if (total >= limit) {
        rdb_vector_resize(&inputs, i + 1);
        break;
      }
    }
  }

  c = rdb_compaction_create(vset->options, level);

  c->input_version = vset->current;

  rdb_version_ref(c->input_version);

  rdb_vector_swap(&c->inputs[0], &inputs);

  rdb_vset_setup_other_inputs(vset, c);

  rdb_vector_clear(&inputs);

  return c;
}

/*
 * Compaction
 */

static void
rdb_compaction_init(rdb_compaction_t *c,
                    const rdb_dbopt_t *options,
                    int level) {
  int i;

  c->level = level;
  c->max_output_file_size = max_file_size_for_level(options, level);
  c->input_version = NULL;
  c->grandparent_index = 0;
  c->seen_key = 0;
  c->overlapped_bytes = 0;

  for (i = 0; i < RDB_NUM_LEVELS; i++)
    c->level_ptrs[i] = 0;

  rdb_vedit_init(&c->edit);
  rdb_vector_init(&c->inputs[0]);
  rdb_vector_init(&c->inputs[1]);
  rdb_vector_init(&c->grandparents);
}

static void
rdb_compaction_clear(rdb_compaction_t *c) {
  if (c->input_version != NULL)
    rdb_version_unref(c->input_version);

  rdb_vedit_clear(&c->edit);
  rdb_vector_clear(&c->inputs[0]);
  rdb_vector_clear(&c->inputs[1]);
  rdb_vector_clear(&c->grandparents);
}

rdb_compaction_t *
rdb_compaction_create(const rdb_dbopt_t *options, int level) {
  rdb_compaction_t *c = rdb_malloc(sizeof(rdb_compaction_t));
  rdb_compaction_init(c, options, level);
  return c;
}

void
rdb_compaction_destroy(rdb_compaction_t *c) {
  rdb_compaction_clear(c);
  rdb_free(c);
}

int
rdb_compaction_level(const rdb_compaction_t *c) {
  return c->level;
}

rdb_vedit_t *
rdb_compaction_edit(rdb_compaction_t *c) {
  return &c->edit;
}

int
rdb_compaction_num_input_files(const rdb_compaction_t *c, int which) {
  return c->inputs[which].length;
}

rdb_filemeta_t *
rdb_compaction_input(const rdb_compaction_t *c, int which, int i) {
  return c->inputs[which].items[i];
}

uint64_t
rdb_compaction_max_output_file_size(const rdb_compaction_t *c) {
  return c->max_output_file_size;
}

int
rdb_compaction_is_trivial_move(const rdb_compaction_t *c) {
  const rdb_vset_t *vset = c->input_version->vset;

  /* Avoid a move if there is lots of overlapping grandparent data.
     Otherwise, the move could create a parent file that will require
     a very expensive merge later on. */
  return rdb_compaction_num_input_files(c, 0) == 1
      && rdb_compaction_num_input_files(c, 1) == 0
      && total_file_size(&c->grandparents) <=
           max_grandparent_overlap_bytes(vset->options);
}

void
rdb_compaction_add_input_deletions(rdb_compaction_t *c, rdb_vedit_t *edit) {
  int which;
  size_t i;

  for (which = 0; which < 2; which++) {
    for (i = 0; i < c->inputs[which].length; i++) {
      const rdb_filemeta_t *file = c->inputs[which].items[i];

      rdb_vedit_remove_file(edit, c->level + which, file->number);
    }
  }
}

int
rdb_compaction_is_base_level_for_key(rdb_compaction_t *c,
                                     const rdb_slice_t *user_key) {
  /* Maybe use binary search to find right entry instead of linear search? */
  const rdb_comparator_t *user_cmp =
    c->input_version->vset->icmp.user_comparator;
  int lvl;

  for (lvl = c->level + 2; lvl < RDB_NUM_LEVELS; lvl++) {
    rdb_vector_t *files = &c->input_version->files[lvl];

    while (c->level_ptrs[lvl] < files->length) {
      rdb_filemeta_t *f = files->items[c->level_ptrs[lvl]];
      rdb_slice_t key = rdb_ikey_user_key(&f->largest);

      if (rdb_compare(user_cmp, user_key, &key) <= 0) {
        /* We've advanced far enough. */
        key = rdb_ikey_user_key(&f->smallest);

        if (rdb_compare(user_cmp, user_key, &key) >= 0) {
          /* Key falls in this file's range, so definitely not base level. */
          return 0;
        }

        break;
      }

      c->level_ptrs[lvl]++;
    }
  }

  return 1;
}

int
rdb_compaction_should_stop_before(rdb_compaction_t *c,
                                  const rdb_slice_t *ikey) {
  const rdb_filemeta_t **items = (const rdb_filemeta_t **)c->grandparents.items;
  const rdb_vset_t *vset = c->input_version->vset;
  const rdb_comparator_t *icmp = &vset->icmp;

  /* Scan to find earliest grandparent file that contains key. */
  while (c->grandparent_index < c->grandparents.length) {
    if (rdb_compare(icmp, ikey, &items[c->grandparent_index]->largest) <= 0)
      break;

    if (c->seen_key)
      c->overlapped_bytes += items[c->grandparent_index]->file_size;

    c->grandparent_index++;
  }

  c->seen_key = 1;

  if (c->overlapped_bytes > max_grandparent_overlap_bytes(vset->options)) {
    /* Too much overlap for current output; start new output. */
    c->overlapped_bytes = 0;
    return 1;
  } else {
    return 0;
  }
}

void
rdb_compaction_release_inputs(rdb_compaction_t *c) {
  if (c->input_version != NULL) {
    rdb_version_unref(c->input_version);
    c->input_version = NULL;
  }
}
