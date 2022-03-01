/*!
 * version_set.h - version set for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_VERSION_SET_H
#define RDB_VERSION_SET_H

#include <stddef.h>
#include <stdint.h>

#include "table/iterator.h"

#include "util/comparator.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/port.h"
#include "util/rbt.h"
#include "util/types.h"

#include "dbformat.h"
#include "log_writer.h"
#include "table_cache.h"
#include "version_edit.h"

/*
 * Types
 */

typedef struct rdb_getstats_s {
  rdb_filemeta_t *seek_file;
  int seek_file_level;
} rdb_getstats_t;

typedef struct rdb_version_s rdb_version_t;
typedef struct rdb_vset_s rdb_vset_t;
typedef struct rdb_compaction_s rdb_compaction_t;

struct rdb_version_s {
  rdb_vset_t *vset;    /* VersionSet to which this Version belongs. */
  rdb_version_t *next; /* Next version in linked list. */
  rdb_version_t *prev; /* Previous version in linked list. */
  int refs;            /* Number of live refs to this version. */

  /* List of files per level. */
  rdb_vector_t files[RDB_NUM_LEVELS]; /* rdb_filemeta_t[] */

  /* Next file to compact based on seek stats. */
  rdb_filemeta_t *file_to_compact;
  int file_to_compact_level;

  /* Level that should be compacted next and its compaction score.
     Score < 1 means compaction is not strictly needed. These fields
     are initialized by finalize(). */
  double compaction_score;
  int compaction_level;
};

struct rdb_vset_s {
  const char *dbname;
  const rdb_dbopt_t *options;
  rdb_tcache_t *table_cache;
  rdb_comparator_t icmp;
  uint64_t next_file_number;
  uint64_t manifest_file_number;
  uint64_t last_sequence;
  uint64_t log_number;
  uint64_t prev_log_number; /* 0 or backing store for memtable being compacted. */

  /* Opened lazily. */
  rdb_wfile_t *descriptor_file;
  rdb_logwriter_t *descriptor_log;
  rdb_version_t dummy_versions; /* Head of circular doubly-linked list of versions. */
  rdb_version_t *current;       /* == dummy_versions.prev */

  /* Per-level key at which the next compaction at that level should start.
     Either an empty string, or a valid rdb_ikey_t. */
  rdb_buffer_t compact_pointer[RDB_NUM_LEVELS];
};

struct rdb_compaction_s {
  int level;
  uint64_t max_output_file_size;
  rdb_version_t *input_version;
  rdb_vedit_t edit;

  /* Each compaction reads inputs from "level" and "level+1". */
  rdb_vector_t inputs[2]; /* The two sets of inputs. */

  /* State used to check for number of overlapping grandparent files
     (parent == level + 1, grandparent == level + 2) */
  rdb_vector_t grandparents;
  size_t grandparent_index;   /* Index in grandparent_starts. */
  int seen_key;               /* Some output key has been seen. */
  int64_t overlapped_bytes;   /* Bytes of overlap between current output
                                 and grandparent files. */

  /* State for implementing is_base_level_for_key. */

  /* level_ptrs holds indices into input_version->levels: our state
     is that we are positioned at one of the file ranges for each
     higher level than the ones involved in this compaction (i.e. for
     all L >= level + 2). */
  size_t level_ptrs[RDB_NUM_LEVELS];
};

/*
 * Helpers
 */

#define find_file rdb__find_file
#define some_file_overlaps_range rdb__some_file_overlaps_range

/* Return the smallest index i such that files[i]->largest >= key. */
/* Return files.size if there is no such file. */
/* REQUIRES: "files" contains a sorted list of non-overlapping files. */
int
find_file(const rdb_comparator_t *icmp,
          const rdb_vector_t *files,
          const rdb_slice_t *key);

/* Returns true iff some file in "files" overlaps the user key range
   [*smallest,*largest].
   smallest==NULL represents a key smaller than all keys in the DB.
   largest==NULL represents a key largest than all keys in the DB. */
/* REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
             in sorted order. */
int
some_file_overlaps_range(const rdb_comparator_t *icmp,
                         int disjoint_sorted_files,
                         const rdb_vector_t *files,
                         const rdb_slice_t *smallest_user_key,
                         const rdb_slice_t *largest_user_key);

/*
 * Version
 */

rdb_version_t *
rdb_version_create(rdb_vset_t *vset);

void
rdb_version_destroy(rdb_version_t *ver);

int
rdb_version_num_files(const rdb_version_t *ver, int level);

/* Append to *iters a sequence of iterators that will
   yield the contents of this Version when merged together. */
/* REQUIRES: This version has been saved (see VersionSet::SaveTo) */
void
rdb_version_add_iterators(rdb_version_t *ver,
                          const rdb_readopt_t *options,
                          rdb_vector_t *iters);

/* Lookup the value for key. If found, store it in *val and
   return OK. Else return a non-OK status. Fills *stats. */
/* REQUIRES: lock is not held */
int
rdb_version_get(rdb_version_t *ver,
                const rdb_readopt_t *options,
                const rdb_lkey_t *k,
                rdb_buffer_t *value,
                rdb_getstats_t *stats);

/* Adds "stats" into the current state. Returns true if a new
   compaction may need to be triggered, false otherwise. */
/* REQUIRES: lock is held */
int
rdb_version_update_stats(rdb_version_t *ver, const rdb_getstats_t *stats);

/* Record a sample of bytes read at the specified internal key.
   Samples are taken approximately once every RDB_READ_BYTES_PERIOD
   bytes. Returns true if a new compaction may need to be triggered. */
/* REQUIRES: lock is held */
int
rdb_version_record_read_sample(rdb_version_t *ver, const rdb_slice_t *ikey);

/* Reference count management (so Versions do not disappear out from
   under live iterators). */
void
rdb_version_ref(rdb_version_t *ver);

void
rdb_version_unref(rdb_version_t *ver);

/* Returns true iff some file in the specified level overlaps
   some part of [*smallest_user_key,*largest_user_key].
   smallest_user_key==NULL represents a key smaller than all the DB's keys.
   largest_user_key==NULL represents a key largest than all the DB's keys. */
int
rdb_version_overlap_in_level(rdb_version_t *ver,
                             int level,
                             const rdb_slice_t *smallest_user_key,
                             const rdb_slice_t *largest_user_key);

/* Return the level at which we should place a new memtable compaction
   result that covers the range [smallest_user_key,largest_user_key]. */
int
rdb_version_pick_level_for_memtable_output(rdb_version_t *ver,
                                           const rdb_slice_t *small_key,
                                           const rdb_slice_t *large_key);

void
rdb_version_get_overlapping_inputs(rdb_version_t *ver,
                                   int level,
                                   const rdb_ikey_t *begin,
                                   const rdb_ikey_t *end,
                                   rdb_vector_t *inputs);

/*
 * VersionSet
 */

rdb_vset_t *
rdb_vset_create(const char *dbname,
                const rdb_dbopt_t *options,
                rdb_tcache_t *table_cache,
                const rdb_comparator_t *cmp);

void
rdb_vset_destroy(rdb_vset_t *vset);

/* Return the current version. */
rdb_version_t *
rdb_vset_current(const rdb_vset_t *vset);

/* Return the current manifest file number. */
uint64_t
rdb_vset_manifest_file_number(const rdb_vset_t *vset);

/* Allocate and return a new file number. */
uint64_t
rdb_vset_new_file_number(rdb_vset_t *vset);

/* Arrange to reuse "file_number" unless a newer file number has
   already been allocated. */
/* REQUIRES: "file_number" was returned by a call to NewFileNumber(). */
void
rdb_vset_reuse_file_number(rdb_vset_t *vset, uint64_t file_number);

/* Return the last sequence number. */
uint64_t
rdb_vset_last_sequence(const rdb_vset_t *vset);

/* Set the last sequence number to s. */
void
rdb_vset_set_last_sequence(rdb_vset_t *vset, uint64_t s);

/* Return the current log file number. */
uint64_t
rdb_vset_log_number(const rdb_vset_t *vset);

/* Return the log file number for the log file that is currently
   being compacted, or zero if there is no such log file. */
uint64_t
rdb_vset_prev_log_number(const rdb_vset_t *vset);

/* Returns true iff some level needs a compaction. */
int
rdb_vset_needs_compaction(const rdb_vset_t *vset);

/* Apply *edit to the current version to form a new descriptor that
   is both saved to persistent state and installed as the new
   current version. Will release *mu while actually writing to the file. */
/* REQUIRES: *mu is held on entry. */
/* REQUIRES: no other thread concurrently calls log_and_apply() */
int
rdb_vset_log_and_apply(rdb_vset_t *vset, rdb_vedit_t *edit, rdb_mutex_t *mu);

/* Recover the last saved descriptor from persistent storage. */
int
rdb_vset_recover(rdb_vset_t *vset, int *save_manifest);

/* Mark the specified file number as used. */
void
rdb_vset_mark_file_number_used(rdb_vset_t *vset, uint64_t number);

/* Return the number of Table files at the specified level. */
int
rdb_vset_num_level_files(const rdb_vset_t *vset, int level);

/* Return the approximate offset in the database of the data for
   "key" as of version "v". */
uint64_t
rdb_vset_approximate_offset_of(rdb_vset_t *vset,
                               rdb_version_t *v,
                               const rdb_ikey_t *ikey);

/* Add all files listed in any live version to *live.
   May also mutate some internal state. */
void
rdb_vset_add_live_files(rdb_vset_t *vset, rb_set64_t *live);

/* Return the combined file size of all files at the specified level. */
int64_t
rdb_vset_num_level_bytes(const rdb_vset_t *vset, int level);

/* Return the maximum overlapping data (in bytes) at next level for any
   file at a level >= 1. */
int64_t
rdb_vset_max_next_level_overlapping_bytes(rdb_vset_t *vset);

/* Create an iterator that reads over the compaction inputs for "*c".
   The caller should delete the iterator when no longer needed. */
rdb_iter_t *
rdb_inputiter_create(rdb_vset_t *vset, rdb_compaction_t *c);

/* Pick level and inputs for a new compaction.
   Returns NULL if there is no compaction to be done.
   Otherwise returns a pointer to a heap-allocated object that
   describes the compaction. Caller should delete the result. */
rdb_compaction_t *
rdb_vset_pick_compaction(rdb_vset_t *vset);

#define add_boundary_inputs rdb__add_boundary_inputs

void
add_boundary_inputs(const rdb_comparator_t *icmp,
                    const rdb_vector_t *level_files,
                    rdb_vector_t *compaction_files);

/* Return a compaction object for compacting the range [begin,end] in
   the specified level. Returns NULL if there is nothing in that
   level that overlaps the specified range. Caller should delete
   the result. */
rdb_compaction_t *
rdb_vset_compact_range(rdb_vset_t *vset,
                       int level,
                       const rdb_ikey_t *begin,
                       const rdb_ikey_t *end);

/*
 * Compaction
 */

rdb_compaction_t *
rdb_compaction_create(const rdb_dbopt_t *options, int level);

void
rdb_compaction_destroy(rdb_compaction_t *c);

/* Return the level that is being compacted. Inputs from "level"
   and "level+1" will be merged to produce a set of "level+1" files. */
int
rdb_compaction_level(const rdb_compaction_t *cmpct);

/* Return the object that holds the edits to the descriptor done
   by this compaction. */
rdb_vedit_t *
rdb_compaction_edit(rdb_compaction_t *cmpct);

/* "which" must be either 0 or 1 */
int
rdb_compaction_num_input_files(const rdb_compaction_t *cmpct, int which);

/* Return the ith input file at "level()+which" ("which" must be 0 or 1). */
rdb_filemeta_t *
rdb_compaction_input(const rdb_compaction_t *cmpct, int which, int i);

/* Maximum size of files to build during this compaction. */
uint64_t
rdb_compaction_max_output_file_size(const rdb_compaction_t *cmpct);

/* Is this a trivial compaction that can be implemented by just
   moving a single input file to the next level (no merging or splitting). */
int
rdb_compaction_is_trivial_move(const rdb_compaction_t *c);

/* Add all inputs to this compaction as delete operations to *edit. */
void
rdb_compaction_add_input_deletions(rdb_compaction_t *c, rdb_vedit_t *edit);

/* Returns true if the information we have available guarantees that
   the compaction is producing data in "level+1" for which no data exists
   in levels greater than "level+1". */
int
rdb_compaction_is_base_level_for_key(rdb_compaction_t *c,
                                     const rdb_slice_t *user_key);

/* Returns true iff we should stop building the current output
   before processing "internal_key". */
int
rdb_compaction_should_stop_before(rdb_compaction_t *c,
                                  const rdb_slice_t *ikey);

/* Release the input version for the compaction, once the compaction
   is successful. */
void
rdb_compaction_release_inputs(rdb_compaction_t *c);

#endif /* RDB_VERSION_SET_H */
