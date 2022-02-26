/*
 * Types
 */

typedef struct rdb_filemeta_s {
  int refs;
  int allowed_seeks;   /* Seeks allowed until compaction. */
  uint64_t number;
  uint64_t file_size;  /* File size in bytes. */
  rdb_ikey_t smallest; /* Smallest internal key served by table. */
  rdb_ikey_t largest;  /* Largest internal key served by table. */
} rdb_filemeta_t;

typedef struct rdb_vedit_s {
  rdb_buffer_t comparator;
  uint64_t log_number;
  uint64_t prev_log_number;
  uint64_t next_file_number;
  rdb_seqnum_t last_sequence;
  int has_comparator;
  int has_log_number;
  int has_prev_log_number;
  int has_next_file_number;
  int has_last_sequence;
  rdb_vector_t compact_pointers; /* ikey_entry_t */
  rdb_set_t deleted_files;       /* file_entry_t */
  rdb_vector_t new_files;        /* meta_entry_t */
} rdb_vedit_t;

typedef struct ikey_entry_s {
  int level;
  rdb_ikey_t key;
} ikey_entry_t;

typedef struct file_entry_s {
  int level;
  uint64_t number;
} file_entry_t;

typedef struct meta_entry_s {
  int level;
  rdb_filemeta_t meta;
} meta_entry_t;
