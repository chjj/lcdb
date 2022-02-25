
// Grouping of constants.  We may want to make some of these
// parameters set via options.
#define RDB_NUM_LEVELS 7

// Level-0 compaction is started when we hit this many files.
#define RDB_L0_COMPACTION_TRIGGER 4

// Soft limit on number of level-0 files.  We slow down writes at this point.
#define RDB_L0_SLOWDOWN_WRITES_TRIGGER 8

// Maximum number of level-0 files.  We stop writes at this point.
#define RDB_L0_STOP_WRITES_TRIGGER 12

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
#define RDB_MAX_MEM_COMPACT_LEVEL 2

// Approximate gap in bytes between samples of data read during iteration.
#define RDB_READ_BYTES_PERIOD 1048576

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
enum rdb_value_type {
  RDB_TYPE_DELETION = 0x0,
  RDB_TYPE_VALUE = 0x1
};

// RDB_VALUE_TYPE_SEEK defines the rdb_value_type that should be passed when
// constructing a rdb_pkey_t object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// rdb_value_type, not the lowest).
#define RDB_VALUE_TYPE_SEEK RDB_TYPE_VALUE

typedef uint64_t rdb_seqnum_t;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
#define RDB_MAX_SEQUENCE ((UINT64_C(1) << 56) - 1)
