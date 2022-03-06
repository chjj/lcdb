#undef NDEBUG

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rdb.h>

int rdb_env_clear(void);

int
main(void) {
  rdb_dbopt_t opt = *rdb_dbopt_default;
  rdb_slice_t key, val, ret;
  char path[1024];
  char kbuf[64];
  char vbuf[64];
  rdb_batch_t b;
  rdb_t *db;
  int i, rc;

  assert(rdb_test_filename(path, sizeof(path), "simpledb"));

  rdb_destroy_db(path, 0);

  {
    opt.create_if_missing = 1;
    opt.error_if_exists = 1;
    opt.filter_policy = rdb_bloom_default;

    rc = rdb_open(path, &opt, &db);

    assert(rc == RDB_OK);

    {
      rdb_batch_init(&b);

      for (i = 0; i < 1000000; i++) {
        sprintf(kbuf, "hello %d padding padding paddi", rand());
        sprintf(vbuf, "world %d", i);

        key = rdb_string(kbuf);
        val = rdb_string(vbuf);

        if (i > 0 && (i % 1000) == 0) {
          rc = rdb_write(db, &b, 0);

          assert(rc == RDB_OK);

          rdb_batch_reset(&b);
        }

        rdb_batch_put(&b, &key, &val);

        assert(rc == RDB_OK);
      }

      rc = rdb_write(db, &b, 0);

      assert(rc == RDB_OK);

      rdb_batch_clear(&b);
    }

    {
      rc = rdb_get(db, &key, &ret, 0);

      assert(rc == RDB_OK);
      assert(rdb_compare(&ret, &val) == 0);

      rdb_free(ret.data);
    }

    {
      char *prop;

      if (rdb_get_property(db, "leveldb.stats", &prop)) {
        puts(prop);
        rdb_free(prop);
      }
    }

    {
      char *prop;

      if (rdb_get_property(db, "leveldb.sstables", &prop)) {
        puts(prop);
        rdb_free(prop);
      }
    }

    rdb_close(db);
  }

  {
    opt.create_if_missing = 0;
    opt.error_if_exists = 0;

    rc = rdb_open(path, &opt, &db);

    assert(rc == RDB_OK);

    {
      ret = rdb_slice(0, 0);
      rc = rdb_get(db, &key, &ret, 0);

      assert(rc == RDB_OK);
      assert(rdb_compare(&ret, &val) == 0);

      rdb_free(ret.data);
    }

    {
      rdb_iter_t *it = rdb_iterator(db, 0);
      int total = 0;

      rdb_iter_seek_first(it);

      while (rdb_iter_valid(it)) {
        rdb_slice_t k = rdb_iter_key(it);
        rdb_slice_t v = rdb_iter_value(it);

        assert(k.size >= 7);
        assert(v.size >= 7);

        assert(memcmp(k.data, "hello ", 6) == 0);
        assert(memcmp(v.data, "world ", 6) == 0);

        rdb_iter_next(it);

        total++;
      }

      assert(total >= 999000);
      assert(rdb_iter_status(it) == RDB_OK);

      rdb_iter_destroy(it);
    }

    rdb_close(db);
  }

  rdb_destroy_db(path, 0);

  rdb_env_clear();

  return 0;
}
