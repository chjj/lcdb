//! build.zig - zig build for lcdb
//! Copyright (c) 2022, Christopher Jeffrey (MIT License).
//! https://github.com/chjj/lcdb

const std = @import("std");
const ArrayList = std.ArrayList;
const fs = std.fs;

//
// Build
//

pub fn build(b: *std.build.Builder) void {
  const pkg_version = "0.0.0";
  const abi_version = b.version(0, 0, 0);
  const inst_step = b.getInstallStep();
  const test_step = b.step("test", "Run tests");
  const bench_step = b.step("bench", "Run benchmarks");

  //
  // Options
  //
  b.setPreferredReleaseMode(.ReleaseFast);

  const target = b.standardTargetOptions(.{});
  const mode = b.standardReleaseOptions();

  const prefix = b.option([]const u8, "prefix",
                          "System install prefix (/usr/local)");
  const enable_bench = b.option(bool, "bench",
                                "Build benchmarks (true)") orelse true;
  const enable_pic = b.option(bool, "pic", "Force PIC (false)");
  const enable_portable = b.option(bool, "portable",
                            "Be as portable as possible (false)") orelse false;
  const enable_pthread = b.option(bool, "pthread",
                                  "Use pthread (true)") orelse true;
  const enable_shared = b.option(bool, "shared",
                                 "Build shared library (true)") orelse true;
  const enable_tests = b.option(bool, "tests",
                                "Enable tests (true)") orelse true;

  const strip = (b.is_release and !target.isWindows());

  //
  // Variables
  //
  var flags = ArrayList([]const u8).init(b.allocator);
  var defines = ArrayList([]const u8).init(b.allocator);
  var libs = ArrayList([]const u8).init(b.allocator);

  defer flags.deinit();
  defer defines.deinit();
  defer libs.deinit();

  //
  // Global Flags
  //
  if (target.isGnuLibC()) {
    flags.append("-std=c89") catch unreachable;
  }

  flags.append("-fvisibility=hidden") catch unreachable;

  if (target.isDarwin()) {
    flags.append("-mmacosx-version-min=10.7") catch unreachable;
  }

  if (mode == .ReleaseFast) {
    flags.append("-O3") catch unreachable;
  }

  //
  // Feature Test Macros
  //
  if (target.isWindows()) {
    defines.append("_WIN32_WINNT=0x501") catch unreachable;
  }

  if (target.isGnuLibC()) {
    defines.append("_GNU_SOURCE") catch unreachable;
  }

  if (target.getOsTag() == .solaris) {
    defines.append("_TS_ERRNO") catch unreachable;
  }

  if (target.getOsTag() == .aix) {
    defines.append("_THREAD_SAFE_ERRNO") catch unreachable;
  }

  //
  // System Libraries
  //
  if (target.isWindows()) {
    libs.append("kernel32") catch unreachable;
  } else {
    if (enable_pthread and target.getOsTag() != .wasi) {
      flags.append("-pthread") catch unreachable;
      libs.append("pthread") catch unreachable;
      defines.append("LDB_PTHREAD") catch unreachable;
    }
  }

  //
  // Sources
  //
  const sources = [_][]const u8{
    "src/util/arena.c",
    "src/util/array.c",
    "src/util/atomic.c",
    "src/util/bloom.c",
    "src/util/buffer.c",
    "src/util/cache.c",
    "src/util/comparator.c",
    "src/util/crc32c.c",
    "src/util/env.c",
    "src/util/hash.c",
    "src/util/internal.c",
    "src/util/logger.c",
    "src/util/options.c",
    "src/util/port.c",
    "src/util/random.c",
    "src/util/rbt.c",
    "src/util/slice.c",
    "src/util/snappy.c",
    "src/util/status.c",
    "src/util/strutil.c",
    "src/util/tar.c",
    "src/util/thread_pool.c",
    "src/util/vector.c",
    "src/table/block.c",
    "src/table/block_builder.c",
    "src/table/filter_block.c",
    "src/table/format.c",
    "src/table/iterator.c",
    "src/table/merger.c",
    "src/table/table.c",
    "src/table/table_builder.c",
    "src/table/two_level_iterator.c",
    "src/builder.c",
    "src/c.c",
    "src/db_impl.c",
    "src/db_iter.c",
    "src/dbformat.c",
    "src/dumpfile.c",
    "src/filename.c",
    "src/log_reader.c",
    "src/log_writer.c",
    "src/memtable.c",
    "src/repair.c",
    "src/skiplist.c",
    "src/table_cache.c",
    "src/version_edit.c",
    "src/version_set.c",
    "src/write_batch.c"
  };

  //
  // Flags
  //
  const warn_flags = [_][]const u8{
    "-pedantic",
    "-Wall",
    "-Wextra",
    "-Wcast-align",
    "-Wconditional-uninitialized",
    "-Wmissing-prototypes",
    "-Wno-implicit-fallthrough",
    "-Wno-long-long",
    "-Wno-overlength-strings",
    "-Wshadow",
    "-Wstrict-prototypes",
    "-Wundef"
  };

  for (warn_flags) |flag| {
    flags.append(flag) catch unreachable;
  }

  //
  // Defines
  //
  if (mode == .Debug) {
    defines.append("LDB_DEBUG") catch unreachable;
  }

  if (!enable_portable) {
    defines.append("LDB_HAVE_FDATASYNC") catch unreachable;
    defines.append("LDB_HAVE_PREAD") catch unreachable;
  }

  //
  // Targets
  //
  const libname = if (target.isWindows()) "liblcdb" else "lcdb";
  const lcdb = b.addStaticLibrary(libname, null);

  lcdb.setTarget(target);
  lcdb.setBuildMode(mode);
  lcdb.install();
  lcdb.linkLibC();
  lcdb.addIncludeDir("./include");
  lcdb.addCSourceFiles(&sources, flags.items);
  lcdb.force_pic = enable_pic;
  lcdb.strip = strip;

  for (defines.items) |def| {
    lcdb.defineCMacroRaw(def);
  }

  if (enable_shared and target.getOsTag() != .wasi) {
    const shared = b.addSharedLibrary("lcdb", null, abi_version);

    shared.setTarget(target);
    shared.setBuildMode(mode);
    shared.install();
    shared.linkLibC();
    shared.addIncludeDir("./include");
    shared.addCSourceFiles(&sources, flags.items);
    shared.strip = strip;

    for (defines.items) |def| {
      shared.defineCMacroRaw(def);
    }

    shared.defineCMacroRaw("LDB_EXPORT");

    for (libs.items) |lib| {
      shared.linkSystemLibrary(lib);
    }
  }

  const dbutil = b.addExecutable("lcdbutil", null);

  dbutil.setTarget(target);
  dbutil.setBuildMode(mode);
  dbutil.install();
  dbutil.linkLibC();
  dbutil.linkLibrary(lcdb);
  dbutil.addIncludeDir("./include");
  dbutil.addCSourceFile("src/dbutil.c", flags.items);
  dbutil.strip = strip;

  for (defines.items) |def| {
    dbutil.defineCMacroRaw(def);
  }

  for (libs.items) |lib| {
    dbutil.linkSystemLibrary(lib);
  }

  //
  // Benchmarks
  //
  const bench = b.addExecutable("db_bench", null);

  bench.setTarget(target);
  bench.setBuildMode(mode);
  bench.linkLibC();
  bench.linkLibrary(lcdb);
  bench.addIncludeDir("./include");
  bench.addIncludeDir("./src");

  bench.addCSourceFiles(&.{
    "bench/db_bench.c",
    "bench/histogram.c",
    "src/util/testutil.c"
  }, flags.items);

  for (defines.items) |def| {
    bench.defineCMacroRaw(def);
  }

  for (libs.items) |lib| {
    bench.linkSystemLibrary(lib);
  }

  if (enable_bench) {
    inst_step.dependOn(&bench.step);
  }

  bench_step.dependOn(&bench.run().step);

  //
  // Tests
  //
  const testutil = b.addStaticLibrary("test", null);

  testutil.setTarget(target);
  testutil.setBuildMode(mode);
  testutil.linkLibC();
  testutil.addIncludeDir("./include");
  testutil.addCSourceFile("src/util/testutil.c", flags.items);

  for (defines.items) |def| {
    testutil.defineCMacroRaw(def);
  }

  const tests = [_][]const u8{
    "arena",
    "autocompact",
    "bloom",
    "c",
    "cache",
    "coding",
    "corruption",
    "crc32c",
    "db",
    "dbformat",
    "env",
    "filename",
    "filter_block",
    "hash",
    "issue178",
    "issue200",
    "issue320",
    "log",
    "rbt",
    "recovery",
    "simple",
    "skiplist",
    "snappy",
    "status",
    "strutil",
    "table",
    "tar",
    "version_edit",
    "version_set",
    "write_batch"
  };

  for (tests) |name| {
    const t = b.addExecutable(b.fmt("t-{s}", .{ name }), null);

    t.setTarget(target);
    t.setBuildMode(mode);
    t.linkLibC();
    t.linkLibrary(testutil);
    t.linkLibrary(lcdb);
    t.addIncludeDir("./include");
    t.addIncludeDir("./src");
    t.addCSourceFile(b.fmt("test/t-{s}.c", .{ name }), flags.items);

    for (defines.items) |def| {
      t.defineCMacroRaw(def);
    }

    for (libs.items) |lib| {
      t.linkSystemLibrary(lib);
    }

    if (enable_tests) {
      inst_step.dependOn(&t.step);
    }

    test_step.dependOn(&t.run().step);
  }

  //
  // Package Config
  //
  if (!target.isWindows() and target.getOsTag() != .wasi) {
    const pkg_prefix = prefix orelse "/usr/local";
    const pkg_libs = if (enable_pthread) "-lpthread" else "";
    const pkg_conf = b.fmt(
      \\prefix={s}
      \\exec_prefix=${{prefix}}
      \\libdir=${{exec_prefix}}/{s}
      \\includedir=${{prefix}}/{s}
      \\
      \\Name: lcdb
      \\Version: {s}
      \\Description: Database for C.
      \\URL: https://github.com/chjj/lcdb
      \\
      \\Cflags: -I${{includedir}}
      \\Libs: -L${{libdir}} -llcdb
      \\Libs.private: {s}
      \\
      ,
      .{
        pkg_prefix,
        "lib",
        "include",
        pkg_version,
        pkg_libs
      }
    );

    fs.cwd().writeFile(b.pathFromRoot("lcdb.pc"), pkg_conf) catch {};
  }

  //
  // Install
  //
  b.installFile("include/lcdb.h", "include/lcdb.h");
  b.installFile("include/lcdb_c.h", "include/lcdb_c.h");

  if (!target.isWindows() and target.getOsTag() != .wasi) {
    b.installFile("LICENSE", "share/licenses/lcdb/LICENSE");
    b.installFile("README.md", "share/doc/lcdb/README.md");
    b.installFile("lcdb.pc", "lib/pkgconfig/lcdb.pc");
  } else {
    b.installFile("LICENSE", "LICENSE");
    b.installFile("README.md", "README.md");
  }
}
