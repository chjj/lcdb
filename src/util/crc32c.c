/*!
 * crc32c.c - crc32c for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * Parts of this software are based on google/crc32c:
 *   Copyright (c) 2017, The CRC32C Authors.
 *   https://github.com/google/crc32c
 *
 * See LICENSE for more information.
 */

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include "coding.h"
#include "crc32c.h"
#include "internal.h"

/*
 * Compat (x86-64)
 */

#undef HAVE_X64_CRC
#undef HAVE_X64_ASM
#undef HAVE_X64_INSTR
#undef HAVE_X64_INTRIN
#undef HAVE_ATOMICS
#undef HAVE_PREFETCH

#if defined(__x86_64__) || defined(_M_X64)
#  if defined(__clang__)
#    if __clang_major__ >= 3 /* Works with Apple Clang. */
#      define HAVE_X64_ASM /* Clang 2.0 (May 2007) */
#      define HAVE_ATOMICS /* Clang 2.3 (Jun 2008) */
#      define HAVE_PREFETCH /* Clang 2.3 (Jun 2008) */
#    endif
#  elif defined(__INTEL_COMPILER) || defined(__ICC)
#    if __INTEL_COMPILER >= 800 /* ICC 8.0 (December 2003) */
#      define HAVE_X64_ASM
#      define HAVE_PREFETCH
#    endif
#    if __INTEL_COMPILER >= 1110 /* ICC 11.1 (June 2009) */
#      define HAVE_ATOMICS
#    endif
#  elif defined(__TINYC__) || defined(__PCC__)
#    define HAVE_X64_ASM
#  elif defined(__NWCC__)
/* Nothing. */
#  elif defined(__GNUC__)
#    if LDB_GNUC_PREREQ(3, 0)
#      define HAVE_X64_ASM
#    endif
#    if LDB_GNUC_PREREQ(4, 1)
#      define HAVE_X64_INSTR
#      define HAVE_ATOMICS
#    endif
#    if LDB_GNUC_PREREQ(3, 1)
#      define HAVE_PREFETCH
#    endif
#  elif defined(_MSC_VER) && defined(__AVX__) /* /arch:AVX */
#    include <intrin.h>
#    define HAVE_X64_INTRIN
#    define HAVE_PREFETCH
#  endif
#endif

#if defined(HAVE_X64_ASM) || defined(HAVE_X64_INTRIN)
#  define HAVE_X64_CRC
#endif

#ifdef HAVE_PREFETCH
#  ifdef HAVE_X64_INTRIN
#    define request_prefetch(p) _mm_prefetch((const char *)(p), _MM_HINT_NTA)
#  else
#    define request_prefetch(p) __builtin_prefetch(p, 0, 0)
#  endif
#endif

/*
 * Compat (aarch64)
 */

#undef HAVE_ARM64_CRC
#undef HAVE_ARM64_ASM
#undef HAVE_ARM64_INTRIN
#undef HAVE_ARMV81A
#undef HAVE_CAPCHECK

/* Check for -march=armv8-a+crc or -march=armv8.1-a. */
#if defined(__aarch64__) && defined(__ARM_ARCH) && __ARM_ARCH >= 8
#  if defined(__ARM_FP) && defined(__ARM_NEON) && defined(__ARM_FEATURE_CRC32)
#    define HAVE_ARMV81A
#  endif
#endif

#ifdef HAVE_ARMV81A
#  if defined(__linux__)
#    if defined(__ANDROID__)
unsigned long getauxval(unsigned long) __attribute__((weak));
#      define AT_HWCAP 16
#      define HAVE_CAPCHECK
#    elif !defined(__UCLIBC__)
#      include <sys/auxv.h>
#      define HAVE_CAPCHECK
#    endif
#    define CRC32_CAPMASK ((1 << 4) | (1 << 7)) /* (HWCAP_PMULL|HWCAP_CRC32) */
#  elif defined(__APPLE__)
#    include <sys/types.h>
#    include <sys/sysctl.h>
#    define HAVE_CAPCHECK
#  endif
#endif

#if defined(HAVE_ARMV81A) && defined(HAVE_CAPCHECK)
#  include <arm_acle.h>
#  include <arm_neon.h>
#  if defined(__GNUC__) || defined(__clang__)
#    define HAVE_ARM64_ASM
#    define HAVE_ATOMICS
#  endif
#  define HAVE_ARM64_CRC
#endif

/*
 * Spinlock
 */

#if defined(HAVE_X64_INTRIN)
#  define ldb_pause _mm_pause
#elif defined(HAVE_ARM64_INTRIN)
#  define ldb_pause __yield
#elif defined(HAVE_X64_ASM)
#  define ldb_pause() __asm__ __volatile__ ("pause\n" ::: "memory")
#elif defined(HAVE_ARM64_ASM)
#  define ldb_pause() __asm__ __volatile__ ("yield\n" ::: "memory")
#else
#  define ldb_pause() do { } while (0)
#endif

#if defined(HAVE_X64_INTRIN) || defined(HAVE_ARM64_INTRIN)
#  define ldb_spinlock_t volatile long
#  define ldb_spinlock(x) while (_InterlockedExchange(x, 1)) ldb_pause()
#  define ldb_spinunlock(x) _InterlockedExchange(x, 0)
#elif defined(HAVE_ATOMICS)
#  define ldb_spinlock_t volatile int
#  define ldb_spinlock(x) while (__sync_lock_test_and_set(x, 1)) ldb_pause()
#  define ldb_spinunlock(x) __sync_lock_release(x)
#else
#  define ldb_spinlock_t int
#  define ldb_spinlock(x) ((void)(x))
#  define ldb_spinunlock(x) ((void)(x))
#endif

/*
 * Constants
 */

static const uint32_t byte_ext_table[256] = {
  0x00000000, 0xf26b8303, 0xe13b70f7, 0x1350f3f4,
  0xc79a971f, 0x35f1141c, 0x26a1e7e8, 0xd4ca64eb,
  0x8ad958cf, 0x78b2dbcc, 0x6be22838, 0x9989ab3b,
  0x4d43cfd0, 0xbf284cd3, 0xac78bf27, 0x5e133c24,
  0x105ec76f, 0xe235446c, 0xf165b798, 0x030e349b,
  0xd7c45070, 0x25afd373, 0x36ff2087, 0xc494a384,
  0x9a879fa0, 0x68ec1ca3, 0x7bbcef57, 0x89d76c54,
  0x5d1d08bf, 0xaf768bbc, 0xbc267848, 0x4e4dfb4b,
  0x20bd8ede, 0xd2d60ddd, 0xc186fe29, 0x33ed7d2a,
  0xe72719c1, 0x154c9ac2, 0x061c6936, 0xf477ea35,
  0xaa64d611, 0x580f5512, 0x4b5fa6e6, 0xb93425e5,
  0x6dfe410e, 0x9f95c20d, 0x8cc531f9, 0x7eaeb2fa,
  0x30e349b1, 0xc288cab2, 0xd1d83946, 0x23b3ba45,
  0xf779deae, 0x05125dad, 0x1642ae59, 0xe4292d5a,
  0xba3a117e, 0x4851927d, 0x5b016189, 0xa96ae28a,
  0x7da08661, 0x8fcb0562, 0x9c9bf696, 0x6ef07595,
  0x417b1dbc, 0xb3109ebf, 0xa0406d4b, 0x522bee48,
  0x86e18aa3, 0x748a09a0, 0x67dafa54, 0x95b17957,
  0xcba24573, 0x39c9c670, 0x2a993584, 0xd8f2b687,
  0x0c38d26c, 0xfe53516f, 0xed03a29b, 0x1f682198,
  0x5125dad3, 0xa34e59d0, 0xb01eaa24, 0x42752927,
  0x96bf4dcc, 0x64d4cecf, 0x77843d3b, 0x85efbe38,
  0xdbfc821c, 0x2997011f, 0x3ac7f2eb, 0xc8ac71e8,
  0x1c661503, 0xee0d9600, 0xfd5d65f4, 0x0f36e6f7,
  0x61c69362, 0x93ad1061, 0x80fde395, 0x72966096,
  0xa65c047d, 0x5437877e, 0x4767748a, 0xb50cf789,
  0xeb1fcbad, 0x197448ae, 0x0a24bb5a, 0xf84f3859,
  0x2c855cb2, 0xdeeedfb1, 0xcdbe2c45, 0x3fd5af46,
  0x7198540d, 0x83f3d70e, 0x90a324fa, 0x62c8a7f9,
  0xb602c312, 0x44694011, 0x5739b3e5, 0xa55230e6,
  0xfb410cc2, 0x092a8fc1, 0x1a7a7c35, 0xe811ff36,
  0x3cdb9bdd, 0xceb018de, 0xdde0eb2a, 0x2f8b6829,
  0x82f63b78, 0x709db87b, 0x63cd4b8f, 0x91a6c88c,
  0x456cac67, 0xb7072f64, 0xa457dc90, 0x563c5f93,
  0x082f63b7, 0xfa44e0b4, 0xe9141340, 0x1b7f9043,
  0xcfb5f4a8, 0x3dde77ab, 0x2e8e845f, 0xdce5075c,
  0x92a8fc17, 0x60c37f14, 0x73938ce0, 0x81f80fe3,
  0x55326b08, 0xa759e80b, 0xb4091bff, 0x466298fc,
  0x1871a4d8, 0xea1a27db, 0xf94ad42f, 0x0b21572c,
  0xdfeb33c7, 0x2d80b0c4, 0x3ed04330, 0xccbbc033,
  0xa24bb5a6, 0x502036a5, 0x4370c551, 0xb11b4652,
  0x65d122b9, 0x97baa1ba, 0x84ea524e, 0x7681d14d,
  0x2892ed69, 0xdaf96e6a, 0xc9a99d9e, 0x3bc21e9d,
  0xef087a76, 0x1d63f975, 0x0e330a81, 0xfc588982,
  0xb21572c9, 0x407ef1ca, 0x532e023e, 0xa145813d,
  0x758fe5d6, 0x87e466d5, 0x94b49521, 0x66df1622,
  0x38cc2a06, 0xcaa7a905, 0xd9f75af1, 0x2b9cd9f2,
  0xff56bd19, 0x0d3d3e1a, 0x1e6dcdee, 0xec064eed,
  0xc38d26c4, 0x31e6a5c7, 0x22b65633, 0xd0ddd530,
  0x0417b1db, 0xf67c32d8, 0xe52cc12c, 0x1747422f,
  0x49547e0b, 0xbb3ffd08, 0xa86f0efc, 0x5a048dff,
  0x8ecee914, 0x7ca56a17, 0x6ff599e3, 0x9d9e1ae0,
  0xd3d3e1ab, 0x21b862a8, 0x32e8915c, 0xc083125f,
  0x144976b4, 0xe622f5b7, 0xf5720643, 0x07198540,
  0x590ab964, 0xab613a67, 0xb831c993, 0x4a5a4a90,
  0x9e902e7b, 0x6cfbad78, 0x7fab5e8c, 0x8dc0dd8f,
  0xe330a81a, 0x115b2b19, 0x020bd8ed, 0xf0605bee,
  0x24aa3f05, 0xd6c1bc06, 0xc5914ff2, 0x37faccf1,
  0x69e9f0d5, 0x9b8273d6, 0x88d28022, 0x7ab90321,
  0xae7367ca, 0x5c18e4c9, 0x4f48173d, 0xbd23943e,
  0xf36e6f75, 0x0105ec76, 0x12551f82, 0xe03e9c81,
  0x34f4f86a, 0xc69f7b69, 0xd5cf889d, 0x27a40b9e,
  0x79b737ba, 0x8bdcb4b9, 0x988c474d, 0x6ae7c44e,
  0xbe2da0a5, 0x4c4623a6, 0x5f16d052, 0xad7d5351
};

static const uint32_t stride_ext_table_0[256] = {
  0x00000000, 0x30d23865, 0x61a470ca, 0x517648af,
  0xc348e194, 0xf39ad9f1, 0xa2ec915e, 0x923ea93b,
  0x837db5d9, 0xb3af8dbc, 0xe2d9c513, 0xd20bfd76,
  0x4035544d, 0x70e76c28, 0x21912487, 0x11431ce2,
  0x03171d43, 0x33c52526, 0x62b36d89, 0x526155ec,
  0xc05ffcd7, 0xf08dc4b2, 0xa1fb8c1d, 0x9129b478,
  0x806aa89a, 0xb0b890ff, 0xe1ced850, 0xd11ce035,
  0x4322490e, 0x73f0716b, 0x228639c4, 0x125401a1,
  0x062e3a86, 0x36fc02e3, 0x678a4a4c, 0x57587229,
  0xc566db12, 0xf5b4e377, 0xa4c2abd8, 0x941093bd,
  0x85538f5f, 0xb581b73a, 0xe4f7ff95, 0xd425c7f0,
  0x461b6ecb, 0x76c956ae, 0x27bf1e01, 0x176d2664,
  0x053927c5, 0x35eb1fa0, 0x649d570f, 0x544f6f6a,
  0xc671c651, 0xf6a3fe34, 0xa7d5b69b, 0x97078efe,
  0x8644921c, 0xb696aa79, 0xe7e0e2d6, 0xd732dab3,
  0x450c7388, 0x75de4bed, 0x24a80342, 0x147a3b27,
  0x0c5c750c, 0x3c8e4d69, 0x6df805c6, 0x5d2a3da3,
  0xcf149498, 0xffc6acfd, 0xaeb0e452, 0x9e62dc37,
  0x8f21c0d5, 0xbff3f8b0, 0xee85b01f, 0xde57887a,
  0x4c692141, 0x7cbb1924, 0x2dcd518b, 0x1d1f69ee,
  0x0f4b684f, 0x3f99502a, 0x6eef1885, 0x5e3d20e0,
  0xcc0389db, 0xfcd1b1be, 0xada7f911, 0x9d75c174,
  0x8c36dd96, 0xbce4e5f3, 0xed92ad5c, 0xdd409539,
  0x4f7e3c02, 0x7fac0467, 0x2eda4cc8, 0x1e0874ad,
  0x0a724f8a, 0x3aa077ef, 0x6bd63f40, 0x5b040725,
  0xc93aae1e, 0xf9e8967b, 0xa89eded4, 0x984ce6b1,
  0x890ffa53, 0xb9ddc236, 0xe8ab8a99, 0xd879b2fc,
  0x4a471bc7, 0x7a9523a2, 0x2be36b0d, 0x1b315368,
  0x096552c9, 0x39b76aac, 0x68c12203, 0x58131a66,
  0xca2db35d, 0xfaff8b38, 0xab89c397, 0x9b5bfbf2,
  0x8a18e710, 0xbacadf75, 0xebbc97da, 0xdb6eafbf,
  0x49500684, 0x79823ee1, 0x28f4764e, 0x18264e2b,
  0x18b8ea18, 0x286ad27d, 0x791c9ad2, 0x49cea2b7,
  0xdbf00b8c, 0xeb2233e9, 0xba547b46, 0x8a864323,
  0x9bc55fc1, 0xab1767a4, 0xfa612f0b, 0xcab3176e,
  0x588dbe55, 0x685f8630, 0x3929ce9f, 0x09fbf6fa,
  0x1baff75b, 0x2b7dcf3e, 0x7a0b8791, 0x4ad9bff4,
  0xd8e716cf, 0xe8352eaa, 0xb9436605, 0x89915e60,
  0x98d24282, 0xa8007ae7, 0xf9763248, 0xc9a40a2d,
  0x5b9aa316, 0x6b489b73, 0x3a3ed3dc, 0x0aecebb9,
  0x1e96d09e, 0x2e44e8fb, 0x7f32a054, 0x4fe09831,
  0xddde310a, 0xed0c096f, 0xbc7a41c0, 0x8ca879a5,
  0x9deb6547, 0xad395d22, 0xfc4f158d, 0xcc9d2de8,
  0x5ea384d3, 0x6e71bcb6, 0x3f07f419, 0x0fd5cc7c,
  0x1d81cddd, 0x2d53f5b8, 0x7c25bd17, 0x4cf78572,
  0xdec92c49, 0xee1b142c, 0xbf6d5c83, 0x8fbf64e6,
  0x9efc7804, 0xae2e4061, 0xff5808ce, 0xcf8a30ab,
  0x5db49990, 0x6d66a1f5, 0x3c10e95a, 0x0cc2d13f,
  0x14e49f14, 0x2436a771, 0x7540efde, 0x4592d7bb,
  0xd7ac7e80, 0xe77e46e5, 0xb6080e4a, 0x86da362f,
  0x97992acd, 0xa74b12a8, 0xf63d5a07, 0xc6ef6262,
  0x54d1cb59, 0x6403f33c, 0x3575bb93, 0x05a783f6,
  0x17f38257, 0x2721ba32, 0x7657f29d, 0x4685caf8,
  0xd4bb63c3, 0xe4695ba6, 0xb51f1309, 0x85cd2b6c,
  0x948e378e, 0xa45c0feb, 0xf52a4744, 0xc5f87f21,
  0x57c6d61a, 0x6714ee7f, 0x3662a6d0, 0x06b09eb5,
  0x12caa592, 0x22189df7, 0x736ed558, 0x43bced3d,
  0xd1824406, 0xe1507c63, 0xb02634cc, 0x80f40ca9,
  0x91b7104b, 0xa165282e, 0xf0136081, 0xc0c158e4,
  0x52fff1df, 0x622dc9ba, 0x335b8115, 0x0389b970,
  0x11ddb8d1, 0x210f80b4, 0x7079c81b, 0x40abf07e,
  0xd2955945, 0xe2476120, 0xb331298f, 0x83e311ea,
  0x92a00d08, 0xa272356d, 0xf3047dc2, 0xc3d645a7,
  0x51e8ec9c, 0x613ad4f9, 0x304c9c56, 0x009ea433
};

static const uint32_t stride_ext_table_1[256] = {
  0x00000000, 0x54075546, 0xa80eaa8c, 0xfc09ffca,
  0x55f123e9, 0x01f676af, 0xfdff8965, 0xa9f8dc23,
  0xabe247d2, 0xffe51294, 0x03eced5e, 0x57ebb818,
  0xfe13643b, 0xaa14317d, 0x561dceb7, 0x021a9bf1,
  0x5228f955, 0x062fac13, 0xfa2653d9, 0xae21069f,
  0x07d9dabc, 0x53de8ffa, 0xafd77030, 0xfbd02576,
  0xf9cabe87, 0xadcdebc1, 0x51c4140b, 0x05c3414d,
  0xac3b9d6e, 0xf83cc828, 0x043537e2, 0x503262a4,
  0xa451f2aa, 0xf056a7ec, 0x0c5f5826, 0x58580d60,
  0xf1a0d143, 0xa5a78405, 0x59ae7bcf, 0x0da92e89,
  0x0fb3b578, 0x5bb4e03e, 0xa7bd1ff4, 0xf3ba4ab2,
  0x5a429691, 0x0e45c3d7, 0xf24c3c1d, 0xa64b695b,
  0xf6790bff, 0xa27e5eb9, 0x5e77a173, 0x0a70f435,
  0xa3882816, 0xf78f7d50, 0x0b86829a, 0x5f81d7dc,
  0x5d9b4c2d, 0x099c196b, 0xf595e6a1, 0xa192b3e7,
  0x086a6fc4, 0x5c6d3a82, 0xa064c548, 0xf463900e,
  0x4d4f93a5, 0x1948c6e3, 0xe5413929, 0xb1466c6f,
  0x18beb04c, 0x4cb9e50a, 0xb0b01ac0, 0xe4b74f86,
  0xe6add477, 0xb2aa8131, 0x4ea37efb, 0x1aa42bbd,
  0xb35cf79e, 0xe75ba2d8, 0x1b525d12, 0x4f550854,
  0x1f676af0, 0x4b603fb6, 0xb769c07c, 0xe36e953a,
  0x4a964919, 0x1e911c5f, 0xe298e395, 0xb69fb6d3,
  0xb4852d22, 0xe0827864, 0x1c8b87ae, 0x488cd2e8,
  0xe1740ecb, 0xb5735b8d, 0x497aa447, 0x1d7df101,
  0xe91e610f, 0xbd193449, 0x4110cb83, 0x15179ec5,
  0xbcef42e6, 0xe8e817a0, 0x14e1e86a, 0x40e6bd2c,
  0x42fc26dd, 0x16fb739b, 0xeaf28c51, 0xbef5d917,
  0x170d0534, 0x430a5072, 0xbf03afb8, 0xeb04fafe,
  0xbb36985a, 0xef31cd1c, 0x133832d6, 0x473f6790,
  0xeec7bbb3, 0xbac0eef5, 0x46c9113f, 0x12ce4479,
  0x10d4df88, 0x44d38ace, 0xb8da7504, 0xecdd2042,
  0x4525fc61, 0x1122a927, 0xed2b56ed, 0xb92c03ab,
  0x9a9f274a, 0xce98720c, 0x32918dc6, 0x6696d880,
  0xcf6e04a3, 0x9b6951e5, 0x6760ae2f, 0x3367fb69,
  0x317d6098, 0x657a35de, 0x9973ca14, 0xcd749f52,
  0x648c4371, 0x308b1637, 0xcc82e9fd, 0x9885bcbb,
  0xc8b7de1f, 0x9cb08b59, 0x60b97493, 0x34be21d5,
  0x9d46fdf6, 0xc941a8b0, 0x3548577a, 0x614f023c,
  0x635599cd, 0x3752cc8b, 0xcb5b3341, 0x9f5c6607,
  0x36a4ba24, 0x62a3ef62, 0x9eaa10a8, 0xcaad45ee,
  0x3eced5e0, 0x6ac980a6, 0x96c07f6c, 0xc2c72a2a,
  0x6b3ff609, 0x3f38a34f, 0xc3315c85, 0x973609c3,
  0x952c9232, 0xc12bc774, 0x3d2238be, 0x69256df8,
  0xc0ddb1db, 0x94dae49d, 0x68d31b57, 0x3cd44e11,
  0x6ce62cb5, 0x38e179f3, 0xc4e88639, 0x90efd37f,
  0x39170f5c, 0x6d105a1a, 0x9119a5d0, 0xc51ef096,
  0xc7046b67, 0x93033e21, 0x6f0ac1eb, 0x3b0d94ad,
  0x92f5488e, 0xc6f21dc8, 0x3afbe202, 0x6efcb744,
  0xd7d0b4ef, 0x83d7e1a9, 0x7fde1e63, 0x2bd94b25,
  0x82219706, 0xd626c240, 0x2a2f3d8a, 0x7e2868cc,
  0x7c32f33d, 0x2835a67b, 0xd43c59b1, 0x803b0cf7,
  0x29c3d0d4, 0x7dc48592, 0x81cd7a58, 0xd5ca2f1e,
  0x85f84dba, 0xd1ff18fc, 0x2df6e736, 0x79f1b270,
  0xd0096e53, 0x840e3b15, 0x7807c4df, 0x2c009199,
  0x2e1a0a68, 0x7a1d5f2e, 0x8614a0e4, 0xd213f5a2,
  0x7beb2981, 0x2fec7cc7, 0xd3e5830d, 0x87e2d64b,
  0x73814645, 0x27861303, 0xdb8fecc9, 0x8f88b98f,
  0x267065ac, 0x727730ea, 0x8e7ecf20, 0xda799a66,
  0xd8630197, 0x8c6454d1, 0x706dab1b, 0x246afe5d,
  0x8d92227e, 0xd9957738, 0x259c88f2, 0x719bddb4,
  0x21a9bf10, 0x75aeea56, 0x89a7159c, 0xdda040da,
  0x74589cf9, 0x205fc9bf, 0xdc563675, 0x88516333,
  0x8a4bf8c2, 0xde4cad84, 0x2245524e, 0x76420708,
  0xdfbadb2b, 0x8bbd8e6d, 0x77b471a7, 0x23b324e1
};

static const uint32_t stride_ext_table_2[256] = {
  0x00000000, 0x678efd01, 0xcf1dfa02, 0xa8930703,
  0x9bd782f5, 0xfc597ff4, 0x54ca78f7, 0x334485f6,
  0x3243731b, 0x55cd8e1a, 0xfd5e8919, 0x9ad07418,
  0xa994f1ee, 0xce1a0cef, 0x66890bec, 0x0107f6ed,
  0x6486e636, 0x03081b37, 0xab9b1c34, 0xcc15e135,
  0xff5164c3, 0x98df99c2, 0x304c9ec1, 0x57c263c0,
  0x56c5952d, 0x314b682c, 0x99d86f2f, 0xfe56922e,
  0xcd1217d8, 0xaa9cead9, 0x020fedda, 0x658110db,
  0xc90dcc6c, 0xae83316d, 0x0610366e, 0x619ecb6f,
  0x52da4e99, 0x3554b398, 0x9dc7b49b, 0xfa49499a,
  0xfb4ebf77, 0x9cc04276, 0x34534575, 0x53ddb874,
  0x60993d82, 0x0717c083, 0xaf84c780, 0xc80a3a81,
  0xad8b2a5a, 0xca05d75b, 0x6296d058, 0x05182d59,
  0x365ca8af, 0x51d255ae, 0xf94152ad, 0x9ecfafac,
  0x9fc85941, 0xf846a440, 0x50d5a343, 0x375b5e42,
  0x041fdbb4, 0x639126b5, 0xcb0221b6, 0xac8cdcb7,
  0x97f7ee29, 0xf0791328, 0x58ea142b, 0x3f64e92a,
  0x0c206cdc, 0x6bae91dd, 0xc33d96de, 0xa4b36bdf,
  0xa5b49d32, 0xc23a6033, 0x6aa96730, 0x0d279a31,
  0x3e631fc7, 0x59ede2c6, 0xf17ee5c5, 0x96f018c4,
  0xf371081f, 0x94fff51e, 0x3c6cf21d, 0x5be20f1c,
  0x68a68aea, 0x0f2877eb, 0xa7bb70e8, 0xc0358de9,
  0xc1327b04, 0xa6bc8605, 0x0e2f8106, 0x69a17c07,
  0x5ae5f9f1, 0x3d6b04f0, 0x95f803f3, 0xf276fef2,
  0x5efa2245, 0x3974df44, 0x91e7d847, 0xf6692546,
  0xc52da0b0, 0xa2a35db1, 0x0a305ab2, 0x6dbea7b3,
  0x6cb9515e, 0x0b37ac5f, 0xa3a4ab5c, 0xc42a565d,
  0xf76ed3ab, 0x90e02eaa, 0x387329a9, 0x5ffdd4a8,
  0x3a7cc473, 0x5df23972, 0xf5613e71, 0x92efc370,
  0xa1ab4686, 0xc625bb87, 0x6eb6bc84, 0x09384185,
  0x083fb768, 0x6fb14a69, 0xc7224d6a, 0xa0acb06b,
  0x93e8359d, 0xf466c89c, 0x5cf5cf9f, 0x3b7b329e,
  0x2a03aaa3, 0x4d8d57a2, 0xe51e50a1, 0x8290ada0,
  0xb1d42856, 0xd65ad557, 0x7ec9d254, 0x19472f55,
  0x1840d9b8, 0x7fce24b9, 0xd75d23ba, 0xb0d3debb,
  0x83975b4d, 0xe419a64c, 0x4c8aa14f, 0x2b045c4e,
  0x4e854c95, 0x290bb194, 0x8198b697, 0xe6164b96,
  0xd552ce60, 0xb2dc3361, 0x1a4f3462, 0x7dc1c963,
  0x7cc63f8e, 0x1b48c28f, 0xb3dbc58c, 0xd455388d,
  0xe711bd7b, 0x809f407a, 0x280c4779, 0x4f82ba78,
  0xe30e66cf, 0x84809bce, 0x2c139ccd, 0x4b9d61cc,
  0x78d9e43a, 0x1f57193b, 0xb7c41e38, 0xd04ae339,
  0xd14d15d4, 0xb6c3e8d5, 0x1e50efd6, 0x79de12d7,
  0x4a9a9721, 0x2d146a20, 0x85876d23, 0xe2099022,
  0x878880f9, 0xe0067df8, 0x48957afb, 0x2f1b87fa,
  0x1c5f020c, 0x7bd1ff0d, 0xd342f80e, 0xb4cc050f,
  0xb5cbf3e2, 0xd2450ee3, 0x7ad609e0, 0x1d58f4e1,
  0x2e1c7117, 0x49928c16, 0xe1018b15, 0x868f7614,
  0xbdf4448a, 0xda7ab98b, 0x72e9be88, 0x15674389,
  0x2623c67f, 0x41ad3b7e, 0xe93e3c7d, 0x8eb0c17c,
  0x8fb73791, 0xe839ca90, 0x40aacd93, 0x27243092,
  0x1460b564, 0x73ee4865, 0xdb7d4f66, 0xbcf3b267,
  0xd972a2bc, 0xbefc5fbd, 0x166f58be, 0x71e1a5bf,
  0x42a52049, 0x252bdd48, 0x8db8da4b, 0xea36274a,
  0xeb31d1a7, 0x8cbf2ca6, 0x242c2ba5, 0x43a2d6a4,
  0x70e65352, 0x1768ae53, 0xbffba950, 0xd8755451,
  0x74f988e6, 0x137775e7, 0xbbe472e4, 0xdc6a8fe5,
  0xef2e0a13, 0x88a0f712, 0x2033f011, 0x47bd0d10,
  0x46bafbfd, 0x213406fc, 0x89a701ff, 0xee29fcfe,
  0xdd6d7908, 0xbae38409, 0x1270830a, 0x75fe7e0b,
  0x107f6ed0, 0x77f193d1, 0xdf6294d2, 0xb8ec69d3,
  0x8ba8ec25, 0xec261124, 0x44b51627, 0x233beb26,
  0x223c1dcb, 0x45b2e0ca, 0xed21e7c9, 0x8aaf1ac8,
  0xb9eb9f3e, 0xde65623f, 0x76f6653c, 0x1178983d
};

static const uint32_t stride_ext_table_3[256] = {
  0x00000000, 0xf20c0dfe, 0xe1f46d0d, 0x13f860f3,
  0xc604aceb, 0x3408a115, 0x27f0c1e6, 0xd5fccc18,
  0x89e52f27, 0x7be922d9, 0x6811422a, 0x9a1d4fd4,
  0x4fe183cc, 0xbded8e32, 0xae15eec1, 0x5c19e33f,
  0x162628bf, 0xe42a2541, 0xf7d245b2, 0x05de484c,
  0xd0228454, 0x222e89aa, 0x31d6e959, 0xc3dae4a7,
  0x9fc30798, 0x6dcf0a66, 0x7e376a95, 0x8c3b676b,
  0x59c7ab73, 0xabcba68d, 0xb833c67e, 0x4a3fcb80,
  0x2c4c517e, 0xde405c80, 0xcdb83c73, 0x3fb4318d,
  0xea48fd95, 0x1844f06b, 0x0bbc9098, 0xf9b09d66,
  0xa5a97e59, 0x57a573a7, 0x445d1354, 0xb6511eaa,
  0x63add2b2, 0x91a1df4c, 0x8259bfbf, 0x7055b241,
  0x3a6a79c1, 0xc866743f, 0xdb9e14cc, 0x29921932,
  0xfc6ed52a, 0x0e62d8d4, 0x1d9ab827, 0xef96b5d9,
  0xb38f56e6, 0x41835b18, 0x527b3beb, 0xa0773615,
  0x758bfa0d, 0x8787f7f3, 0x947f9700, 0x66739afe,
  0x5898a2fc, 0xaa94af02, 0xb96ccff1, 0x4b60c20f,
  0x9e9c0e17, 0x6c9003e9, 0x7f68631a, 0x8d646ee4,
  0xd17d8ddb, 0x23718025, 0x3089e0d6, 0xc285ed28,
  0x17792130, 0xe5752cce, 0xf68d4c3d, 0x048141c3,
  0x4ebe8a43, 0xbcb287bd, 0xaf4ae74e, 0x5d46eab0,
  0x88ba26a8, 0x7ab62b56, 0x694e4ba5, 0x9b42465b,
  0xc75ba564, 0x3557a89a, 0x26afc869, 0xd4a3c597,
  0x015f098f, 0xf3530471, 0xe0ab6482, 0x12a7697c,
  0x74d4f382, 0x86d8fe7c, 0x95209e8f, 0x672c9371,
  0xb2d05f69, 0x40dc5297, 0x53243264, 0xa1283f9a,
  0xfd31dca5, 0x0f3dd15b, 0x1cc5b1a8, 0xeec9bc56,
  0x3b35704e, 0xc9397db0, 0xdac11d43, 0x28cd10bd,
  0x62f2db3d, 0x90fed6c3, 0x8306b630, 0x710abbce,
  0xa4f677d6, 0x56fa7a28, 0x45021adb, 0xb70e1725,
  0xeb17f41a, 0x191bf9e4, 0x0ae39917, 0xf8ef94e9,
  0x2d1358f1, 0xdf1f550f, 0xcce735fc, 0x3eeb3802,
  0xb13145f8, 0x433d4806, 0x50c528f5, 0xa2c9250b,
  0x7735e913, 0x8539e4ed, 0x96c1841e, 0x64cd89e0,
  0x38d46adf, 0xcad86721, 0xd92007d2, 0x2b2c0a2c,
  0xfed0c634, 0x0cdccbca, 0x1f24ab39, 0xed28a6c7,
  0xa7176d47, 0x551b60b9, 0x46e3004a, 0xb4ef0db4,
  0x6113c1ac, 0x931fcc52, 0x80e7aca1, 0x72eba15f,
  0x2ef24260, 0xdcfe4f9e, 0xcf062f6d, 0x3d0a2293,
  0xe8f6ee8b, 0x1afae375, 0x09028386, 0xfb0e8e78,
  0x9d7d1486, 0x6f711978, 0x7c89798b, 0x8e857475,
  0x5b79b86d, 0xa975b593, 0xba8dd560, 0x4881d89e,
  0x14983ba1, 0xe694365f, 0xf56c56ac, 0x07605b52,
  0xd29c974a, 0x20909ab4, 0x3368fa47, 0xc164f7b9,
  0x8b5b3c39, 0x795731c7, 0x6aaf5134, 0x98a35cca,
  0x4d5f90d2, 0xbf539d2c, 0xacabfddf, 0x5ea7f021,
  0x02be131e, 0xf0b21ee0, 0xe34a7e13, 0x114673ed,
  0xc4babff5, 0x36b6b20b, 0x254ed2f8, 0xd742df06,
  0xe9a9e704, 0x1ba5eafa, 0x085d8a09, 0xfa5187f7,
  0x2fad4bef, 0xdda14611, 0xce5926e2, 0x3c552b1c,
  0x604cc823, 0x9240c5dd, 0x81b8a52e, 0x73b4a8d0,
  0xa64864c8, 0x54446936, 0x47bc09c5, 0xb5b0043b,
  0xff8fcfbb, 0x0d83c245, 0x1e7ba2b6, 0xec77af48,
  0x398b6350, 0xcb876eae, 0xd87f0e5d, 0x2a7303a3,
  0x766ae09c, 0x8466ed62, 0x979e8d91, 0x6592806f,
  0xb06e4c77, 0x42624189, 0x519a217a, 0xa3962c84,
  0xc5e5b67a, 0x37e9bb84, 0x2411db77, 0xd61dd689,
  0x03e11a91, 0xf1ed176f, 0xe215779c, 0x10197a62,
  0x4c00995d, 0xbe0c94a3, 0xadf4f450, 0x5ff8f9ae,
  0x8a0435b6, 0x78083848, 0x6bf058bb, 0x99fc5545,
  0xd3c39ec5, 0x21cf933b, 0x3237f3c8, 0xc03bfe36,
  0x15c7322e, 0xe7cb3fd0, 0xf4335f23, 0x063f52dd,
  0x5a26b1e2, 0xa82abc1c, 0xbbd2dcef, 0x49ded111,
  0x9c221d09, 0x6e2e10f7, 0x7dd67004, 0x8fda7dfa
};

/* CRCs are pre- and post- conditioned by xoring with all ones. */
#define CRC32_XOR UINT32_C(0xffffffff)
#define PREFETCH_HORIZON 256

#ifdef HAVE_X64_CRC

#ifdef HAVE_PREFETCH
static const uint32_t block0_skip_table[8][16] = {
  {0x00000000, 0xff770459, 0xfb027e43, 0x04757a1a,
   0xf3e88a77, 0x0c9f8e2e, 0x08eaf434, 0xf79df06d,
   0xe23d621f, 0x1d4a6646, 0x193f1c5c, 0xe6481805,
   0x11d5e868, 0xeea2ec31, 0xead7962b, 0x15a09272},
  {0x00000000, 0xc196b2cf, 0x86c1136f, 0x4757a1a0,
   0x086e502f, 0xc9f8e2e0, 0x8eaf4340, 0x4f39f18f,
   0x10dca05e, 0xd14a1291, 0x961db331, 0x578b01fe,
   0x18b2f071, 0xd92442be, 0x9e73e31e, 0x5fe551d1},
  {0x00000000, 0x21b940bc, 0x43728178, 0x62cbc1c4,
   0x86e502f0, 0xa75c424c, 0xc5978388, 0xe42ec334,
   0x08267311, 0x299f33ad, 0x4b54f269, 0x6aedb2d5,
   0x8ec371e1, 0xaf7a315d, 0xcdb1f099, 0xec08b025},
  {0x00000000, 0x104ce622, 0x2099cc44, 0x30d52a66,
   0x41339888, 0x517f7eaa, 0x61aa54cc, 0x71e6b2ee,
   0x82673110, 0x922bd732, 0xa2fefd54, 0xb2b21b76,
   0xc354a998, 0xd3184fba, 0xe3cd65dc, 0xf38183fe},
  {0x00000000, 0x012214d1, 0x024429a2, 0x03663d73,
   0x04885344, 0x05aa4795, 0x06cc7ae6, 0x07ee6e37,
   0x0910a688, 0x0832b259, 0x0b548f2a, 0x0a769bfb,
   0x0d98f5cc, 0x0cbae11d, 0x0fdcdc6e, 0x0efec8bf},
  {0x00000000, 0x12214d10, 0x24429a20, 0x3663d730,
   0x48853440, 0x5aa47950, 0x6cc7ae60, 0x7ee6e370,
   0x910a6880, 0x832b2590, 0xb548f2a0, 0xa769bfb0,
   0xd98f5cc0, 0xcbae11d0, 0xfdcdc6e0, 0xefec8bf0},
  {0x00000000, 0x27f8a7f1, 0x4ff14fe2, 0x6809e813,
   0x9fe29fc4, 0xb81a3835, 0xd013d026, 0xf7eb77d7,
   0x3a294979, 0x1dd1ee88, 0x75d8069b, 0x5220a16a,
   0xa5cbd6bd, 0x8233714c, 0xea3a995f, 0xcdc23eae},
  {0x00000000, 0x745292f2, 0xe8a525e4, 0x9cf7b716,
   0xd4a63d39, 0xa0f4afcb, 0x3c0318dd, 0x48518a2f,
   0xaca00c83, 0xd8f29e71, 0x44052967, 0x3057bb95,
   0x780631ba, 0x0c54a348, 0x90a3145e, 0xe4f186ac}
};
#endif

static const uint32_t block1_skip_table[8][16] = {
  {0x00000000, 0x79113270, 0xf22264e0, 0x8b335690,
   0xe1a8bf31, 0x98b98d41, 0x138adbd1, 0x6a9be9a1,
   0xc6bd0893, 0xbfac3ae3, 0x349f6c73, 0x4d8e5e03,
   0x2715b7a2, 0x5e0485d2, 0xd537d342, 0xac26e132},
  {0x00000000, 0x889667d7, 0x14c0b95f, 0x9c56de88,
   0x298172be, 0xa1171569, 0x3d41cbe1, 0xb5d7ac36,
   0x5302e57c, 0xdb9482ab, 0x47c25c23, 0xcf543bf4,
   0x7a8397c2, 0xf215f015, 0x6e432e9d, 0xe6d5494a},
  {0x00000000, 0xa605caf8, 0x49e7e301, 0xefe229f9,
   0x93cfc602, 0x35ca0cfa, 0xda282503, 0x7c2deffb,
   0x2273faf5, 0x8476300d, 0x6b9419f4, 0xcd91d30c,
   0xb1bc3cf7, 0x17b9f60f, 0xf85bdff6, 0x5e5e150e},
  {0x00000000, 0x44e7f5ea, 0x89cfebd4, 0xcd281e3e,
   0x1673a159, 0x529454b3, 0x9fbc4a8d, 0xdb5bbf67,
   0x2ce742b2, 0x6800b758, 0xa528a966, 0xe1cf5c8c,
   0x3a94e3eb, 0x7e731601, 0xb35b083f, 0xf7bcfdd5},
  {0x00000000, 0x59ce8564, 0xb39d0ac8, 0xea538fac,
   0x62d66361, 0x3b18e605, 0xd14b69a9, 0x8885eccd,
   0xc5acc6c2, 0x9c6243a6, 0x7631cc0a, 0x2fff496e,
   0xa77aa5a3, 0xfeb420c7, 0x14e7af6b, 0x4d292a0f},
  {0x00000000, 0x8eb5fb75, 0x1887801b, 0x96327b6e,
   0x310f0036, 0xbfbafb43, 0x2988802d, 0xa73d7b58,
   0x621e006c, 0xecabfb19, 0x7a998077, 0xf42c7b02,
   0x5311005a, 0xdda4fb2f, 0x4b968041, 0xc5237b34},
  {0x00000000, 0xc43c00d8, 0x8d947741, 0x49a87799,
   0x1ec49873, 0xdaf898ab, 0x9350ef32, 0x576cefea,
   0x3d8930e6, 0xf9b5303e, 0xb01d47a7, 0x7421477f,
   0x234da895, 0xe771a84d, 0xaed9dfd4, 0x6ae5df0c},
  {0x00000000, 0x7b1261cc, 0xf624c398, 0x8d36a254,
   0xe9a5f1c1, 0x92b7900d, 0x1f813259, 0x64935395,
   0xd6a79573, 0xadb5f4bf, 0x208356eb, 0x5b913727,
   0x3f0264b2, 0x4410057e, 0xc926a72a, 0xb234c6e6}
};

static const uint32_t block2_skip_table[8][16] = {
  {0x00000000, 0x8f158014, 0x1bc776d9, 0x94d2f6cd,
   0x378eedb2, 0xb89b6da6, 0x2c499b6b, 0xa35c1b7f,
   0x6f1ddb64, 0xe0085b70, 0x74daadbd, 0xfbcf2da9,
   0x589336d6, 0xd786b6c2, 0x4354400f, 0xcc41c01b},
  {0x00000000, 0xde3bb6c8, 0xb99b1b61, 0x67a0ada9,
   0x76da4033, 0xa8e1f6fb, 0xcf415b52, 0x117aed9a,
   0xedb48066, 0x338f36ae, 0x542f9b07, 0x8a142dcf,
   0x9b6ec055, 0x4555769d, 0x22f5db34, 0xfcce6dfc},
  {0x00000000, 0xde85763d, 0xb8e69a8b, 0x6663ecb6,
   0x742143e7, 0xaaa435da, 0xccc7d96c, 0x1242af51,
   0xe84287ce, 0x36c7f1f3, 0x50a41d45, 0x8e216b78,
   0x9c63c429, 0x42e6b214, 0x24855ea2, 0xfa00289f},
  {0x00000000, 0xd569796d, 0xaf3e842b, 0x7a57fd46,
   0x5b917ea7, 0x8ef807ca, 0xf4affa8c, 0x21c683e1,
   0xb722fd4e, 0x624b8423, 0x181c7965, 0xcd750008,
   0xecb383e9, 0x39dafa84, 0x438d07c2, 0x96e47eaf},
  {0x00000000, 0x6ba98c6d, 0xd75318da, 0xbcfa94b7,
   0xab4a4745, 0xc0e3cb28, 0x7c195f9f, 0x17b0d3f2,
   0x5378f87b, 0x38d17416, 0x842be0a1, 0xef826ccc,
   0xf832bf3e, 0x939b3353, 0x2f61a7e4, 0x44c82b89},
  {0x00000000, 0xa6f1f0f6, 0x480f971d, 0xeefe67eb,
   0x901f2e3a, 0x36eedecc, 0xd810b927, 0x7ee149d1,
   0x25d22a85, 0x8323da73, 0x6dddbd98, 0xcb2c4d6e,
   0xb5cd04bf, 0x133cf449, 0xfdc293a2, 0x5b336354},
  {0x00000000, 0x4ba4550a, 0x9748aa14, 0xdcecff1e,
   0x2b7d22d9, 0x60d977d3, 0xbc3588cd, 0xf791ddc7,
   0x56fa45b2, 0x1d5e10b8, 0xc1b2efa6, 0x8a16baac,
   0x7d87676b, 0x36233261, 0xeacfcd7f, 0xa16b9875},
  {0x00000000, 0xadf48b64, 0x5e056039, 0xf3f1eb5d,
   0xbc0ac072, 0x11fe4b16, 0xe20fa04b, 0x4ffb2b2f,
   0x7df9f615, 0xd00d7d71, 0x23fc962c, 0x8e081d48,
   0xc1f33667, 0x6c07bd03, 0x9ff6565e, 0x3202dd3a}
};

#define BLOCK_GROUPS 3
#define BLOCK0_SIZE (16 * 1024 / BLOCK_GROUPS / 64 * 64)
#define BLOCK1_SIZE (4 * 1024 / BLOCK_GROUPS / 8 * 8)
#define BLOCK2_SIZE (1024 / BLOCK_GROUPS / 8 * 8)

#endif /* HAVE_X64_CRC */

/*
 * Helpers
 */

/* Returns the smallest address >= the given address that is aligned to N bytes.
 *
 * N must be a power of two.
 */
static LDB_INLINE const void *
round_up(const void *p, uintptr_t N) {
  return (const void *)(((uintptr_t)p + (N - 1)) & ~(N - 1));
}

/*
 * CRC32C (Generic)
 */

static uint32_t
crc32c_generic(uint32_t z, const uint8_t *xp, size_t xn) {
  const uint8_t *p = xp;
  const uint8_t *e = p + xn;
  uint32_t l = z ^ CRC32_XOR;

/* Process one byte at a time. */
#define STEP1 do {                  \
  int c = (l & 0xff) ^ *p++;        \
  l = byte_ext_table[c] ^ (l >> 8); \
} while (0)

/* Process one of the 4 strides of 4-byte data. */
#define STEP4(s) do {                                  \
  crc##s = ldb_fixed32_decode(p + s * 4) ^             \
           stride_ext_table_3[crc##s & 0xff] ^         \
           stride_ext_table_2[(crc##s >> 8) & 0xff] ^  \
           stride_ext_table_1[(crc##s >> 16) & 0xff] ^ \
           stride_ext_table_0[crc##s >> 24];           \
} while (0)

/* Process a 16-byte swath of 4 strides, each of which has 4 bytes of data. */
#define STEP16 do { \
  STEP4(0);         \
  STEP4(1);         \
  STEP4(2);         \
  STEP4(3);         \
  p += 16;          \
} while (0)

/* Process 4 bytes that were already loaded into a word. */
#define STEP4W(w) do {                       \
  w ^= l;                                    \
                                             \
  for (i = 0; i < 4; i++)                    \
    w = (w >> 8) ^ byte_ext_table[w & 0xff]; \
                                             \
  l = w;                                     \
} while (0)

  /* Point x at first 4-byte aligned byte in the buffer.
     This might be past the end of the buffer. */
  const uint8_t *x = round_up(p, 4);

  if (x <= e) {
    /* Process bytes p is 4-byte aligned. */
    while (p != x)
      STEP1;
  }

  if ((e - p) >= 16) {
    /* Load a 16-byte swath into the stride partial results. */
    uint32_t crc0 = ldb_fixed32_decode(p + 0 * 4) ^ l;
    uint32_t crc1 = ldb_fixed32_decode(p + 1 * 4);
    uint32_t crc2 = ldb_fixed32_decode(p + 2 * 4);
    uint32_t crc3 = ldb_fixed32_decode(p + 3 * 4);
    uint32_t tmp;
    size_t i;

    p += 16;

#ifdef HAVE_PREFETCH
    while ((e - p) > PREFETCH_HORIZON) {
      request_prefetch(p + PREFETCH_HORIZON);

      /* Process 64 bytes at a time. */
      STEP16;
      STEP16;
      STEP16;
      STEP16;
    }
#endif

    /* Process one 16-byte swath at a time. */
    while ((e - p) >= 16)
      STEP16;

    /* Advance one word at a time as far as possible. */
    while ((e - p) >= 4) {
      STEP4(0);

      tmp = crc0;
      crc0 = crc1;
      crc1 = crc2;
      crc2 = crc3;
      crc3 = tmp;

      p += 4;
    }

    /* Combine the 4 partial stride results. */
    l = 0;

    STEP4W(crc0);
    STEP4W(crc1);
    STEP4W(crc2);
    STEP4W(crc3);
  }

  /* Process the last few bytes. */
  while (p != e)
    STEP1;

#undef STEP1
#undef STEP4
#undef STEP16
#undef STEP4W

  return l ^ CRC32_XOR;
}

static uint32_t
(*crc32c_extend)(uint32_t, const uint8_t *, size_t) = &crc32c_generic;

/*
 * CRC32C (Hardware)
 */

#if defined(HAVE_X64_CRC)

#define asm_load64(p) (*((const uint64_t *)(const void *)(p)))

#if defined(HAVE_X64_INTRIN)
#define asm_crc32_u8(z, x) ((z) = _mm_crc32_u8(z, x))
#define asm_crc32_u64(z, x) ((z) = _mm_crc32_u64(z, x))
#elif defined(HAVE_X64_INSTR)
#define asm_crc32_u8(z, x) \
  __asm__ __volatile__ (   \
    "crc32b %b1, %q0\n"    \
    : "+r" (z)             \
    : "rm" (x)             \
  )
#define asm_crc32_u64(z, x) \
  __asm__ __volatile__ (    \
    "crc32q %q1, %q0\n"     \
    : "+r" (z)              \
    : "rm" (x)              \
  )
#else /* !HAVE_X64_INSTR */
#define asm_crc32_u8(z, x)                       \
  __asm__ __volatile__ (                         \
    /* crc32 %dl, %rax */                        \
    ".byte 0xf2, 0x48, 0x0f, 0x38, 0xf0, 0xc2\n" \
    : "+a" (z)                                   \
    : "d" (x)                                    \
  )
#define asm_crc32_u64(z, x)                      \
  __asm__ __volatile__ (                         \
    /* crc32 %rdx, %rax */                       \
    ".byte 0xf2, 0x48, 0x0f, 0x38, 0xf1, 0xc2\n" \
    : "+a" (z)                                   \
    : "d" (x)                                    \
  )
#endif /* !HAVE_X64_INSTR */

static uint32_t
crc32c_sse42(uint32_t z, const uint8_t *xp, size_t xn) {
  const uint8_t *p = xp;
  const uint8_t *e = xp + xn;
  uint32_t l = z ^ CRC32_XOR;
  uint64_t l64;

#define STEP1 do {     \
  asm_crc32_u8(l, *p); \
  p++;                 \
} while (0)

#define STEP8(crc, data) do {           \
  asm_crc32_u64(crc, asm_load64(data)); \
  data += 8;                            \
} while (0)

#define STEP8X3(crc0, crc1, crc2, bs) do {     \
  asm_crc32_u64(crc0, asm_load64(p));          \
  asm_crc32_u64(crc1, asm_load64(p + bs));     \
  asm_crc32_u64(crc2, asm_load64(p + 2 * bs)); \
  p += 8;                                      \
} while (0)

#define SKIP_BLOCK(crc, tab) do {                               \
  crc = tab[0][crc & 0xf] ^ tab[1][(crc >> 4) & 0xf] ^          \
        tab[2][(crc >> 8) & 0xf] ^ tab[3][(crc >> 12) & 0xf] ^  \
        tab[4][(crc >> 16) & 0xf] ^ tab[5][(crc >> 20) & 0xf] ^ \
        tab[6][(crc >> 24) & 0xf] ^ tab[7][(crc >> 28) & 0xf];  \
} while (0)

  /* Point x at first 8-byte aligned byte in the buffer.
     This might be past the end of the buffer. */
  const uint8_t *x = round_up(p, 8);

  if (x <= e) {
    /* Process bytes p is 8-byte aligned. */
    while (p != x)
      STEP1;
  }

  /* Process the data in predetermined block sizes with tables for quickly
     combining the checksum. Experimentally it's better to use larger block
     sizes where possible so use a hierarchy of decreasing block sizes. */
  l64 = l;

#ifdef HAVE_PREFETCH
  while ((e - p) >= BLOCK_GROUPS * BLOCK0_SIZE /* 16384 */) {
    uint64_t l641 = 0;
    uint64_t l642 = 0;
    int i;

    for (i = 0; i < BLOCK0_SIZE; i += 8 * 8) {
      /* Prefetch ahead to hide latency. */
      request_prefetch(p + PREFETCH_HORIZON);
      request_prefetch(p + BLOCK0_SIZE + PREFETCH_HORIZON);
      request_prefetch(p + 2 * BLOCK0_SIZE + PREFETCH_HORIZON);

      /* Process 64 bytes at a time. */
      STEP8X3(l64, l641, l642, BLOCK0_SIZE);
      STEP8X3(l64, l641, l642, BLOCK0_SIZE);
      STEP8X3(l64, l641, l642, BLOCK0_SIZE);
      STEP8X3(l64, l641, l642, BLOCK0_SIZE);
      STEP8X3(l64, l641, l642, BLOCK0_SIZE);
      STEP8X3(l64, l641, l642, BLOCK0_SIZE);
      STEP8X3(l64, l641, l642, BLOCK0_SIZE);
      STEP8X3(l64, l641, l642, BLOCK0_SIZE);
    }

    /* Combine results. */
    SKIP_BLOCK(l64, block0_skip_table);
    l64 ^= l641;
    SKIP_BLOCK(l64, block0_skip_table);
    l64 ^= l642;
    p += (BLOCK_GROUPS - 1) * BLOCK0_SIZE;
  }
#endif

  while ((e - p) >= BLOCK_GROUPS * BLOCK1_SIZE /* 4096 */) {
    uint64_t l641 = 0;
    uint64_t l642 = 0;
    int i;

    for (i = 0; i < BLOCK1_SIZE; i += 8)
      STEP8X3(l64, l641, l642, BLOCK1_SIZE);

    SKIP_BLOCK(l64, block1_skip_table);
    l64 ^= l641;
    SKIP_BLOCK(l64, block1_skip_table);
    l64 ^= l642;
    p += (BLOCK_GROUPS - 1) * BLOCK1_SIZE;
  }

  while ((e - p) >= BLOCK_GROUPS * BLOCK2_SIZE /* 1024 */) {
    uint64_t l641 = 0;
    uint64_t l642 = 0;
    int i;

    for (i = 0; i < BLOCK2_SIZE; i += 8)
      STEP8X3(l64, l641, l642, BLOCK2_SIZE);

    SKIP_BLOCK(l64, block2_skip_table);
    l64 ^= l641;
    SKIP_BLOCK(l64, block2_skip_table);
    l64 ^= l642;
    p += (BLOCK_GROUPS - 1) * BLOCK2_SIZE;
  }

  /* Process bytes 16 at a time. */
  while ((e - p) >= 16) {
    STEP8(l64, p);
    STEP8(l64, p);
  }

  l = (uint32_t)l64;

  /* Process the last few bytes. */
  while (p != e)
    STEP1;

#undef STEP1
#undef STEP8
#undef STEP8X3
#undef SKIP_BLOCK

  return l ^ CRC32_XOR;
}

static int
has_sse42(void) {
#if defined(HAVE_X64_INTRIN)
  unsigned int regs[4];
  __cpuid((int *)regs, 1);
  return (regs[2] >> 20) & 1;
#elif defined(HAVE_X64_ASM)
  uint64_t a = 1;
  uint64_t c = 0;
  uint64_t b;

  __asm__ __volatile__ (
    "xchgq %%rbx, %q1\n"
    "cpuid\n"
    "xchgq %%rbx, %q1\n"
    : "+a" (a), "=&r" (b),
      "+c" (c)
    :: "rdx"
  );

  return (c >> 20) & 1;
#else
  return 0;
#endif
}

static int
can_accelerate(void) {
  static const uint8_t buf[] = "TestCRCBuffer";
  return crc32c_sse42(0, buf, sizeof(buf) - 1) == 0xdcbc59fa;
}

int
ldb_crc32c_init(void) {
  static volatile int result = -1;
  static ldb_spinlock_t lock = 0;

  ldb_spinlock(&lock);

  if (result == -1) {
    if (has_sse42() && can_accelerate()) {
      crc32c_extend = &crc32c_sse42;
      result = 1;
    } else {
      result = 0;
    }
  }

  ldb_spinunlock(&lock);

  return result;
}

#elif defined(HAVE_ARM64_CRC)

#define KBYTES 1032
#define SEGMENTBYTES 256

#define cast16(p) ((const uint16_t *)(const void *)(p))
#define cast32(p) ((const uint32_t *)(const void *)(p))
#define cast64(p) ((const uint64_t *)(const void *)(p))

/* Compute 8bytes for each segment parallelly. */
#define CRC32C32BYTES(P, IND) do {                        \
  crc1 = __crc32cd(                                       \
    crc1, *(cast64(P) + (SEGMENTBYTES / 8) * 1 + (IND))); \
  crc2 = __crc32cd(                                       \
    crc2, *(cast64(P) + (SEGMENTBYTES / 8) * 2 + (IND))); \
  crc3 = __crc32cd(                                       \
    crc3, *(cast64(P) + (SEGMENTBYTES / 8) * 3 + (IND))); \
  crc0 = __crc32cd(                                       \
    crc0, *(cast64(P) + (SEGMENTBYTES / 8) * 0 + (IND))); \
} while (0)

/* Compute 8*8 bytes for each segment parallelly. */
#define CRC32C256BYTES(P, IND) do {  \
  CRC32C32BYTES((P), (IND) * 8 + 0); \
  CRC32C32BYTES((P), (IND) * 8 + 1); \
  CRC32C32BYTES((P), (IND) * 8 + 2); \
  CRC32C32BYTES((P), (IND) * 8 + 3); \
  CRC32C32BYTES((P), (IND) * 8 + 4); \
  CRC32C32BYTES((P), (IND) * 8 + 5); \
  CRC32C32BYTES((P), (IND) * 8 + 6); \
  CRC32C32BYTES((P), (IND) * 8 + 7); \
} while (0)

/* Compute 4*8*8 bytes for each segment parallelly. */
#define CRC32C1024BYTES(P) do { \
  CRC32C256BYTES((P), 0);       \
  CRC32C256BYTES((P), 1);       \
  CRC32C256BYTES((P), 2);       \
  CRC32C256BYTES((P), 3);       \
  (P) += 4 * SEGMENTBYTES;      \
} while (0)

static uint32_t
crc32c_arm64(uint32_t z, const uint8_t *xp, size_t xn) {
  const poly64_t k0 = 0x8d96551c;
  const poly64_t k1 = 0xbd6f81f8;
  const poly64_t k2 = 0xdcb17aa4;
  uint32_t crc0, crc1, crc2, crc3;
  uint64_t t0, t1, t2;
  const uint8_t *sp;

  z ^= CRC32_XOR;

  /* Point sp at first 8-byte aligned byte in the buffer.
     This might be past the end of the buffer. */
  sp = round_up(xp, 8);

  if (sp <= xp + xn) {
    /* Process bytes xp is 8-byte aligned. */
    while (xp != sp) {
      z = __crc32cb(z, *xp);
      xp += 1;
      xn -= 1;
    }
  }

  while (xn >= KBYTES) {
    crc0 = z;
    crc1 = 0;
    crc2 = 0;
    crc3 = 0;

    /* Process 1024 bytes in parallel. */
    CRC32C1024BYTES(xp);

    /* Merge the 4 partial CRC32C values. */
    t2 = (uint64_t)vmull_p64(crc2, k2);
    t1 = (uint64_t)vmull_p64(crc1, k1);
    t0 = (uint64_t)vmull_p64(crc0, k0);
    z = __crc32cd(crc3, *cast64(xp));
    xp += sizeof(uint64_t);
    z ^= __crc32cd(0, t2);
    z ^= __crc32cd(0, t1);
    z ^= __crc32cd(0, t0);

    xn -= KBYTES;
  }

  while (xn >= 8) {
    z = __crc32cd(z, *cast64(xp));
    xp += 8;
    xn -= 8;
  }

  if (xn & 4) {
    z = __crc32cw(z, *cast32(xp));
    xp += 4;
  }

  if (xn & 2) {
    z = __crc32ch(z, *cast16(xp));
    xp += 2;
  }

  if (xn & 1)
    z = __crc32cb(z, *xp);

  return z ^ CRC32_XOR;
}

static int
has_armv8_crc32(void) {
#if defined(__linux__)
#ifdef __ANDROID__
  unsigned long hwcap = (&getauxval != NULL) ? getauxval(AT_HWCAP) : 0;
#else
  unsigned long hwcap = getauxval(AT_HWCAP);
#endif
  return (hwcap & CRC32_CAPMASK) == CRC32_CAPMASK;
#elif defined(__APPLE__)
  size_t len = sizeof(int);
  int val = 0;

  if (sysctlbyname("hw.optional.armv8_crc32", &val, &len, NULL, 0) != 0)
    return 0;

  return val != 0;
#else
  return 0;
#endif
}

static int
can_accelerate(void) {
  static const uint8_t buf[] = "TestCRCBuffer";
  return crc32c_arm64(0, buf, sizeof(buf) - 1) == 0xdcbc59fa;
}

int
ldb_crc32c_init(void) {
  static volatile int result = -1;
  static ldb_spinlock_t lock = 0;

  ldb_spinlock(&lock);

  if (result == -1) {
    if (has_armv8_crc32() && can_accelerate()) {
      crc32c_extend = &crc32c_arm64;
      result = 1;
    } else {
      result = 0;
    }
  }

  ldb_spinunlock(&lock);

  return result;
}

#else /* !HAVE_ARM64_CRC */

int
ldb_crc32c_init(void) {
  return 0;
}

#endif /* !HAVE_ARM64_CRC */

uint32_t
ldb_crc32c_extend(uint32_t z, const uint8_t *xp, size_t xn) {
  return crc32c_extend(z, xp, xn);
}
