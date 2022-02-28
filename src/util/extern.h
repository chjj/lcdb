/*!
 * extern.h - extern definitions for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_EXTERN_H
#define RDB_EXTERN_H

#ifdef RDB_EXPORT
#  if defined(__EMSCRIPTEN__)
#    include <emscripten.h>
#    define RDB_EXTERN EMSCRIPTEN_KEEPALIVE
#  elif defined(__wasm__)
#    define RDB_EXTERN __attribute__((visibility("default")))
#  elif defined(_WIN32)
#    define RDB_EXTERN __declspec(dllexport)
#  elif defined(__GNUC__) && __GNUC__ >= 4
#    define RDB_EXTERN __attribute__((visibility("default")))
#  endif
#endif

#ifndef RDB_EXTERN
#  define RDB_EXTERN
#endif

#endif /* RDB_EXTERN_H */
