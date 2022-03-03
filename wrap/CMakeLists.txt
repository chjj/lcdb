# CMakeLists.txt - cmake build for leveldb wrapper
# Copyright (c) 2022, Christopher Jeffrey (MIT License).
# https://github.com/chjj/rdb

cmake_minimum_required(VERSION 3.4)
project(rdb LANGUAGES C)

add_library(rdb STATIC wrap.c)

target_link_libraries(rdb PRIVATE leveldb)
