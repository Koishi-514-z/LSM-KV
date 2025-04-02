cmake_minimum_required(VERSION 3.10)
project(LSM_TREE)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(kvstore kvstore_api.h kvstore.h
        kvstore.cc skiplist.cpp skiplist.h sstable.cpp sstable.h
        bloom.cpp bloom.h MurmurHash3.h utils.h 
        sstablehead.cpp sstablehead.h)

add_executable(correctness correctness.cc test.h)

add_executable(persistence persistence.cc test.h)

add_subdirectory(third_party/llama.cpp)
add_subdirectory(embedding)
add_subdirectory(test)

target_link_libraries(kvstore PUBLIC embedding)

target_link_libraries(correctness PUBLIC kvstore)

target_link_libraries(persistence PUBLIC kvstore)
