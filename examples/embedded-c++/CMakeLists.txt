cmake_minimum_required(VERSION 2.8.12...3.29)
project(example-c++)

set(CMAKE_CXX_STANDARD 17)

include_directories(../../src/include)
link_directories(../../build/release/src)

add_executable(example main.cpp)
target_link_libraries(example duckdb)

add_executable(upsert upsert.cpp)
target_link_libraries(upsert duckdb)
