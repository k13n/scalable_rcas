cmake_minimum_required (VERSION 3.0)
project (app)

# enable compiler warnings
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -O3")
# set(CMAKE_CXX_CLANG_TIDY clang-tidy)

# set the C++ version
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

# # show make output
# set(CMAKE_VERBOSE_MAKEFILE ON)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
add_subdirectory(src)
add_subdirectory(tests)


## Various tools

add_executable(app ${CMAKE_CURRENT_SOURCE_DIR}/apps/app.cpp)
add_executable(count_pages ${CMAKE_CURRENT_SOURCE_DIR}/apps/count_pages.cpp)
add_executable(create_index ${CMAKE_CURRENT_SOURCE_DIR}/apps/create_index.cpp)
add_executable(csv2partition ${CMAKE_CURRENT_SOURCE_DIR}/apps/csv2partition.cpp)
add_executable(partition2csv ${CMAKE_CURRENT_SOURCE_DIR}/apps/partition2csv.cpp)
add_executable(query_index ${CMAKE_CURRENT_SOURCE_DIR}/apps/query_index.cpp)
add_executable(selectivity_computer ${CMAKE_CURRENT_SOURCE_DIR}/apps/selectivity_computer.cpp)

# for gcc on linux
target_link_libraries(app cas stdc++fs)
target_link_libraries(count_pages cas stdc++fs)
target_link_libraries(create_index cas stdc++fs)
target_link_libraries(csv2partition cas stdc++fs)
target_link_libraries(partition2csv cas stdc++fs)
target_link_libraries(query_index cas stdc++fs)
target_link_libraries(selectivity_computer cas stdc++fs)


## Various benchmarks

add_executable(exp_cost_model ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/exp_cost_model.cpp)
add_executable(exp_dataset_size ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/exp_dataset_size.cpp)
add_executable(exp_dsc_computation ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/exp_dsc_computation.cpp)
add_executable(exp_insertion ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/exp_insertion.cpp)
add_executable(exp_mem_insertion ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/exp_mem_insertion.cpp)
add_executable(exp_memory_keys ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/exp_memory_keys.cpp)
add_executable(exp_memory_management ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/exp_memory_management.cpp)
add_executable(exp_partitioning_threshold ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/exp_partitioning_threshold.cpp)
add_executable(exp_querying ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/exp_querying.cpp)
add_executable(exp_structure ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/exp_structure.cpp)

target_link_libraries(exp_cost_model cas stdc++fs)
target_link_libraries(exp_dataset_size cas stdc++fs)
target_link_libraries(exp_dsc_computation cas stdc++fs)
target_link_libraries(exp_insertion cas stdc++fs)
target_link_libraries(exp_mem_insertion cas stdc++fs)
target_link_libraries(exp_memory_keys cas stdc++fs)
target_link_libraries(exp_memory_management cas stdc++fs)
target_link_libraries(exp_partitioning_threshold cas stdc++fs)
target_link_libraries(exp_querying cas stdc++fs)
target_link_libraries(exp_structure cas stdc++fs)
