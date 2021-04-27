project (dcas-test)

# reference helper classes
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(castest
  ${CMAKE_CURRENT_SOURCE_DIR}/test.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/cas/path_matcher_test.cpp
)
target_link_libraries(castest cas)
