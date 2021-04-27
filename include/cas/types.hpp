#pragma once

#include "cas/swh_pid.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace cas {

// info about memory pages used in bulk-loding
const size_t PAGE_SZ_64KB  = 1l << 16;
const size_t PAGE_SZ_32KB  = 1l << 15;
const size_t PAGE_SZ_16KB  = 1l << 14;
const size_t PAGE_SZ_8KB   = 1l << 13;
const size_t PAGE_SZ_4KB   = 1l << 12;

// info about index pages
using page_nr_t = size_t;
const page_nr_t ROOT_IDX_PAGE_NR = 0;

// reference type
using ref_t = std::array<std::byte, 20>;
std::string ToString(const ref_t& ref);

// value types
using vint32_t = int32_t;
using vint64_t = int64_t;
using vstring_t = std::string;

// limits
const vint32_t  VINT32_MIN  = INT32_MIN;
const vint32_t  VINT32_MAX  = INT32_MAX;
const vint64_t  VINT64_MIN  = INT64_MIN;
const vint64_t  VINT64_MAX  = INT64_MAX;
const vstring_t VSTRING_MIN = "";
const vstring_t VSTRING_MAX({ static_cast<char>(0xFF), static_cast<char>(0x00) });

const size_t BYTE_MAX = 256;

// size of a pointer
const size_t PTR_SIZE = 8;

// byte buffer used by the query
const size_t QUERY_BUFFER_SIZE = 10000;
using QueryBuffer = std::array<std::byte, QUERY_BUFFER_SIZE>;


// node clustering algorithm
// - RS: right sibling [Kanne et al. IBM Systems Journal '06]
// - FDW: flat tree, dynamic programming for tree width [Kanne et al. VLDB'06]
enum class ClusteringAlgorithm {
  RS,
  FDW
};

enum class MemoryPlacement {
  FrontLoading,
  Uniform,
  BackLoading,
  AllOrNothing,
};

enum class DscComputation {
  ByteByByte,
  TwoPass,
  Proactive,
};


std::string ToString(ClusteringAlgorithm v);
std::string ToString(MemoryPlacement v);
std::string ToString(DscComputation v);

//page buffer
const int query_buffer = 10000;

} // namespace cas
