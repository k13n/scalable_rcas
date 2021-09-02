#pragma once

#include "cas/swh_pid.hpp"
#include <string>

namespace cas {


#define REF_T_SWHPID
using ref_t = cas::SwhPid;

/* #define REF_T_UINT64 */
/* using ref_t = uint64_t; */


// interface for the reference type
std::string ToString(const ref_t& ref);


// useful if the reference type ref_t is uint64_t
std::string ToString(const uint64_t& ref);


} // namespace cas
