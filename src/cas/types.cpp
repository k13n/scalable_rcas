#include "cas/types.hpp"
#include <stdexcept>


std::string cas::ToString(MemoryPlacement v){
  switch (v) {
    case MemoryPlacement::FrontLoading:
      return "front-loading";
    case MemoryPlacement::Uniform:
      return "uniform";
    case MemoryPlacement::BackLoading:
      return "back-loading";
    case MemoryPlacement::AllOrNothing:
      return "all-or-nothing";
    default:
      throw std::runtime_error{"unknown MemoryPlacement"};
  }
  return "";
}


std::string cas::ToString(DscComputation v) {
  switch (v) {
    case DscComputation::ByteByByte:
      return "byte-by-byte";
    case DscComputation::TwoPass:
      return "two-pass";
    case DscComputation::Proactive:
      return "proactive";
    default:
      throw std::runtime_error{"unknown DscComputation"};
  }
  return "";
}


std::string cas::ToString(const uint64_t& ref) {
  return std::to_string(ref);
}
