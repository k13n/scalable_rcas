#include "cas/swh_pid.hpp"
#include <iomanip>
#include <sstream>

std::string cas::ToString(const cas::SwhPid& ref) {
  std::stringstream result;
  for (size_t i = 0; i < ref.size(); ++i) {
    result << std::setfill('0') << std::setw(2)
      << std::hex << static_cast<int>(ref[i]);
  }
  return result.str();
}


cas::SwhPid cas::ParseSwhPid(const std::string& input) {
  if (input.size() != 40) {
    throw std::runtime_error{"invalid SWH PID"};
  }
  SwhPid ref;
  // set hash value
  auto char2int = [](char val) -> int {
    if(val >= '0' && val <= '9') {
      return val - '0';
    } if(val >= 'A' && val <= 'F') {
      return val - 'A' + 10;
    } if(val >= 'a' && val <= 'f') {
      return val - 'a' + 10;
    } else {
      throw std::invalid_argument("Invalid input string");
    }
  };
  for (size_t i = 0, j = 0; i < input.size(); i+=2, ++j) {
    int byte = char2int(input[i])*16 + char2int(input[i+1]);
    ref[j] = static_cast<std::byte>(byte);
  }
  return ref;
}
