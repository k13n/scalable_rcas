#pragma once

#include <array>
#include <string>


namespace cas {

// reference type
using ref_t = std::array<std::byte, 20>;

std::string ToString(const ref_t& ref);
ref_t ParseSwhPid(const std::string& input);

} // namespace cas
