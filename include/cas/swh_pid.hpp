#pragma once

#include <array>
#include <string>


namespace cas {

// reference type
using SwhPid = std::array<std::byte, 20>;

std::string ToString(const SwhPid& ref);
SwhPid ParseSwhPid(const std::string& input);

} // namespace cas
