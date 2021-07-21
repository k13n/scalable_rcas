#pragma once

#include <cstdint>
#include <string>

namespace cas {

enum class Dimension : uint8_t {
  PATH,
  VALUE,
  LEAF,
};

std::string ToString(Dimension dim);
cas::Dimension AlternateDimension(Dimension dim);

} // namespace cas
