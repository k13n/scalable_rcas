#include "cas/dimension.hpp"
#include <stdexcept>

std::string cas::ToString(cas::Dimension dim) {
  switch (dim) {
    case Dimension::PATH:  return "PATH";
    case Dimension::VALUE: return "VALUE";
    case Dimension::LEAF:  return "LEAF";
    default:
      return "";
  }
}


cas::Dimension cas::AlternateDimension(cas::Dimension dim) {
  switch (dim) {
    case Dimension::PATH:
      return Dimension::VALUE;
    case Dimension::VALUE:
      return Dimension::PATH;
    default:
      throw std::runtime_error{"LEAF does not have an alternate dimension"};
  }
}
