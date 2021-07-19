#include "cas/dimension.hpp"

std::string cas::ToString(cas::Dimension dim) {
  switch (dim) {
    case Dimension::PATH:  return "PATH";
    case Dimension::VALUE: return "VALUE";
    case Dimension::LEAF:  return "LEAF";
    default:
      return "";
  }
}

