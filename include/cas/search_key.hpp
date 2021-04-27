#pragma once

#include "cas/key.hpp"
#include <vector>
#include <string>

namespace cas {

template<class VType>
struct SearchKey {
  std::string path_;
  VType low_;
  VType high_;

  SearchKey();
  SearchKey(std::string path, VType low, VType high);

  void Dump() const;
  void DumpConcise() const;
};


struct BinarySK {
  std::vector<std::byte> path_;
  std::vector<std::byte> low_;
  std::vector<std::byte> high_;

  void Dump() const;
};


} // namespace cas
