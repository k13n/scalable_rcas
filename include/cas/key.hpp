#pragma once

#include "cas/types.hpp"
#include <cstddef>
#include <cstdint>
#include <memory>

namespace cas {

template<class VType>
struct Key {
  /* Path */
  std::string path_;
  /* Value */
  VType value_;
  /* Reference  */
  ref_t ref_;

  Key() = default;
  Key(const std::string& path, const VType& value, const ref_t& ref);

  size_t Size() const;

  void Dump() const;
  void DumpConcise() const;
};


template<class VType>
bool operator==(const Key<VType>& lhs, const Key<VType>& rhs) {
  return lhs.value_ == rhs.value_ &&
    lhs.path_ == rhs.path_ &&
    lhs.ref_ == rhs.ref_;
}


} // namespace cas
