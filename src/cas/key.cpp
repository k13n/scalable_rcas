#include "cas/key.hpp"

#include <iostream>


template<class VType>
cas::Key<VType>::Key(
  const std::string& path, const VType& value, const ref_t& ref)
    : path_(path)
    , value_(value)
    , ref_(ref)
{ }


template<class VType>
size_t cas::Key<VType>::Size() const {
  return path_.size() + sizeof(VType) + sizeof(ref_t);
}


template<class VType>
void cas::Key<VType>::Dump() const {
  std::cout << "Path:  " << path_  << "\n";
  std::cout << "Value: " << value_ << "\n";
  std::cout << "Ref:   " << ToString(ref_) << "\n";
}


template<class VType>
void cas::Key<VType>::DumpConcise() const {
  std::cout << "("
    << path_ << ", "
    << value_ << ", "
    << ToString(ref_) << ")\n";
}


// explicit instantiations to separate header from implementation
template struct cas::Key<cas::vint32_t>;
template struct cas::Key<cas::vint64_t>;
template struct cas::Key<cas::vstring_t>;
