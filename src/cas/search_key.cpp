#include "cas/search_key.hpp"
#include "cas/util.hpp"

#include <iostream>


template<class VType>
cas::SearchKey<VType>::SearchKey(std::string path, VType low, VType high)
  : path_(std::move(path))
  , low_(low)
  , high_(high)
{}


template<class VType>
void cas::SearchKey<VType>::Dump() const {
  std::cout << "SearchKey\n";
  std::cout << "Low:  " << low_ << "\n";
  std::cout << "High: " << high_ << "\n";
  std::cout << "Path: " << path_ << "\n";
}


template<class VType>
void cas::SearchKey<VType>::DumpConcise() const {
  std::cout << "Q(";
  std::cout << path_;
  std::cout << ", " << low_;
  std::cout << ", " << high_;
  std::cout << ")";
  std::cout << "\n";
}


void cas::BinarySK::Dump() const {
  std::cout << "SearchKey(Binary)" << "\n";
  std::cout << "Low:  ";
  cas::util::DumpHexValues(low_);
  std::cout << "\n";
  std::cout << "High: ";
  cas::util::DumpHexValues(high_);
  std::cout << "\n";
  std::cout << "Path: ";
  cas::util::DumpHexValues(path_);
  std::cout << "\n";
}


// explicit instantiations to separate header from implementation
template struct cas::SearchKey<cas::vint32_t>;
template struct cas::SearchKey<cas::vint64_t>;
template struct cas::SearchKey<cas::vstring_t>;
