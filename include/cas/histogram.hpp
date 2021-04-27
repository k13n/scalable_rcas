#pragma once

#include <map>

namespace cas {

class Histogram {
  std::map<size_t, size_t> data_;

public:
  inline void Record(size_t utilization) {
    ++data_[utilization];
  }

  void Print(int nr_bins) const;
  void Print(int nr_bins, size_t upper_bound) const;
  void PrintStats() const;
  void Dump() const;

  double Average() const;
  size_t MaxValue() const;
  size_t Count() const;

private:
  void Print_(int nr_bins, size_t upper_bound) const;
};


} // namespace cas
