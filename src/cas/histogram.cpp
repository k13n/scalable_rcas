#include "cas/histogram.hpp"
#include <iostream>
#include <cmath>


size_t cas::Histogram::MaxValue() const {
  return std::prev(data_.end())->first;
}


double cas::Histogram::Average() const {
  size_t total_count = 0;
  size_t total_val = 0;
  for (const auto& [val,count] : data_) {
    total_count += count;
    total_val += val*count;
  }
  return total_val / static_cast<double>(total_count);
}


size_t cas::Histogram::Count() const {
  size_t sum = 0;
  for (const auto& [_,count] : data_) {
    sum += count;
  }
  return sum;
}


void cas::Histogram::PrintStats() const {
  std::cout << "count_: " << Count() << "\n";
  std::cout << "average_: " << Average() << "\n";
}


void cas::Histogram::Dump() const {
  size_t sum = 0;
  for (const auto& [_,count] : data_) {
    sum += count;
  }
  size_t cumulative_count = 0;
  std::cout << "value;count;percent;cumulative_count;cumulative_percent\n";
  for (const auto& [value, count] : data_) {
    cumulative_count += count;
    double percent = count / static_cast<double>(sum) * 100;
    double cumulative_percent = cumulative_count / static_cast<double>(sum) * 100;
    std::cout <<
      value << ";" <<
      count << ";" <<
      percent << ";" <<
      cumulative_count << ";" <<
      cumulative_percent << "\n";
  }
}


void cas::Histogram::Print(int nr_bins) const {
  auto upper_bound = std::prev(data_.end())->first;
  Print_(nr_bins, upper_bound);
}


void cas::Histogram::Print(int nr_bins, size_t upper_bound) const {
  Print_(nr_bins, upper_bound);
}


void cas::Histogram::Print_(int nr_bins, size_t upper_bound) const {
  if (nr_bins == 0) {
    return;
  }
  auto bin_width = static_cast<int>(std::ceil(upper_bound / static_cast<double>(nr_bins)));

  size_t sum = 0;
  for (const auto& [_,count] : data_) {
    sum += count;
  }

  auto it = data_.begin();
  const auto end = data_.end();

  size_t count = 0;
  size_t count_cumulative = 0;

  // header
  std::cout << "range low high value_percent count count_percent cumulative_count cumulative_percent\n";

  for (int i = 0; i < nr_bins; ++i) {
    count = 0;
    size_t low  = i * bin_width;
    size_t high = (i+1) * bin_width;
    if (i == nr_bins-1) {
      // make sure that the last interval includes upper_bound
      ++high;
    }
    while (it->first < high && it != end) {
      count += it->second;
      count_cumulative += it->second;
      ++it;
    }
    int value_percent = (high / static_cast<double>(upper_bound)) * 100;
    double count_percent = count / static_cast<double>(sum) * 100;
    double percent_cumulative = count_cumulative / static_cast<double>(sum) * 100;
    std::cout << "[" << low << "," << high << ") " <<
        low << " " <<
        high << " " <<
        value_percent << " " <<
        count << " " <<
        count_percent << " " <<
        count_cumulative << " " <<
        percent_cumulative << "\n";
  }
}
