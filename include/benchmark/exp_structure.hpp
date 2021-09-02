#pragma once

#include "cas/context.hpp"
#include "cas/histogram.hpp"

namespace benchmark {


template<class VType>
class ExpStructure {
  cas::Context context_;


public:
  ExpStructure(
      const cas::Context& context
  );

  void Execute();

private:
  void PrintOutput(
      const cas::Histogram& histogram,
      int nr_bins,
      const std::string& msg);

  void PrintOutput(
      const cas::Histogram& histogram,
      int nr_bins,
      size_t upper_bound,
      const std::string& msg);
};

}; // namespace benchmark
