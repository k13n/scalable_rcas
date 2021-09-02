#pragma once

#include "cas/bulk_loader_stats.hpp"
#include "cas/context.hpp"
#include <vector>

namespace benchmark {


template<class VType>
class ExpInsertion {
  cas::Context context_;
  const std::vector<double>& bulkload_fractions_;
  std::vector<std::pair<double, cas::BulkLoaderStats>> results_;

public:
  ExpInsertion(
      const cas::Context& context,
      const std::vector<double>& bulkload_fractions
  );

  void Execute();

private:
  void Execute(double bulkload_fraction);
  void PrintOutput();
};

}; // namespace benchmark

