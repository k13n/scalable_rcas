#pragma once

#include "cas/bulk_loader_stats.hpp"
#include "cas/context.hpp"
#include <vector>

namespace benchmark {


template<class VType>
class ExpPartitioningThreshold {
  cas::Context context_;
  const std::vector<size_t>& thresholds_;
  const std::vector<size_t>& dataset_sizes_;
  std::vector<std::pair<cas::Context, cas::BulkLoaderStats>> results_;

public:
  ExpPartitioningThreshold(
      const cas::Context& context,
      const std::vector<size_t>& tresholds,
      const std::vector<size_t>& dataset_sizes
  );

  void Execute();

private:
  void Execute(size_t threshold, size_t dataset_size);
  void PrintOutput();
};

}; // namespace benchmark
