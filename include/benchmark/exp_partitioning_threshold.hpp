#pragma once

#include "cas/bulk_loader_stats.hpp"
#include "cas/query_stats.hpp"
#include "cas/context.hpp"
#include "cas/search_key.hpp"
#include <vector>

namespace benchmark {


template<class VType>
class ExpPartitioningThreshold {
  struct Stats {
    cas::BulkLoaderStats bulk_stats_;
    cas::QueryStats query_stats_;
  };

  cas::Context context_;
  const std::vector<size_t>& thresholds_;
  const std::vector<size_t>& dataset_sizes_;
  const std::vector<cas::SearchKey<VType>>& queries_;
  std::vector<std::pair<cas::Context, Stats>> results_;

public:
  ExpPartitioningThreshold(
      const cas::Context& context,
      const std::vector<size_t>& tresholds,
      const std::vector<size_t>& dataset_sizes,
      const std::vector<cas::SearchKey<VType>>& queries
  );

  void Execute();

private:
  void Execute(size_t threshold, size_t dataset_size);
  void PrintOutput();
};

}; // namespace benchmark
