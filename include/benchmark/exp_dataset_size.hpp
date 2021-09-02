#pragma once

#include "cas/bulk_loader_stats.hpp"
#include "cas/context.hpp"
#include <vector>

namespace benchmark {


template<class VType>
class ExpDatasetSize {
  cas::Context context_;
  const std::vector<size_t>& dataset_sizes_;
  std::vector<cas::BulkLoaderStats> results_;

public:
  ExpDatasetSize(
      const cas::Context& context,
      const std::vector<size_t>& dataset_sizes
  );

  void Execute();

private:
  void Execute(size_t dataset_size);
  void PrintOutput();
};

}; // namespace benchmark
