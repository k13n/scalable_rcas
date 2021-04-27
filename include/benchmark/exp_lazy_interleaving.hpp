#pragma once

#include "cas/bulk_loader_stats.hpp"
#include "cas/context.hpp"
#include <vector>

namespace benchmark {


template<class VType, size_t PAGE_SZ>
class ExpLazyInterleaving {
  cas::Context context_;
  const std::vector<bool>& interleaving_settings_;
  const std::vector<size_t>& dataset_sizes_;
  std::vector<std::pair<cas::Context, cas::BulkLoaderStats>> results_;

public:
  ExpLazyInterleaving(
      const cas::Context& context,
      const std::vector<bool>& interleaving_settings,
      const std::vector<size_t>& dataset_sizes
  );

  void Execute();

private:
  void Execute(bool interleaving_setting, size_t dataset_size);
  void PrintOutput();
};

}; // namespace benchmark
