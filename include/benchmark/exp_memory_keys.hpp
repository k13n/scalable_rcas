#pragma once

#include "cas/bulk_loader_stats.hpp"
#include "cas/context.hpp"
#include <vector>

namespace benchmark {


template<class VType>
class ExpMemoryKeys {
  cas::Context context_;
  const std::vector<size_t>& nr_memory_keys_;
  std::vector<std::pair<size_t, cas::BulkLoaderStats>> results_;

public:
  ExpMemoryKeys(
      const cas::Context& context,
      const std::vector<size_t>& nr_memory_keys
  );

  void Execute();

private:
  void Execute(size_t nr_memory_keys);
  void PrintOutput();
};

}; // namespace benchmark

