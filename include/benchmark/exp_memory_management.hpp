#pragma once

#include "cas/bulk_loader_stats.hpp"
#include "cas/context.hpp"
#include <vector>

namespace benchmark {


template<class VType>
class ExpMemoryManagement {
  cas::Context context_;
  const std::vector<cas::MemoryPlacement>& approaches_;
  const std::vector<size_t>& memory_sizes_;
  std::vector<cas::BulkLoaderStats> results_;

public:
  ExpMemoryManagement(
      const cas::Context& context,
      const std::vector<cas::MemoryPlacement>& approaches,
      const std::vector<size_t>& memory_size
  );

  void Execute();

private:
  void ExecuteMethod(cas::MemoryPlacement method, size_t memory_size);
  void PrintOutput();
};

}; // namespace benchmark
