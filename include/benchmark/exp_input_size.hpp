#pragma once

#include "cas/bulk_loader_stats.hpp"
#include "cas/context.hpp"
#include <vector>

namespace benchmark {


template<class VType, size_t PAGE_SZ>
class ExpInputSize {
  cas::Context context_;
  const std::vector<size_t>& memory_sizes_;
  std::vector<cas::BulkLoaderStats> results_;

public:
  ExpInputSize(
      const cas::Context& context,
      const std::vector<size_t>& input_
  );

  void Execute();

private:
  void ExecuteMethod(cas::MemoryPlacement method, size_t memory_size);
  void PrintOutput();
};

}; // namespace benchmark
