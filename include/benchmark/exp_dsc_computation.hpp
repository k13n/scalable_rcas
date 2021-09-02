#pragma once

#include "cas/bulk_loader_stats.hpp"
#include "cas/context.hpp"
#include <vector>

namespace benchmark {


template<class VType>
class ExpDscComputation {
  cas::Context context_;
  const std::vector<cas::DscComputation>& approaches_;
  const std::vector<size_t>& memory_sizes_;
  std::vector<cas::BulkLoaderStats> results_;

public:
  ExpDscComputation(
      const cas::Context& context,
      const std::vector<cas::DscComputation>& approaches,
      const std::vector<size_t>& memory_sizes
  );

  void Execute();

private:
  void ExecuteMethod(cas::DscComputation method, size_t memory_size);
  void PrintOutput();
};

}; // namespace benchmark
