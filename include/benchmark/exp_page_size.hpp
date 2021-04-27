#pragma once

#include "cas/bulk_loader_stats.hpp"
#include "cas/context.hpp"
#include <map>

namespace benchmark {


template<class VType, size_t PAGE_SZ>
class ExpPageSizeExecutor {
public:
  cas::BulkLoaderStats Run(cas::Context context);
};


template<class VType>
class ExpPageSize {
  cas::Context context_;
  std::map<size_t, cas::BulkLoaderStats> results_;

public:
  ExpPageSize(const cas::Context& context);
  void Execute();

private:
  void PrintOutput();
};

}; // namespace benchmark
