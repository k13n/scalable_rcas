#pragma once

#include "cas/bulk_loader_stats.hpp"
#include "cas/context.hpp"
#include <vector>
#include <string>

namespace benchmark {


template<class VType>
class ExpMemInsertion {
  const std::string dataset_path_;
  const size_t nr_keys_;

public:
  ExpMemInsertion(
      const std::string& dataset_path,
      const size_t nr_keys);

  void Execute();
};

}; // namespace benchmark

