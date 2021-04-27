#pragma once

#include "cas/query_stats.hpp"
#include "cas/search_key.hpp"
#include "cas/context.hpp"
#include "cas/page_buffer.hpp"
#include "cas/pager.hpp"
#include "cas/query.hpp"
#include <vector>

namespace benchmark {


template<class VType, size_t PAGE_SZ>
class ExpQuerying {
  const std::string& index_file_;
  const std::vector<cas::SearchKey<VType>>& queries_;
  int nr_repetitions_;
  cas::Pager<PAGE_SZ> pager_;
  cas::PageBuffer<PAGE_SZ> page_buffer_;

  std::vector<cas::BinarySK> encoded_queries_;
  std::vector<cas::QueryStats> results_;

public:
  ExpQuerying(
      const std::string& index_file,
      const std::vector<cas::SearchKey<VType>>& queries,
      int nr_repetitions = 1
  );

  void Execute();

private:
  void PrintOutput();
};

}; // namespace benchmark
