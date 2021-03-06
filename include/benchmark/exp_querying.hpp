#pragma once

#include "cas/query_stats.hpp"
#include "cas/search_key.hpp"
#include "cas/context.hpp"
#include "cas/pager.hpp"
#include "cas/query.hpp"
#include <vector>

namespace benchmark {


template<class VType>
class ExpQuerying {
  const std::string& pipeline_dir_;
  const std::vector<cas::SearchKey<VType>>& queries_;
  const bool clear_page_cache_;
  const bool do_warmup_;
  int nr_repetitions_;

  std::vector<cas::BinarySK> encoded_queries_;
  std::vector<cas::QueryStats> results_;

public:
  ExpQuerying(
      const std::string& pipeline_dir,
      const std::vector<cas::SearchKey<VType>>& queries,
      bool clear_page_cache = false,
      bool do_warmup = false,
      int nr_repetitions = 1
  );

  void Execute();

  std::vector<cas::QueryStats> Results() {
    return results_;
  }


private:
  void DoWarmUp();
  void PrintOutput();
};

}; // namespace benchmark
