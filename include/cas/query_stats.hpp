#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>


namespace cas {

struct QueryStats {
  size_t nr_matches_ = 0;
  size_t read_nodes_ = 0;
  size_t read_path_nodes_ = 0;
  size_t read_value_nodes_ = 0;
  size_t read_leaf_nodes_ = 0;
  size_t runtime_mus_ = 0;
  size_t value_matching_mus_ = 0;
  size_t path_matching_mus_ = 0;
  size_t ref_matching_mus_ = 0;
  size_t sum_depth_ = 0;
  size_t page_reads_ = 0;
  size_t page_reads_from_disk_ = 0;
  size_t page_reads_from_buffer_ = 0;

  void Dump() const;

  static QueryStats Avg(const std::vector<QueryStats>& stats);
};

} // namespace cas
