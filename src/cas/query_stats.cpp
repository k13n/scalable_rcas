#include "cas/query_stats.hpp"
#include <iostream>


void cas::QueryStats::Dump() const {
  size_t read_nodes_ = read_path_nodes_ + read_value_nodes_ + read_leaf_nodes_;
  std::cout << "QueryStats";
  std::cout << "\nMatches: " << nr_matches_;
  std::cout << "\nRead Nodes: " << read_nodes_;
  std::cout << "\nRead Path Nodes: " << read_path_nodes_;
  std::cout << "\nRead Value Nodes: " << read_value_nodes_;
  std::cout << "\nRead Leaf Nodes: " << read_leaf_nodes_;
  std::cout << "\nRuntime (mus): " << runtime_mus_;
  std::cout << "\nValue Matching Runtime (mus): " << value_matching_mus_;
  std::cout << "\nPath Matching Runtime (mus):  " << path_matching_mus_;
  std::cout << "\nRef Matching Runtime (mus):   " << ref_matching_mus_;
  if(nr_matches_==0){
    std::cout << "\nAverage depth of the matches: " << "None";
  }
  else{
    std::cout << "\nAverage depth of the matches: " << sum_depth_/nr_matches_;
  }
  std::cout << "\npage_reads_: " << page_reads_;
  std::cout << "\npage_reads_from_disk_: " << page_reads_from_disk_;
  std::cout << "\npage_reads_from_buffer_: " << page_reads_from_buffer_;
  std::cout << "\n";
}


cas::QueryStats cas::QueryStats::Avg(const std::vector<cas::QueryStats>& stats) {
  cas::QueryStats result;
  if (!stats.empty()) {
    for (const auto& stat : stats) {
      result.nr_matches_ += stat.nr_matches_;
      result.read_path_nodes_ += stat.read_path_nodes_;
      result.read_value_nodes_ += stat.read_value_nodes_;
      result.read_leaf_nodes_ += stat.read_leaf_nodes_;
      result.runtime_mus_ += stat.runtime_mus_;
      result.value_matching_mus_ += stat.value_matching_mus_;
      result.path_matching_mus_ += stat.path_matching_mus_;
      result.ref_matching_mus_ += stat.ref_matching_mus_;
      result.page_reads_ += stat.page_reads_;
      result.page_reads_from_disk_ += stat.page_reads_from_disk_;
      result.page_reads_from_buffer_ += stat.page_reads_from_buffer_;
    }
    result.nr_matches_  = static_cast<size_t>(result.nr_matches_  / stats.size());
    result.read_path_nodes_ = static_cast<size_t>(result.read_path_nodes_  / stats.size());
    result.read_value_nodes_ = static_cast<size_t>(result.read_value_nodes_  / stats.size());
    result.read_leaf_nodes_ = static_cast<size_t>(result.read_leaf_nodes_  / stats.size());
    result.runtime_mus_ = static_cast<size_t>(result.runtime_mus_ / stats.size());
    result.value_matching_mus_ = static_cast<size_t>(result.value_matching_mus_ / stats.size());
    result.path_matching_mus_ = static_cast<size_t>(result.path_matching_mus_ / stats.size());
    result.ref_matching_mus_ = static_cast<size_t>(result.ref_matching_mus_ / stats.size());
    result.page_reads_ = static_cast<size_t>(result.page_reads_ / stats.size());
    result.page_reads_from_disk_ = static_cast<size_t>(result.page_reads_from_disk_ / stats.size());
    result.page_reads_from_buffer_ = static_cast<size_t>(result.page_reads_from_buffer_ / stats.size());
  }
  return result;
}
