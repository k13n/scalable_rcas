#include "cas/query_stats.hpp"
#include <iostream>


void cas::QueryStats::Dump() const {
  std::cout << "QueryStats";
  std::cout << "\nMatches: " << nr_matches_;
  std::cout << "\nRead Nodes: " << read_nodes_;
  std::cout << "\nRead Path Nodes: " << read_path_nodes_;
  std::cout << "\nRead Value Nodes: " << read_value_nodes_;
  std::cout << "\nRead Leaf Nodes: " << read_leaf_nodes_;
  std::cout << "\nRuntime (mus): " << runtime_mus_;
  size_t depth = (nr_matches_ == 0) ? 0 : sum_depth_/nr_matches_;
  std::cout << "\nAverage depth of the matches: " << depth;
  std::cout << "\n";
}


cas::QueryStats cas::QueryStats::Sum(const std::vector<cas::QueryStats>& stats) {
  cas::QueryStats result;
  if (stats.empty()) {
    return result;
  }
  for (const auto& stat : stats) {
    result.nr_matches_ += stat.nr_matches_;
    result.read_nodes_ += stat.read_nodes_;
    result.read_path_nodes_ += stat.read_path_nodes_;
    result.read_value_nodes_ += stat.read_value_nodes_;
    result.read_leaf_nodes_ += stat.read_leaf_nodes_;
    result.runtime_mus_ += stat.runtime_mus_;
    result.sum_depth_ += stat.sum_depth_;
  }
  return result;
}


cas::QueryStats cas::QueryStats::Avg(const std::vector<cas::QueryStats>& stats) {
  cas::QueryStats result;
  if (stats.empty()) {
    return result;
  }
  result = Sum(stats);
  result.nr_matches_  /= stats.size();
  result.read_nodes_ /= stats.size();
  result.read_path_nodes_ /= stats.size();
  result.read_value_nodes_ /= stats.size();
  result.read_leaf_nodes_ /= stats.size();
  result.runtime_mus_ /= stats.size();
  result.sum_depth_ /= stats.size();
  return result;
}
