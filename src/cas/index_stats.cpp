#include "cas/index_stats.hpp"
#include <cstring>
#include <iostream>
#include <vector>


cas::IndexStats::IndexStats(cas::Pager& pager)
  : pager_(pager)
{}


void cas::IndexStats::Compute() {
}



void cas::IndexStats::Dump() {
  internal_depth_histo_.Dump();
  std::cout << "\n\n";
  external_depth_histo_.Dump();
}
