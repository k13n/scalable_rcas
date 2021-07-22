#include "cas/index_stats.hpp"
#include <cstring>
#include <iostream>
#include <vector>


template<size_t PAGE_SZ>
cas::IndexStats<PAGE_SZ>::IndexStats(cas::Pager<PAGE_SZ>& pager)
  : pager_(pager)
{}


template<size_t PAGE_SZ>
void cas::IndexStats<PAGE_SZ>::Compute() {
}



template<size_t PAGE_SZ>
void cas::IndexStats<PAGE_SZ>::Dump() {
  internal_depth_histo_.Dump();
  std::cout << "\n\n";
  external_depth_histo_.Dump();
}



template class cas::IndexStats<cas::PAGE_SZ_64KB>;
template class cas::IndexStats<cas::PAGE_SZ_32KB>;
template class cas::IndexStats<cas::PAGE_SZ_16KB>;
template class cas::IndexStats<cas::PAGE_SZ_8KB>;
template class cas::IndexStats<cas::PAGE_SZ_4KB>;
