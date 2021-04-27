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
  std::vector<State> stack;
  // put root node on stack
  stack.emplace_back(
      pager_.Fetch(cas::ROOT_IDX_PAGE_NR),
      cas::ROOT_IDX_PAGE_NR,
      0,
      cas::kNullByte,
      0,
      0
  );
  while (!stack.empty()) {
    State s = stack.back();
    stack.pop_back();
    auto [record,found] = FetchRecord(s);
    if (!found) {
      continue;
    }
    internal_depth_histo_.Record(s.internal_depth_);
    external_depth_histo_.Record(s.external_depth_);
    if (record.dimension_ == cas::Dimension::LEAF) {
      internal_leaf_depth_histo_.Record(s.internal_depth_);
      external_leaf_depth_histo_.Record(s.external_depth_);
    } else {
      cas::page_nr_t prev_page_nr = 0;
      for (int i = 0; i <= 0xFF; ++i) {
        if (record.child_pages_[i] != 0 && record.child_pages_[i] != prev_page_nr) {
          stack.emplace_back(
              pager_.Fetch(record.child_pages_[i]),
              record.child_pages_[i],
              0,
              static_cast<std::byte>(i),
              s.internal_depth_ + 1,
              s.external_depth_ + 1);
          prev_page_nr = record.child_pages_[i];
        } else if (record.child_pointers_[i] != 0) {
          stack.emplace_back(
              s.page_,
              s.page_nr_,
              record.child_pointers_[i],
              static_cast<std::byte>(i),
              s.internal_depth_ + 1,
              s.external_depth_);
        }
      }
    }
  }
}



template<size_t PAGE_SZ>
std::pair<cas::Record,bool> cas::IndexStats<PAGE_SZ>::FetchRecord(const State& s) {
  const auto& page = s.page_;
  uint16_t offset = s.page_offset_;
  if (offset > 0) {
    return {cas::PageParser<PAGE_SZ>::PageToRecord(*page, offset, pager_), true};
  }

  // if the partition_size is 0 it means it's actually 256, since we
  // cannot represent 256 in uint8_t
  uint8_t partition_size = 0;
  CopyFromPage(*page, offset, &partition_size, sizeof(uint8_t));
  int real_partition_size = partition_size;
  if (partition_size == 0) {
    real_partition_size = 256;
  }

  const int payload_beginning = 1+(real_partition_size*3);
  while (offset < payload_beginning) {
    uint8_t byte = 0;
    uint16_t address = 0;
    CopyFromPage(*page, offset, &byte, sizeof(byte));
    CopyFromPage(*page, offset, &address, sizeof(address));
    if (byte == static_cast<uint8_t>(s.parent_byte_)) {
      offset = address;
      return {cas::PageParser<PAGE_SZ>::PageToRecord(*page, offset, pager_), true};
    }
  }

  Record empty;
  return {empty, false};
}


template<size_t PAGE_SZ>
void cas::IndexStats<PAGE_SZ>::CopyFromPage(
    const cas::IdxPage<PAGE_SZ>& page,
    uint16_t& offset, void* dst, size_t count) {
  if (offset + count > page.size()) {
    auto msg = "page address violation: " + std::to_string(offset+count);
    throw std::out_of_range{msg};
  }
  std::memcpy(dst, page.begin()+offset, count);
  offset += count;
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
