#pragma once

#include "cas/pager.hpp"
#include "cas/histogram.hpp"
#include "cas/key_encoding.hpp"
#include <map>
#include <set>

namespace cas {

class IndexStats {
  Pager& pager_;

  struct State {
    std::shared_ptr<IdxPage> page_;
    page_nr_t page_nr_;
    uint16_t page_offset_ = 0;
    std::byte parent_byte_ = cas::kNullByte;
    int internal_depth_ = 0;
    int external_depth_ = 0;

    State(std::shared_ptr<IdxPage> page,
        cas::page_nr_t page_nr,
        int page_offset,
        std::byte parent_byte,
        int internal_depth,
        int external_depth
    )
      : page_(page)
      , page_nr_(page_nr)
      , page_offset_(page_offset)
      , parent_byte_(parent_byte)
      , internal_depth_(internal_depth)
      , external_depth_(external_depth) {}
  };

public:
  cas::Histogram internal_depth_histo_;
  cas::Histogram external_depth_histo_;
  cas::Histogram internal_leaf_depth_histo_;
  cas::Histogram external_leaf_depth_histo_;

  IndexStats(Pager& pager);
  void Compute();
  void Dump();
};

} // namespace cas
