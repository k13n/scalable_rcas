#pragma once

#include "cas/pager.hpp"
#include "cas/dimension.hpp"
#include "cas/types.hpp"
#include <deque>
#include <iostream>
#include <unordered_map>

namespace cas {

template<size_t PAGE_SZ>
class PageBuffer {
  size_t size_;
  std::unordered_map<cas::page_nr_t, std::shared_ptr<IdxPage<PAGE_SZ>>> buffer_;

public:
  PageBuffer(size_t size) :
    size_{size}
  {}

  void Fill(cas::Pager<PAGE_SZ>& pager);
  std::shared_ptr<IdxPage<PAGE_SZ>> GetPage(cas::page_nr_t pagenr) const;
  void Dump();
};


}
