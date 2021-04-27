#pragma once

#include "cas/binary_key.hpp"
#include "cas/dimension.hpp"
#include "cas/pager.hpp"
#include "cas/types.hpp"
#include <memory>
#include <vector>


namespace cas {

struct Record {
  cas::page_nr_t page_nr_ = 0;
  cas::Dimension dimension_ = cas::Dimension::PATH;
  std::vector<std::byte> path_;
  std::vector<std::byte> value_;
  std::array<cas::page_nr_t, cas::BYTE_MAX> child_pages_{};
  std::array<uint16_t, cas::BYTE_MAX> child_pointers_{};
  std::vector<cas::MemoryKey> references_;
  bool has_overflow_page_ = false;
  page_nr_t overflow_page_nr_ = 0;

  void Dump() const;
};


template<size_t PAGE_SZ>
class PageParser {
public:
  static Record PageToRecord(
      const cas::IdxPage<PAGE_SZ>& page,
      uint16_t offset, Pager<PAGE_SZ>& pager);

  static void TraverseOverflowPages(Record& record,
      Pager<PAGE_SZ>& pager);

private:
  static void CopyFromPage(
      const cas::IdxPage<PAGE_SZ>& page,
      uint16_t& offset, void* dst, size_t count);
};



} // namespace cas
