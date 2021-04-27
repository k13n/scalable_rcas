#include "cas/record.hpp"
#include "cas/util.hpp"
#include <cstring>
#include <iostream>



template<size_t PAGE_SZ>
void cas::PageParser<PAGE_SZ>::CopyFromPage(
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
cas::Record cas::PageParser<PAGE_SZ>::PageToRecord(
    const cas::IdxPage<PAGE_SZ>& page,
    uint16_t offset, cas::Pager<PAGE_SZ>& pager) {
  cas::Record record;

  uint16_t plen = 0;
  uint16_t vlen = 0;
  uint16_t clen = 0;
  uint32_t rlen = 0;

  // copy common header
  record.dimension_ = static_cast<cas::Dimension>(page.at(offset));
  ++offset;
  CopyFromPage(page, offset, &plen, sizeof(plen));
  CopyFromPage(page, offset, &vlen, sizeof(vlen));

  // copy node-type specific header
  if (record.dimension_ == cas::Dimension::LEAF) {
    CopyFromPage(page, offset, &rlen, sizeof(rlen));
    record.has_overflow_page_ = page.at(offset) == std::byte{1};
    ++offset;
  } else {
    CopyFromPage(page, offset, &clen, sizeof(clen));
  }

  // copy prefixes
  record.path_.resize(plen);
  record.value_.resize(vlen);
  CopyFromPage(page, offset, &record.path_[0], plen);
  CopyFromPage(page, offset, &record.value_[0], vlen);

  if (record.dimension_ == cas::Dimension::LEAF) {
    if (record.has_overflow_page_) {
      size_t nr_bytes = sizeof(cas::page_nr_t);
      CopyFromPage(page, offset, &record.overflow_page_nr_, nr_bytes);
      TraverseOverflowPages(record, pager);
    } else {
      record.references_.reserve(rlen);
      for (uint32_t i = 0; i < rlen; ++i) {
        MemoryKey leaf_key;
        uint16_t lk_plen;
        uint16_t lk_vlen;
        CopyFromPage(page, offset, &lk_plen, sizeof(lk_plen));
        CopyFromPage(page, offset, &lk_vlen, sizeof(lk_vlen));
        leaf_key.path_.resize(lk_plen);
        leaf_key.value_.resize(lk_vlen);
        CopyFromPage(page, offset, &leaf_key.path_[0], lk_plen);
        CopyFromPage(page, offset, &leaf_key.value_[0], lk_vlen);
        CopyFromPage(page, offset, leaf_key.ref_.data(), sizeof(cas::ref_t));
        record.references_.push_back(leaf_key);
      }
    }
    return record;
  }

  for (uint16_t pos = 0; pos < clen; ++pos) {
    auto type = static_cast<uint8_t>(page.at(offset));
    ++offset;
    if (type == 0) {
      uint8_t vlow = 0;
      uint8_t vhigh = 0;
      cas::page_nr_t page_nr = 0;
      CopyFromPage(page, offset, &vlow, sizeof(uint8_t));
      CopyFromPage(page, offset, &vhigh, sizeof(uint8_t));
      CopyFromPage(page, offset, &page_nr, sizeof(cas::page_nr_t));
      for (int i = vlow; i <= vhigh; ++i) {
        record.child_pages_[i] = page_nr;
      }
    } else {
      uint8_t byte = 0;
      CopyFromPage(page, offset, &byte, sizeof(uint8_t));
      CopyFromPage(page, offset, &record.child_pointers_[byte], sizeof(uint16_t));
    }
  }

  return record;
}


template<size_t PAGE_SZ>
void cas::PageParser<PAGE_SZ>::TraverseOverflowPages(
    cas::Record& record, cas::Pager<PAGE_SZ>& pager) {
  cas::page_nr_t cur_page_nr = record.overflow_page_nr_;
  bool has_another_page = true;
  while (has_another_page) {
    uint16_t offset = 0;
    auto page = pager.Fetch(cur_page_nr);
    // check if there are continuation pages
    has_another_page = page->at(0) == std::byte{1};
    ++offset;
    // get continuation page number
    CopyFromPage(*page, offset, &cur_page_nr, sizeof(cas::page_nr_t));
    // get the number of keys in this page
    uint16_t nr_keys;
    CopyFromPage(*page, offset, &nr_keys, sizeof(nr_keys));
    // copy references to the record
    for (uint16_t i = 0; i < nr_keys; ++i) {
      MemoryKey leaf_key;
      uint16_t lk_plen;
      uint16_t lk_vlen;
      CopyFromPage(*page, offset, &lk_plen, sizeof(lk_plen));
      CopyFromPage(*page, offset, &lk_vlen, sizeof(lk_vlen));
      leaf_key.path_.resize(lk_plen);
      leaf_key.value_.resize(lk_vlen);
      CopyFromPage(*page, offset, &leaf_key.path_[0], lk_plen);
      CopyFromPage(*page, offset, &leaf_key.value_[0], lk_vlen);
      CopyFromPage(*page, offset, leaf_key.ref_.data(), sizeof(cas::ref_t));
      record.references_.push_back(leaf_key);
    }
  }
}


void cas::Record::Dump() const {
  std::cout << "(Record)";
  std::cout << "\ndimension_: ";
  switch (dimension_) {
  case cas::Dimension::PATH:
    std::cout << "PATH";
    break;
  case cas::Dimension::VALUE:
    std::cout << "VALUE";
    break;
  case cas::Dimension::LEAF:
    std::cout << "LEAF";
    break;
  }
  std::cout << "\npath_: ";
  cas::util::DumpHexValues(path_);
  std::cout << "\nvalue_: ";
  cas::util::DumpHexValues(value_);
  std::cout << "\ncontained keys: ";
  for (const auto& leaf_key : references_) {
    std::cout << "\n  path_: ";
    cas::util::DumpHexValues(leaf_key.path_);
    std::cout << "\n  value_: ";
    cas::util::DumpHexValues(leaf_key.value_);
    std::cout << "\n  ref_: ";
    std::cout << ToString(leaf_key.ref_) << "\n";
  }
  std::cout << "\n";
}



template class cas::PageParser<cas::PAGE_SZ_64KB>;
template class cas::PageParser<cas::PAGE_SZ_32KB>;
template class cas::PageParser<cas::PAGE_SZ_16KB>;
template class cas::PageParser<cas::PAGE_SZ_8KB>;
template class cas::PageParser<cas::PAGE_SZ_4KB>;
