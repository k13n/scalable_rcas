#include "cas/page_buffer.hpp"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <deque>
#include <stdexcept>
#include <unordered_map>


template<size_t PAGE_SZ>
static void CopyFromPage(const std::shared_ptr<cas::IdxPage<PAGE_SZ>>& page,
    uint16_t& offset, void* dst, size_t count) {
  if (offset + count > page->size()) {
    auto msg = "page address violation: " + std::to_string(offset+count);
    throw std::out_of_range{msg};
  }
  std::memcpy(dst, page->begin()+offset, count);
  offset += count;
}


template<size_t PAGE_SZ>
void cas::PageBuffer<PAGE_SZ>::Fill(cas::Pager<PAGE_SZ>& pager) {
  buffer_.clear();
  buffer_.reserve(size_);
  buffer_[cas::ROOT_IDX_PAGE_NR] = pager.Fetch(cas::ROOT_IDX_PAGE_NR);

  int max_depth = 0;
  size_t nr_of_saved_pages = 1;

  // queue for nodes: page, offset, depth
  std::deque<std::tuple<std::shared_ptr<IdxPage<PAGE_SZ>>, uint16_t, int>> nodes;
  nodes.push_back({buffer_[cas::ROOT_IDX_PAGE_NR], 0, 0});

  while (nr_of_saved_pages < size_ && !nodes.empty()){
    auto [page, offset, depth] = nodes.front();
    nodes.pop_front();
    if (offset == 0) {
      // we are at the beginning of a new page, put all
      // sibling subtrees into the queue
      uint8_t partition_size = 0;
      CopyFromPage(page, offset, &partition_size, sizeof(uint8_t));
      const int payload_beginning = 1+(partition_size*3);
      while (offset < payload_beginning) {
        uint8_t byte = 0;
        uint16_t address = 0;
        CopyFromPage(page, offset, &byte, sizeof(byte));
        CopyFromPage(page, offset, &address, sizeof(address));
        // we add the subtrees to the front of the queue.
        // in other words, we are just expanding this inter-page
        // pointer and traverse the roots of these subtrees next
        nodes.push_front({page, address, depth});
      }
    } else {
      // we are at a node contained inside (not at the beginning)
      // of a page
      uint16_t plen = 0;
      uint16_t vlen = 0;
      uint16_t clen = 0;

      // copy common header
      auto dimension = static_cast<cas::Dimension>(page->at(offset));
      ++offset;
      CopyFromPage(page, offset, &plen, sizeof(plen));
      CopyFromPage(page, offset, &vlen, sizeof(vlen));

      // copy node-type specific header
      if (dimension == cas::Dimension::LEAF) {
        continue;
      } else {
        CopyFromPage(page, offset, &clen, sizeof(clen));
      }

      // skip over prefixes
      offset += plen;
      offset += vlen;

      for (uint16_t pos = 0; pos < clen && nr_of_saved_pages < size_; ++pos) {
        auto type = static_cast<uint8_t>(page->at(offset));
        ++offset;

        if (max_depth < depth + 1) {
          max_depth = depth + 1;
        }

        if (type == 0) {
          // inter-page pointer
          offset += sizeof(uint8_t)*2; // skip over the [vlow, vhigh]
          cas::page_nr_t page_nr = 0;
          CopyFromPage(page, offset, &page_nr, sizeof(cas::page_nr_t));
          if (buffer_.find(page_nr) == buffer_.end()) {
            buffer_[page_nr] = pager.Fetch(page_nr);
            ++nr_of_saved_pages;
            nodes.push_back({ buffer_[page_nr] , 0, depth+1 });
          } else {
            //this should never happen since each page has a unique interpagepointer to it
            throw std::runtime_error{"page already in buffer"};
          }
        } else {
          // intra-page pointer
          uint8_t byte = 0;
          uint16_t address = 0;
          CopyFromPage(page, offset, &byte, sizeof(uint8_t));
          CopyFromPage(page, offset, &address, sizeof(uint16_t));
          nodes.push_back({page, address, depth+1});
        }
      }
    }
  }
  /* std::cout << "Depth in the page buffer: " << max_depth << std::endl; */
}


template<size_t PAGE_SZ>
std::shared_ptr<cas::IdxPage<PAGE_SZ>>
cas::PageBuffer<PAGE_SZ>::GetPage(cas::page_nr_t pagenr) const {
  auto search = buffer_.find(pagenr);
  if (search == buffer_.end()){
    return nullptr;
  } else{
    return search->second;
  }
}


template<size_t PAGE_SZ>
void cas::PageBuffer<PAGE_SZ>::Dump(){
    std::cout<<"Buffer contains " << buffer_.size() << " element(s)" << std::endl;
}



// explicit instantiations to separate header from implementation
template class cas::PageBuffer<cas::PAGE_SZ_64KB>;
template class cas::PageBuffer<cas::PAGE_SZ_32KB>;
template class cas::PageBuffer<cas::PAGE_SZ_16KB>;
template class cas::PageBuffer<cas::PAGE_SZ_8KB>;
template class cas::PageBuffer<cas::PAGE_SZ_4KB>;
