#pragma once

#include "cas/memory_page.hpp"
#include "cas/types.hpp"
#include "cas/dimension.hpp"
#include <memory>
#include <vector>

namespace cas {

template<size_t PAGE_SZ>
class MemoryPool {
  const size_t max_pages_;
  const MemoryPageType type_;
  std::vector<MemoryPage<PAGE_SZ>> pages_;
  std::byte* address_;

public:
  MemoryPool(size_t max_pages, MemoryPageType type);
  ~MemoryPool();

  /* delete copy/move constructor/assignment */
  MemoryPool(const MemoryPool& other) = delete;
  MemoryPool(MemoryPool&& other) = delete;
  MemoryPool& operator=(const MemoryPool& other) = delete;
  MemoryPool& operator=(MemoryPool&& other) = delete;

  MemoryPage<PAGE_SZ> Get();
  void Release(MemoryPage<PAGE_SZ>&& page);

  size_t NrFreePages() { return pages_.size(); }
  size_t NrUsedPages() { return max_pages_ - pages_.size(); }
  bool HasFreePage() { return pages_.size() > 0; }
  size_t Capacity() { return max_pages_; }
  bool Full() { return pages_.size() == max_pages_; }

  void FillWithZeros();
};


template<size_t PAGE_SZ>
class MemoryPools {
private:
  MemoryPools(
      size_t input_sz,
      size_t output_sz,
      size_t work_sz,
      size_t cache_killer_sz
  )
    : input_(input_sz, MemoryPageType::INPUT)
    , output_(output_sz, MemoryPageType::OUTPUT)
    , work_(work_sz, MemoryPageType::WORK)
    , cache_killer_(cache_killer_sz, MemoryPageType::CACHE_KILLER)
  {
    // make sure that the cache_killer_ pages are actually
    // allocated by the OS
    cache_killer_.FillWithZeros();
  }

  /* delete copy/move constructor/assignment */
  MemoryPools(const MemoryPools& other) = delete;
  MemoryPools(MemoryPools&& other) = delete;
  MemoryPools& operator=(const MemoryPools& other) = delete;
  MemoryPools& operator=(MemoryPools&& other) = delete;

public:
  MemoryPool<PAGE_SZ> input_;
  MemoryPool<PAGE_SZ> output_;
  MemoryPool<PAGE_SZ> work_;
  MemoryPool<PAGE_SZ> cache_killer_;

  static MemoryPools<PAGE_SZ> Construct(
      size_t max_memory,
      size_t memory_capacity = 0);

  void Dump();
};

} // namespace cas
