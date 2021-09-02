#include "cas/memory_pool.hpp"
#include "cas/types.hpp"
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>


cas::MemoryPools cas::MemoryPools::Construct(
    size_t max_memory,
    size_t memory_capacity)
{
  if (0 < memory_capacity && memory_capacity < max_memory) {
    throw std::bad_alloc();
  }
  size_t available_pages = max_memory / cas::PAGE_SZ;
  if (available_pages < 1 + cas::BYTE_MAX) {
    throw std::bad_alloc();
  }
  // determine input pool size
  size_t input_sz = 1;
  available_pages -= 1;
  // determine output pool size
  size_t output_sz = cas::BYTE_MAX;
  available_pages -= cas::BYTE_MAX;
  // the remaining pages are work pool pages
  size_t work_sz = available_pages;
  // see how many pages are left that can be occupied
  size_t cache_killer_sz = 0;
  size_t total_pages = memory_capacity / cas::PAGE_SZ;
  if (total_pages > (input_sz + output_sz + work_sz)) {
    cache_killer_sz = total_pages - (input_sz + output_sz + work_sz);
  }
  return MemoryPools(input_sz, output_sz, work_sz, cache_killer_sz);
}


cas::MemoryPool::MemoryPool(size_t max_pages, MemoryPageType type)
  : max_pages_(max_pages)
  , type_(type)
{
  if (max_pages * cas::PAGE_SZ <= 0) {
    address_ = nullptr;
    return;
  }
  address_ = static_cast<std::byte*>(mmap(NULL, max_pages * cas::PAGE_SZ,
      PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS,
      -1, 0));
  if (address_ == MAP_FAILED) {
    std::cerr << "could not mmap memory" << std::endl;
    std::terminate();
  }
  pages_.reserve(max_pages_);
  for (size_t i = 0; i < max_pages_; ++i) {
    pages_.emplace_back(address_ + i * cas::PAGE_SZ);
    pages_.back().Type(type);
  }
}

cas::MemoryPool::~MemoryPool() {
  if (address_ == nullptr) {
    return;
  }
  int rt = munmap(address_, max_pages_ * cas::PAGE_SZ);
  if (rt < 0) {
    std::cerr << "could not munmap allocated memory" << std::endl;
    std::terminate();
  }
}


cas::MemoryPage cas::MemoryPool::Get() {
  if (pages_.empty()) {
    throw std::bad_alloc();
  }
  auto page = std::move(pages_.back());
  page.Reset();
  page.Type(type_);
  pages_.pop_back();
  return page;
}


void cas::MemoryPool::Release(MemoryPage&& page) {
  if (pages_.size() >= max_pages_) {
    throw std::runtime_error{"pool capacity reached (" + std::to_string(max_pages_) + ")"};
  }
  pages_.push_back(std::move(page));
}


void cas::MemoryPools::Dump() {
  std::cout << "MemoryPool (";
  std::cout << "input: " <<
    input_.NrUsedPages() << "/" << input_.Capacity() << "; ";
  std::cout << "output: " <<
    output_.NrUsedPages() << "/" << output_.Capacity() << "; ";
  std::cout << "work: " <<
    work_.NrUsedPages() << "/" << work_.Capacity() << "; ";
  std::cout << "cache_killer: " <<
    cache_killer_.NrUsedPages() << "/" << cache_killer_.Capacity() << ")\n";
}


void cas::MemoryPool::FillWithZeros() {
  for (auto& page : pages_) {
    std::memset(page.Data(), 0, cas::PAGE_SZ);
  }
}
