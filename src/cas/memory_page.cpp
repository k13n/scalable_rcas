#include "cas/memory_page.hpp"
#include <cstring>
#include <iostream>


template<size_t PAGE_SZ>
void cas::MemoryPage<PAGE_SZ>::Push(const cas::BinaryKey& key) {
  size_t key_size = key.ByteSize();
  if (tail_pos_ + key_size > PAGE_SZ) {
    throw std::runtime_error{"key does not fit"};
  }
  std::memcpy(data_ + tail_pos_, key.Begin(), key_size);
  ++NrKeys();
  tail_pos_ += key_size;
}


template<size_t PAGE_SZ>
void cas::MemoryPage<PAGE_SZ>::Dump() {
  std::cout << "nr_keys_: " << NrKeys() << "\n";
  for (auto& key : *this) {
    key.Dump();
  }
}


template class cas::MemoryPage<cas::PAGE_SZ_64KB>;
template class cas::MemoryPage<cas::PAGE_SZ_32KB>;
template class cas::MemoryPage<cas::PAGE_SZ_16KB>;
template class cas::MemoryPage<cas::PAGE_SZ_8KB>;
template class cas::MemoryPage<cas::PAGE_SZ_4KB>;
