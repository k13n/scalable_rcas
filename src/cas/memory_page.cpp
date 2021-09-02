#include "cas/memory_page.hpp"
#include <cstring>
#include <iostream>


void cas::MemoryPage::Push(const cas::BinaryKey& key) {
  size_t key_size = key.ByteSize();
  if (tail_pos_ + key_size > Size()) {
    throw std::runtime_error{"key does not fit"};
  }
  std::memcpy(data_ + tail_pos_, key.Begin(), key_size);
  ++NrKeys();
  tail_pos_ += key_size;
}


void cas::MemoryPage::Dump() {
  std::cout << "nr_keys_: " << NrKeys() << "\n";
  for (auto& key : *this) {
    key.Dump();
  }
}
