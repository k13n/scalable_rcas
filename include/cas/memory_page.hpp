#pragma once

#include "cas/binary_key.hpp"
#include <array>
#include <iostream>
#include <memory>


namespace cas {

enum class MemoryPageType : uint8_t {
  WORK,
  INPUT,
  OUTPUT,
  CACHE_KILLER,
};


template<size_t PAGE_SZ>
class MemoryPage {
private:
  static const size_t OFFSET_NR_KEYS = 0;
  static const size_t OFFSET_DATA = OFFSET_NR_KEYS + sizeof(uint16_t);

  std::byte* data_;
  size_t tail_pos_ = OFFSET_DATA;
  MemoryPageType type_ = MemoryPageType::WORK;

public:
  MemoryPage(std::byte* data) : data_(data) {
    if (data != nullptr) {
      NrKeys() = 0;
    }
  }

  /* delete copy constructor/assignment */
  MemoryPage(const MemoryPage& other) = delete;
  MemoryPage& operator=(const MemoryPage& other) = delete;
  /* implement default move constructor/assignment */
  MemoryPage(MemoryPage&& other) = default;
  MemoryPage& operator=(MemoryPage&& other) = default;

  void Push(const BinaryKey& key);

  uint16_t& NrKeys() const {
    return *reinterpret_cast<uint16_t*>(data_ + OFFSET_NR_KEYS);
  }

  std::byte* Data() { return data_; }
  const std::byte* Data() const { return data_; }
  size_t Size() const { return PAGE_SZ; }
  size_t FreeSpace() const { return PAGE_SZ - tail_pos_; }
  MemoryPageType Type() const { return type_; }
  void Type(MemoryPageType type) { type_ = type; }

  bool IsNull() { return data_ == nullptr; }

  void Reset() {
    NrKeys() = 0;
    tail_pos_ = OFFSET_DATA;
  }

  void Dump();

  /* iterator implementation */
  class iterator;
  iterator begin() { return iterator(data_ + OFFSET_DATA, 0); }
  iterator end() { return iterator(data_ + OFFSET_DATA, NrKeys()); }

  class iterator {
    cas::BinaryKey key_;
    std::byte* data_;
    uint16_t key_nr_;

  public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = cas::BinaryKey;
    using difference_type = int;
    using pointer = cas::BinaryKey*;
    using reference = cas::BinaryKey&;

    iterator(std::byte* data, uint16_t key_nr)
      : key_(data)
      , data_(data)
      , key_nr_(key_nr)
    { }
    iterator operator++() {
      iterator i = *this;
      ++key_nr_;
      data_ += key_.ByteSize();
      key_ = BinaryKey(data_);
      return i;
    }
    pointer operator->()  { return &key_; }
    reference operator*() { return key_; }
    bool operator==(const iterator& rhs) { return key_nr_ == rhs.key_nr_; }
    bool operator!=(const iterator& rhs) { return key_nr_ != rhs.key_nr_; }
  };
};


} // namespace cas
