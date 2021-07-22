#pragma once

#include "cas/types.hpp"
#include "cas/dimension.hpp"
#include <memory>
#include <cstring>

namespace cas {


struct MemoryKey {
  std::vector<uint8_t> path_;
  std::vector<uint8_t> value_;
  cas::ref_t ref_;
};


class BinaryKey {
  static const size_t POS_LEN_PATH = 0;
  static const size_t POS_LEN_VALUE = POS_LEN_PATH + sizeof(uint16_t);
  static const size_t POS_REF = POS_LEN_VALUE + sizeof(uint16_t);
  static const size_t POS_DATA = POS_REF + sizeof(ref_t);

  std::byte* data_;

public:
  BinaryKey(std::byte* data) : data_(data) {}
  ~BinaryKey() {}

  /* // delete copy/move constructors/assignments */
  /* BinaryKey(const BinaryKey& other) = delete; */
  /* BinaryKey(const BinaryKey&& other) = delete; */
  /* BinaryKey& operator=(const BinaryKey& other) = delete; */
  /* BinaryKey& operator=(BinaryKey&& other) = delete; */

  inline void LenPath(uint16_t len) {
    Write<uint16_t>(len, POS_LEN_PATH);
  }
  inline uint16_t LenPath() const {
    return Read<uint16_t>(POS_LEN_PATH);
  }

  inline void LenValue(uint16_t len) {
    Write<uint16_t>(len, POS_LEN_VALUE);
  }
  inline uint16_t LenValue() const {
    return Read<uint16_t>(POS_LEN_VALUE);
  }

  inline void Ref(const ref_t& ref) {
    std::memcpy(data_ + POS_REF, ref.data(), ref.size());
  }
  inline const ref_t& Ref() const {
    return *reinterpret_cast<ref_t*>(data_ + POS_REF);
  }

  inline std::byte* Path() {
    return data_ + POS_DATA;
  }
  inline std::byte* Path() const {
    return data_ + POS_DATA;
  }

  inline std::byte* Value() {
    return data_ + POS_DATA + LenPath();
  }
  inline std::byte* Value() const {
    return data_ + POS_DATA + LenPath();
  }

  inline std::byte* Begin() const {
    return data_;
  }
  inline std::byte* End() const {
    return data_ + ByteSize();
  }

  inline size_t ByteSize() const {
    return POS_DATA + LenPath() + LenValue();
  }

  void Dump() const;

private:
  template<class V>
  inline void Write(V value, size_t pos) {
    *reinterpret_cast<V*>(data_ + pos) = value;
  }

  template<class V>
  inline V Read(size_t pos) const {
    return *reinterpret_cast<V*>(data_ + pos);
  }
};


} // namespace cas
