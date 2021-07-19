#pragma once

#include "cas/dimension.hpp"
#include "cas/types.hpp"
#include "cas/util.hpp"
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <functional>
#include <iostream>

namespace cas {


class NodeReader {
  static constexpr int POS_D = 0;
  static constexpr int POS_LEN_P = 1;
  static constexpr int POS_LEN_V = 2;
  static constexpr int POS_M = 3;
  static constexpr int POS_P = 5;

  const uint8_t* buffer_;

public:

  using ChildCallback = std::function<void(uint8_t byte, size_t pos)>;
  using SuffixCallback = std::function<void(
      uint8_t len_p, const uint8_t* path,
      uint8_t len_v, const uint8_t* value,
      cas::ref_t ref)>;


  NodeReader(const uint8_t* buffer) : buffer_{buffer} {};

  inline cas::Dimension Dimension() const {
    return static_cast<cas::Dimension>(buffer_[POS_D]);
  }

  inline uint8_t LenPath() const {
    return buffer_[POS_LEN_P];
  }

  inline uint8_t LenValue() const {
    return buffer_[POS_LEN_V];
  }

  inline uint16_t NrEntries() const {
    uint16_t result = 0;
    result |= static_cast<uint16_t>(buffer_[POS_M]   << 8);
    result |= static_cast<uint16_t>(buffer_[POS_M+1] << 0);
    return result;
  }

  inline uint16_t NrChildren() const {
    return Dimension() == cas::Dimension::LEAF
      ? 0
      : NrEntries();
  }

  inline uint16_t NrSuffixes() const {
    return Dimension() == cas::Dimension::LEAF
      ? NrEntries()
      : 0;
  }

  inline const uint8_t* Path() const {
    return &buffer_[POS_P];
  }

  inline const uint8_t* Value() const {
    return &buffer_[POS_P + LenPath()];
  }

  inline bool IsLeaf() const {
    return Dimension() == cas::Dimension::LEAF;
  }

  void ForEachChild(const ChildCallback& callback) const {
    size_t offset = POS_P + LenPath() + LenValue();
    for (uint16_t i = 0, sz = NrChildren(); i < sz; ++i) {
      uint8_t b = buffer_[offset++];
      size_t ptr = 0;
      ptr |= (static_cast<size_t>(buffer_[offset++]) << 40);
      ptr |= (static_cast<size_t>(buffer_[offset++]) << 32);
      ptr |= (static_cast<size_t>(buffer_[offset++]) << 24);
      ptr |= (static_cast<size_t>(buffer_[offset++]) << 16);
      ptr |= (static_cast<size_t>(buffer_[offset++]) <<  8);
      ptr |= (static_cast<size_t>(buffer_[offset++]) <<  0);
      callback(b, ptr);
    }
  }

  void ForEachSuffix(const SuffixCallback& callback) const {
    size_t offset = POS_P + LenPath() + LenValue();
    for (uint16_t i = 0, sz = NrSuffixes(); i < sz; ++i) {
      uint8_t len_p = buffer_[offset++];
      uint8_t len_v = buffer_[offset++];
      const uint8_t* path  = &buffer_[offset];
      const uint8_t* value = &buffer_[offset + len_p];
      cas::ref_t ref;
      std::memcpy(&ref[0], &buffer_[offset + len_p + len_v], sizeof(cas::ref_t));
      callback(len_p, path, len_v, value, ref);
    }
  }

  void Dump() {
    std::cout << "Dimension: " << cas::ToString(Dimension()) << "\n";
    std::cout << "LenP: " << static_cast<int>(LenPath()) << "\n";
    std::cout << "LenV: " << static_cast<int>(LenValue()) << "\n";
    std::cout << "Path: ";
    cas::util::DumpHexValues(Path(), 0, LenPath());
    std::cout << "\n";
    std::cout << "Value: ";
    cas::util::DumpHexValues(Value(), 0, LenValue());
    std::cout << "\n";
    if (IsLeaf()) {
      std::cout << "NrSuffixes: " << NrSuffixes() << "\n";
      int i = 0;
      ForEachSuffix([&i](
            uint8_t len_p, const uint8_t* path,
            uint8_t len_v, const uint8_t* value,
            cas::ref_t ref) -> void {
        printf("  [%3d] Path (%3d): ", ++i, len_p);
        cas::util::DumpHexValues(path, 0, len_p);
        printf("\n        Value (%3d): ", len_v);
        cas::util::DumpHexValues(value, 0, len_v);
        printf("\n        Revision: %s\n", cas::ToString(ref).c_str());
      });
    } else {
      std::cout << "NrChildren: " << NrChildren() << "\n";
      int i = 0;
      ForEachChild([&i](uint8_t byte, size_t pos) -> void {
        printf("  [%3d] 0x%02X --> %ld\n", ++i, byte, pos);
      });
    }
  }

private:
  void CopyFromBuffer(size_t& offset, void* dst, size_t count) {
    std::memcpy(dst, buffer_ + offset, count);
    offset += count;
  };

};


} // namespace cas