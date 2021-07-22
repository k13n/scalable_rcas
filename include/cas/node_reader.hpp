#pragma once

#include "cas/dimension.hpp"
#include "cas/inode.hpp"
#include "cas/types.hpp"
#include "cas/util.hpp"
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <functional>
#include <iostream>

namespace cas {


class NodeReader : public INode {
  static constexpr int POS_D = 0;
  static constexpr int POS_LEN_PV = 1;
  static constexpr int POS_M = 3;
  static constexpr int POS_P = 5;

  const uint8_t* head_;
  const uint8_t* buffer_;

public:

  NodeReader(const uint8_t* head, size_t pos)
    : head_{head}
    , buffer_{head + pos}
  {}

  inline cas::Dimension Dimension() const override {
    return static_cast<cas::Dimension>(buffer_[POS_D]);
  }

  inline size_t LenPath() const override {
    uint16_t data = 0;
    data |= static_cast<uint16_t>(buffer_[POS_LEN_PV]   << 8);
    data |= static_cast<uint16_t>(buffer_[POS_LEN_PV+1] << 0);
    auto [plen, vlen] = cas::util::DecodeSizes(data);
    return plen;
  }

  inline size_t LenValue() const override {
    uint16_t data = 0;
    data |= static_cast<uint16_t>(buffer_[POS_LEN_PV]   << 8);
    data |= static_cast<uint16_t>(buffer_[POS_LEN_PV+1] << 0);
    auto [plen, vlen] = cas::util::DecodeSizes(data);
    return vlen;
  }

  inline size_t NrEntries() const {
    uint16_t result = 0;
    result |= static_cast<uint16_t>(buffer_[POS_M]   << 8);
    result |= static_cast<uint16_t>(buffer_[POS_M+1] << 0);
    return result;
  }

  inline size_t NrChildren() const override {
    return Dimension() == cas::Dimension::LEAF
      ? 0
      : NrEntries();
  }

  inline size_t NrSuffixes() const override {
    return Dimension() == cas::Dimension::LEAF
      ? NrEntries()
      : 0;
  }

  inline const uint8_t* Path() const override {
    return &buffer_[POS_P];
  }

  inline const uint8_t* Value() const override {
    return &buffer_[POS_P + LenPath()];
  }

  void ForEachChild(const INode::ChildCallback& callback) const override {
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
      NodeReader node{head_, ptr};
      callback(b, &node);
    }
  }

  void ForEachSuffix(const INode::SuffixCallback& callback) const override {
    size_t offset = POS_P + LenPath() + LenValue();
    for (uint16_t i = 0, sz = NrSuffixes(); i < sz; ++i) {
      uint16_t len_data = 0;
      len_data |= static_cast<uint16_t>(buffer_[offset++] << 8);
      len_data |= static_cast<uint16_t>(buffer_[offset++] << 0);
      auto [len_p, len_v] = cas::util::DecodeSizes(len_data);
      const uint8_t* path  = &buffer_[offset];
      offset += len_p;
      const uint8_t* value = &buffer_[offset];
      offset += len_v;
      cas::ref_t ref;
      std::memcpy(&ref[0], &buffer_[offset], sizeof(cas::ref_t));
      offset += sizeof(cas::ref_t);
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
      ForEachChild([&i](uint8_t byte, INode* child) -> void {
        auto c = static_cast<NodeReader*>(child);
        printf("  [%3d] 0x%02X --> %ld\n", ++i, byte, c->buffer_ - c->head_);
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
