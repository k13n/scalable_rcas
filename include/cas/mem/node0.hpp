#pragma once

#include "cas/mem/node.hpp"
#include "cas/binary_key.hpp"

namespace cas {
namespace mem {


class Node0 : public Node {
public:
  std::vector<cas::MemoryKey> suffixes_;

  Node0();

  Node0(const BinaryKey& bkey, size_t path_pos, size_t value_pos);
  /* Node0(const InterleavedKey& ikey, size_t pos); */

  // meta information
  int NodeWidth() const override {
    return 0;
  };

  // traversing
  Node* LocateChild(uint8_t key_byte) const override;
  void ForEachChild(uint8_t low, uint8_t high, const ChildIterator& callback) const override;
  void ForEachSuffix(const SuffixIterator& callback) const override;

  // updating
  void Put(uint8_t key_byte, Node* child) override;
  Node* Grow() override;
  void ReplaceBytePointer(uint8_t key_byte, Node* child) override;
};


} // namespace mem
} // namespace cas
