#pragma once

#include "cas/mem/node.hpp"


namespace cas {
namespace mem {

class Node256 : public Node {
public:
  Node* children_[256];

  Node256(cas::Dimension dimension);

  // meta information
  int NodeWidth() const override {
    return 256;
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
