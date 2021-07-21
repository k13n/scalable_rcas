#pragma once

#include "cas/mem/node.hpp"


namespace cas {
namespace mem {

class Node4 : public Node {
public:
  uint8_t keys_[4];
  Node* children_[4];

  Node4(cas::Dimension dimension);

  // meta information
  int NodeWidth() const override {
    return 4;
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
