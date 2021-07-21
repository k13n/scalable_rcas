#pragma once

#include "cas/mem/node.hpp"


namespace cas {
namespace mem {

const int kEmptyIndex = 0xFF;

class Node48 : public Node {
public:
  uint8_t indexes_[256];
  Node* children_[48];

  Node48(cas::Dimension dimension);

  // meta information
  int NodeWidth() const override {
    return 48;
  };

  // traversing
  Node* LocateChild(uint8_t key_byte) const override;
  void ForEachChild(uint8_t low, uint8_t high, const ChildIterator& callback) const override;
  void ForEachSuffix(const SuffixIterator& callback) const override;

  // updating
  void Put(uint8_t key_byte, Node* child) override;
  Node* Grow() override;
  void ReplaceBytePointer(uint8_t key_byte, Node* child) override;

private:
  void DumpIndexes();
};

} // namespace mem
} // namespace cas
