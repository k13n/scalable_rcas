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
  size_t NrSuffixes() const override {
    return 0;
  }

  // traversing
  Node* LocateChild(uint8_t key_byte) const override;
  void ForEachChild(const ChildCallback& callback) const override;

  // updating
  void Put(uint8_t key_byte, Node* child) override;
  Node* Grow() override;
  void ReplaceBytePointer(uint8_t key_byte, Node* child) override;
};

} // namespace mem
} // namespace cas
