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
  inline int NodeWidth() const override {
    return 4;
  };
  inline size_t NrSuffixes() const override {
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
