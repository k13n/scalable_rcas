#pragma once

#include "cas/mem/node.hpp"
#include "cas/mem/node0.hpp"
#include "cas/binary_key.hpp"

namespace cas {
namespace mem {


class Insertion {
  cas::mem::Node** root_ = nullptr;
  cas::BinaryKey bkey_;
  const size_t partitioning_threshold_;

public:

  Insertion(cas::mem::Node** root_,
      cas::BinaryKey bkey_,
      size_t partitioning_threshold);

  void Execute();


private:
  void LazilyRestructureLeafNode(
      Node0* node,
      size_t gP, size_t gV,
      size_t iP, size_t iV);

  Node* LazilyRestructureNode(
      Node* node,
      Node* parent_node,
      size_t gP, size_t gV,
      size_t iP, size_t iV);

};


} // namespace mem
} // namespace cas
