#include "cas/mem/insertion.hpp"
#include "cas/mem/node0.hpp"
#include "cas/mem/node4.hpp"
#include "cas/mem/node16.hpp"
#include "cas/mem/node48.hpp"
#include "cas/mem/node256.hpp"
#include <iostream>


cas::mem::Insertion::Insertion(
        cas::mem::Node** root,
        cas::BinaryKey bkey,
        size_t partitioning_threshold)
  : root_{root}
  , bkey_{bkey}
  , partitioning_threshold_{partitioning_threshold}
{}


void cas::mem::Insertion::Execute() {
  size_t path_pos = 0;
  size_t value_pos = 0;

  if (*root_ == nullptr) {
    *root_ = new cas::mem::Node0(bkey_, path_pos, value_pos);
    return;
  }

  cas::mem::Node* grandparent = nullptr;
  cas::mem::Node* parent = nullptr;
  cas::mem::Node* node = *root_;
  uint8_t parent_byte = 0x00;
  uint8_t next_byte = 0x00;

  // positions in the key's strings
  size_t gP = 0, gV = 0;
  // positions in the current node's prefixes
  size_t iP = 0, iV = 0;

  while (node != nullptr) {
    iP = 0;
    iV = 0;
    // match prefixes
    while (iP < node->LenPath() && gP < bkey_.LenPath() &&
           node->Path()[iP] == static_cast<uint8_t>(bkey_.Path()[gP])) {
      ++iP;
      ++gP;
    }
    while (iV < node->LenValue() && gV < bkey_.LenValue() &&
           node->Value()[iV] == static_cast<uint8_t>(bkey_.Value()[gV])) {
      ++iV;
      ++gV;
    }
    // CASE 1: check if full key is matched
    if (gP >= bkey_.LenPath() && gV >= bkey_.LenValue()) {
      break;
    }
    // CASE 3: check for mismatch
    if (iP < node->LenPath() || iV < node->LenValue()) {
      break;
    }
    // descend node and keep track of partial way (grandparent, parent)
    grandparent = parent;
    parent = node;
    parent_byte = next_byte;
    switch (node->Dimension()) {
      case cas::Dimension::PATH:
        next_byte = static_cast<uint8_t>(bkey_.Path()[gP]);
        node = node->LocateChild(next_byte);
        ++gP;
        break;
      case cas::Dimension::VALUE:
        next_byte = static_cast<uint8_t>(bkey_.Value()[gV]);
        node = node->LocateChild(next_byte);
        ++gV;
        break;
      default:
        throw std::runtime_error{"impossible"};
    }
  }

  // CASE 2: we need to add another leaf node
  // we must check this case first because gP and gV have been
  // modified inside the while loop when we checked for this case
  if (node == nullptr) {
    // check if the parent needs to grow to the next node size
    if (parent->IsFull()) {
      auto* parent_replacement = parent->Grow();
      if (grandparent == nullptr) {
        // parent_replacement replaces the old root node
        *root_ = parent_replacement;
      } else {
        // parent_replacement replaces the parent in the grandparent
        grandparent->ReplaceBytePointer(parent_byte, parent_replacement);
      }
      delete parent;
      parent = parent_replacement;
    }
    auto* leaf = new cas::mem::Node0(bkey_, gP, gV);
    parent->Put(next_byte, leaf);
    return;
  }

  // CASE 1: full key is matched
  if (gP >= bkey_.LenPath() && gV >= bkey_.LenValue()) {
    if (node->Dimension() != cas::Dimension::LEAF) {
      throw std::runtime_error{"this must be a leaf node"};
    }
    // we add a new suffix to the suffixes_ (we do not check partitioning_threshold_,
    // because we couldn't even split this node since there's no mismatch)
    auto leaf = static_cast<cas::mem::Node0*>(node);
    leaf->refs_.push_back(bkey_.Ref());
    return;
  }

  // CASE 3: we have a mismatch
  if (iP < node->LenPath() || iV < node->LenValue()) {
    auto* intermediate_node = LazilyRestructureNode(node, parent, gP, gV, iP, iV);
    if (parent == nullptr) {
      *root_ = intermediate_node;
    } else {
      parent->ReplaceBytePointer(next_byte, intermediate_node);
    }
    return;
  }

  throw std::runtime_error{"impossible (one of cases 1..3 must have occurred)"};
}


cas::mem::Node* cas::mem::Insertion::LazilyRestructureNode(
    cas::mem::Node* node, cas::mem::Node* parent_node,
    size_t gP, size_t gV, size_t iP, size_t iV)
{
  // first we determine the dimension in which the new node
  // partitions the data
  cas::Dimension dimension;
  bool path_mismatch  = iP < node->LenPath();
  bool value_mismatch = iV < node->LenValue();
  if (path_mismatch && value_mismatch) {
    dimension = parent_node == nullptr
      ? cas::Dimension::VALUE
      : cas::AlternateDimension(parent_node->Dimension());
  } else if (path_mismatch && !value_mismatch) {
    dimension = cas::Dimension::PATH;
  } else if (!path_mismatch && value_mismatch) {
    dimension = cas::Dimension::VALUE;
  } else {
    throw std::runtime_error{"impossible that there's no mismatch"};
  }

  // create new intermediate parent node and set its prefixes
  // to the longest common path and value prefixes
  auto* intermediate_parent = new cas::mem::Node4(dimension);
  for (size_t i = 0; i < iP; ++i) {
    intermediate_parent->prefixes_.push_back(node->Path()[i]);
  }
  for (size_t i = 0; i < iV; ++i) {
    intermediate_parent->prefixes_.push_back(node->Value()[i]);
  }
  intermediate_parent->separator_pos_ = iP;

  // create new sibling/leaf node for new key
  uint8_t leaf_byte = 0x00;
  switch (dimension) {
    case cas::Dimension::PATH:
      leaf_byte = static_cast<uint8_t>(bkey_.Path()[gP]);
      ++gP;
      break;
    case cas::Dimension::VALUE:
      leaf_byte = static_cast<uint8_t>(bkey_.Value()[gV]);
      ++gV;
      break;
    default:
      break;
  }
  auto* leaf = new cas::mem::Node0(bkey_, gP, gV);

  // update node's prefixes, remove common bytes
  uint8_t node_byte = 0x00;
  switch (dimension) {
    case cas::Dimension::PATH:
      node_byte = node->Path()[iP];
      ++iP;
      break;
    case cas::Dimension::VALUE:
      node_byte = node->Value()[iV];
      ++iV;
      break;
    default:
      break;
  }
  std::vector<uint8_t> buffer;
  for (size_t i = iP; i < node->LenPath(); ++i) {
    buffer.push_back(node->Path()[i]);
  }
  for (size_t i = iV; i < node->LenValue(); ++i) {
    buffer.push_back(node->Value()[i]);
  }
  node->separator_pos_ = node->LenPath() - iP;
  node->prefixes_ = buffer;

  // re-wire nodes
  intermediate_parent->Put(leaf_byte, leaf);
  intermediate_parent->Put(node_byte, node);

  return intermediate_parent;
}
