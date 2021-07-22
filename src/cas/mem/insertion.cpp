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
    // check if full key is matched
    if (gP >= bkey_.LenPath() && gV >= bkey_.LenValue()) {
      break;
    }
    // check for mismatch
    if (iP < node->LenPath() || iV < node->LenValue()) {
      break;
    }
    // if we reached a leaf node we need to add a new suffix
    if (node->Dimension() == cas::Dimension::LEAF) {
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

  // CASE 1: we need to add another leaf node
  if (node == nullptr) {
    /* std::cout << "[DEBUG] CASE 1\n"; */
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

  // CASE 2: full key is matched
  if (gP >= bkey_.LenPath() && gV >= bkey_.LenValue()) {
    /* std::cout << "[DEBUG] CASE 2\n"; */
    if (node->Dimension() != cas::Dimension::LEAF) {
      throw std::runtime_error{"this must be a leaf node"};
    }
    // we add a new suffix to the suffixes_ (we do not check partitioning_threshold_,
    // because we couldn't even split this node since there's no mismatch)
    auto leaf = static_cast<cas::mem::Node0*>(node);
    cas::MemoryKey suffix;
    suffix.ref_ = bkey_.Ref();
    leaf->suffixes_.push_back(suffix);
    return;
  }

  // CASE 3: we have a mismatch
  if (iP < node->LenPath() || iV < node->LenValue()) {
    /* std::cout << "[DEBUG] CASE 3\n"; */
    if (node->Dimension() == cas::Dimension::LEAF &&
        node->NrSuffixes() < partitioning_threshold_) {
      auto leaf = static_cast<cas::mem::Node0*>(node);
      LazilyRestructureLeafNode(leaf, gP, gV, iP, iV);
    } else {
      /* std::cout << "[DEBUG] CASE 3: LazilyRestructureNode\n"; */
      auto* intermediate_node = LazilyRestructureNode(node, parent, gP, gV, iP, iV);
      if (parent == nullptr) {
        *root_ = intermediate_node;
      } else {
        parent->ReplaceBytePointer(next_byte, intermediate_node);
      }
    }
    return;
  }

  // CASE 4: partial key is matched until leaf, we need to add another suffix
  if (node->Dimension() == cas::Dimension::LEAF) {
    /* std::cout << "[DEBUG] CASE 4\n"; */
    auto leaf = static_cast<cas::mem::Node0*>(node);

    // add another suffix to the existing leaf
    cas::MemoryKey suffix;
    for (size_t i = gP; i < bkey_.LenPath(); ++i) {
      suffix.path_.push_back(bkey_.Path()[i]);
    }
    for (size_t i = gV; i < bkey_.LenValue(); ++i) {
      suffix.value_.push_back(bkey_.Value()[i]);
    }
    suffix.ref_ = bkey_.Ref();
    leaf->suffixes_.push_back(suffix);

    // check if the leaf is too big now, in which case we must partition it
    if (leaf->NrSuffixes() > partitioning_threshold_) {
      auto* replacement_node = PartitionLeafNode(leaf);
      if (parent == nullptr) {
        *root_ = replacement_node;
      } else {
        parent->ReplaceBytePointer(next_byte, replacement_node);
      }
    }
    return;
  }
}


cas::mem::Node* cas::mem::Insertion::PartitionLeafNode(cas::mem::Node0* node) {
  // determine new dimension of leaf node
  bool p_possible = node->suffixes_[0].path_.size() > 0;
  bool v_possible = node->suffixes_[0].value_.size() > 0;
  if (!p_possible && !v_possible) {
    throw std::runtime_error{"cannot partition this leaf node"};
  }
  // we could make a better choice by looking at the parent of the node
  cas::Dimension dimension = v_possible
    ? cas::Dimension::VALUE
    : cas::Dimension::PATH;


  // re-partition the suffixes
  std::array<cas::mem::Node0*, 256> partition_table;
  for (size_t i = 0; i < partition_table.size(); ++i) {
    partition_table[i] = nullptr;
  }
  for (auto& suffix : node->suffixes_) {
    uint8_t byte;
    if (dimension == cas::Dimension::PATH) {
      byte = static_cast<uint8_t>(suffix.path_[0]);
      suffix.path_.erase(suffix.path_.begin());
    } else {
      byte = static_cast<uint8_t>(suffix.value_[0]);
      suffix.value_.erase(suffix.value_.begin());
    }
    if (partition_table[byte] == nullptr) {
      partition_table[byte] = new cas::mem::Node0();
    }
    partition_table[byte]->suffixes_.push_back(suffix);
  }

  // chop off new common prefixes
  for (size_t i = 0; i < partition_table.size(); ++i) {
    if (partition_table[i] == nullptr) {
      continue;
    }
    auto new_leaf = partition_table[i];
    // compute dsc bytes
    auto& ref = new_leaf->suffixes_[0];
    size_t uP = ref.path_.size();
    size_t uV = ref.value_.size();
    for (auto& suffix : new_leaf->suffixes_) {
      size_t gP = 0, gV = 0;
      while (gP < uP && ref.path_[gP]  == suffix.path_[gP])  { ++gP; }
      while (gV < uV && ref.value_[gV] == suffix.value_[gV]) { ++gV; }
      uP = gP;
      uV = gV;
    }
    // set new_leaf's common prefixes
    for (size_t i = 0; i < uP; ++i) {
      new_leaf->prefixes_.push_back(static_cast<uint8_t>(ref.path_[i]));
    }
    for (size_t i = 0; i < uV; ++i) {
      new_leaf->prefixes_.push_back(static_cast<uint8_t>(ref.value_[i]));
    }
    new_leaf->separator_pos_ = uP;
    // drop suffixes' prefixes
    for (auto& suffix : new_leaf->suffixes_) {
      suffix.path_.erase(suffix.path_.begin(), suffix.path_.begin() + uP);
      suffix.value_.erase(suffix.value_.begin(), suffix.value_.begin() + uV);
    }
  }


  // count the number of partitions to see what node type we need
  int nr_partitions = 0;
  for (size_t i = 0; i < partition_table.size(); ++i) {
    if (partition_table[i] != nullptr) {
      ++nr_partitions;
    }
  }

  // create new intermedate node
  cas::mem::Node* replacement_node = nullptr;
  if (nr_partitions <= 4) {
    replacement_node = new cas::mem::Node4(dimension);
  } else if (nr_partitions <= 16) {
    replacement_node = new cas::mem::Node16(dimension);
  } else if (nr_partitions <= 48) {
    replacement_node = new cas::mem::Node48(dimension);
  } else {
    replacement_node = new cas::mem::Node256(dimension);
  }
  replacement_node->separator_pos_ = node->separator_pos_;
  replacement_node->prefixes_ = node->prefixes_;

  // add all the new nodes as children of the new replacement_node
  for (size_t i = 0; i < partition_table.size(); ++i) {
    if (partition_table[i] != nullptr) {
      replacement_node->Put(static_cast<uint8_t>(i), partition_table[i]);
    }
  }

  // replacement_node replaced node
  delete node;

  return replacement_node;
}


void cas::mem::Insertion::LazilyRestructureLeafNode(
    cas::mem::Node0* node, size_t gP, size_t gV, size_t iP, size_t iV)
{
  // a part of the node's prefixes becomes discriminative. we must push
  // everything after the discriminative part to the suffixes
  std::vector<std::byte> buffer1;
  for (auto& suffix : node->suffixes_) {
    // push discriminative path part to path suffix
    if (iP < node->LenPath()) {
      buffer1.clear();
      for (size_t i = iP; i < node->LenPath(); ++i) {
        buffer1.push_back(static_cast<std::byte>(node->Path()[i]));
      }
      for (std::byte byte : suffix.path_) {
        buffer1.push_back(byte);
      }
      suffix.path_ = buffer1;
    }
    // push discriminative value part to value suffix
    if (iV < node->LenValue()) {
      buffer1.clear();
      for (size_t i = iV; i < node->LenValue(); ++i) {
        buffer1.push_back(static_cast<std::byte>(node->Value()[i]));
      }
      for (std::byte byte : suffix.value_) {
        buffer1.push_back(byte);
      }
      suffix.value_ = buffer1;
    }
  }

  // now we delete the discriminative part from the node
  std::vector<uint8_t> buffer2;
  for (size_t i = 0; i < iP; ++i) {
    buffer2.push_back(node->Path()[i]);
  }
  for (size_t i = 0; i < iV; ++i) {
    buffer2.push_back(node->Value()[i]);
  }
  node->separator_pos_ = iP;
  node->prefixes_ = buffer2;

  // add the new key to the suffixes
  cas::MemoryKey suffix;
  for (size_t i = gP; i < bkey_.LenPath(); ++i) {
    suffix.path_.push_back(bkey_.Path()[i]);
  }
  for (size_t i = gV; i < bkey_.LenValue(); ++i) {
    suffix.value_.push_back(bkey_.Value()[i]);
  }
  suffix.ref_ = bkey_.Ref();
  node->suffixes_.push_back(suffix);
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

  // update node's prefixes, remove common bytes
  uint8_t node_byte;
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

  // create new leaf node for new key
  uint8_t leaf_byte;
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
  /* std::cout << "[DEBUG] gP: " << gP << "; gV: " << gV << "\n"; */
  auto* leaf = new cas::mem::Node0(bkey_, gP, gV);

  // re-wire nodes
  intermediate_parent->Put(leaf_byte, leaf);
  intermediate_parent->Put(node_byte, node);

  return intermediate_parent;
}
