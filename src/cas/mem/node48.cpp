#include "cas/mem/node48.hpp"
#include "cas/mem/node256.hpp"
#include <stdexcept>


cas::mem::Node48::Node48(cas::Dimension dimension)
    : cas::mem::Node(dimension)
{
  memset(indexes_, cas::mem::kEmptyIndex, 256*sizeof(uint8_t));
  memset(children_, 0, 48*sizeof(uintptr_t));
}


void cas::mem::Node48::ForEachChild(const cas::INode::ChildCallback& callback) const {
  for (int i = 0x00; i <= 0xFF; ++i) {
    if (indexes_[i] != cas::mem::kEmptyIndex) {
      callback(static_cast<uint8_t>(i), children_[indexes_[i]]);
    }
  }
}


cas::mem::Node* cas::mem::Node48::LocateChild(uint8_t key_byte) const {
  uint8_t index = indexes_[key_byte];
  if (index == cas::mem::kEmptyIndex) {
    return nullptr;
  }
  return children_[index];
}


void cas::mem::Node48::Put(uint8_t key_byte, Node* child) {
  if (nr_children_ >= 48) {
    throw std::runtime_error{"Node48 size limit reached"};
  }
  int pos = 0;
  while (pos < 48 && children_[pos] != nullptr) {
    ++pos;
  }
  indexes_[key_byte] = pos;
  children_[pos] = child;
  ++nr_children_;
}


void cas::mem::Node48::ReplaceBytePointer(uint8_t key_byte, cas::mem::Node* child) {
  uint8_t pos = indexes_[key_byte];
  if (pos == cas::mem::kEmptyIndex) {
    throw std::runtime_error{"key_byte not contained in Node48"};
  }
  children_[pos] = child;
}


cas::mem::Node* cas::mem::Node48::Grow() {
  auto* node256 = new cas::mem::Node256(dimension_);
  node256->nr_children_ = 48;
  node256->separator_pos_ = separator_pos_;
  node256->prefixes_ = std::move(prefixes_);
  for (int i = 0; i < 256; ++i) {
    if (indexes_[i] != cas::mem::kEmptyIndex) {
      node256->children_[i] = children_[indexes_[i]];
    }
  }
  return node256;
}
