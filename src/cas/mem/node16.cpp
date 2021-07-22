#include "cas/mem/node16.hpp"
#include "cas/mem/node48.hpp"
#include <cassert>


cas::mem::Node16::Node16(cas::Dimension dimension)
  : cas::mem::Node(dimension)
{
  memset(static_cast<uint8_t*>(keys_), 0, 16*sizeof(uint8_t));
  memset(static_cast<Node**>(children_), 0, 16*sizeof(uintptr_t));
}


void cas::mem::Node16::ForEachChild(const cas::INode::ChildCallback& callback) const {
  for (int i = 0; i < nr_children_; ++i) {
    callback(keys_[i], children_[i]);
  }
}


cas::mem::Node* cas::mem::Node16::LocateChild(uint8_t key_byte) const {
  for (int i = 0; i < nr_children_; ++i) {
    if (key_byte == keys_[i]) {
      return children_[i];
    }
  }
  return nullptr;
}


void cas::mem::Node16::Put(uint8_t key_byte, Node* child) {
  if (nr_children_ >= 16) {
    throw std::runtime_error{"Node16 size limit reached"};
  }
  int pos = 0;
  while (pos < nr_children_ && key_byte > keys_[pos]) {
    ++pos;
  }
  std::memmove(keys_+pos+1, keys_+pos, (nr_children_-pos)*sizeof(uint8_t));
  std::memmove(children_+pos+1, children_+pos, (nr_children_-pos)*sizeof(uintptr_t));
  keys_[pos] = key_byte;
  children_[pos] = child;
  ++nr_children_;
}


void cas::mem::Node16::ReplaceBytePointer(uint8_t key_byte, cas::mem::Node* child) {
  for (int i = 0; i < nr_children_; ++i) {
    if (keys_[i] == key_byte) {
      children_[i] = child;
      return;
    }
  }
  throw std::runtime_error{"key_byte not contained in Node16"};
}


cas::mem::Node* cas::mem::Node16::Grow() {
  auto* node48 = new cas::mem::Node48(dimension_);
  node48->nr_children_ = 16;
  node48->separator_pos_ = separator_pos_;
  node48->prefixes_ = std::move(prefixes_);
  for (int i = 0; i < nr_children_; ++i) {
      node48->indexes_[keys_[i]] = i;
  }
  std::memcpy(node48->children_, children_, 16*sizeof(uintptr_t));
  return node48;
}
