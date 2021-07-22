#include "cas/mem/node4.hpp"
#include "cas/mem/node16.hpp"
#include <cassert>


cas::mem::Node4::Node4(cas::Dimension dimension)
  : cas::mem::Node(dimension)
{
  memset(keys_, 0, 4*sizeof(uint8_t));
  memset(children_, 0, 4*sizeof(uintptr_t));
}


void cas::mem::Node4::ForEachChild(
      uint8_t low, uint8_t high,
      const cas::mem::ChildIterator& callback) const {
  for (int i = 0; i < nr_children_; ++i) {
    if (low <= keys_[i] && keys_[i] <= high) {
      callback(keys_[i], children_[i]);
    }
  }
}


void cas::mem::Node4::ForEachSuffix(const cas::mem::SuffixIterator&) const {
  // NO-OP
}


cas::mem::Node* cas::mem::Node4::LocateChild(uint8_t key_byte) const {
  for (int i = 0; i < nr_children_; ++i) {
    if (key_byte == keys_[i]) {
      return children_[i];
    }
  }
  return nullptr;
}


void cas::mem::Node4::Put(uint8_t key_byte, Node* child) {
  if (nr_children_ >= 4) {
    throw std::runtime_error{"Node4 size limit reached"};
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


void cas::mem::Node4::ReplaceBytePointer(uint8_t key_byte, cas::mem::Node* child) {
  for (int i = 0; i < nr_children_; ++i) {
    if (keys_[i] == key_byte) {
      children_[i] = child;
      return;
    }
  }
  throw std::runtime_error{"key_byte not contained in Node4"};
}


cas::mem::Node* cas::mem::Node4::Grow() {
  auto* node16 = new cas::mem::Node16(dimension_);
  node16->nr_children_ = 4;
  node16->separator_pos_ = separator_pos_;
  node16->prefixes_ = std::move(prefixes_);
  std::memcpy(node16->keys_, keys_, 4*sizeof(uint8_t));
  std::memcpy(node16->children_, children_, 4*sizeof(uintptr_t));
  return node16;
}
