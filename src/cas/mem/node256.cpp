#include "cas/mem/node256.hpp"


cas::mem::Node256::Node256(cas::Dimension dimension)
    : cas::mem::Node(dimension)
{
  memset(children_, 0, 256*sizeof(uintptr_t));
}


void cas::mem::Node256::ForEachChild(const cas::INode::ChildCallback& callback) const {
  for (int i = 0; i <= 0xFF; ++i) {
    if (children_[i] != nullptr) {
      callback(static_cast<uint8_t>(i), children_[i]);
    }
  }
}


cas::mem::Node* cas::mem::Node256::LocateChild(uint8_t key_byte) const {
	return children_[key_byte];
}


void cas::mem::Node256::Put(uint8_t key_byte, Node* child) {
  if (children_[key_byte] != nullptr) {
    throw std::runtime_error{"key already occupied"};
  }
  children_[key_byte] = child;
  ++nr_children_;
}


void cas::mem::Node256::ReplaceBytePointer(uint8_t key_byte, cas::mem::Node* child) {
  if (children_[key_byte] == nullptr) {
		throw std::runtime_error{"key_byte not contained in Node256"};
  }
  children_[key_byte] = child;
}


cas::mem::Node* cas::mem::Node256::Grow() {
	throw std::runtime_error{"Node256 cannot grow"};
}
