#include "cas/mem/node.hpp"

cas::mem::Node::Node(cas::Dimension dimension)
  : dimension_(dimension)
{}


cas::Dimension cas::mem::Node::Dimension() const {
  return dimension_;
}


size_t cas::mem::Node::LenPath() const {
  return separator_pos_;
}


size_t cas::mem::Node::LenValue() const {
  return prefixes_.size() - separator_pos_;
}


const uint8_t* cas::mem::Node::Path() const {
  return &prefixes_[0];
}


const uint8_t* cas::mem::Node::Value() const {
  return &prefixes_[separator_pos_];
}


bool cas::mem::Node::IsFull() const {
  return NodeWidth() == nr_children_;
}


uint16_t cas::mem::Node::NrChildren() const {
  return nr_children_;
}


uint16_t cas::mem::Node::NrSuffixes() const {
  return nr_suffixes_;
}
