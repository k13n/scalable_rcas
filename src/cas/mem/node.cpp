#include "cas/mem/node.hpp"
#include <iostream>

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


void cas::mem::Node::DumpRecursive(uint8_t parent_byte, int depth) const {
  for (int i = 0; i < depth; ++i) {
    std::cout << "  ";
  }
  if (depth > 0) {
    printf("[%02X] ", static_cast<unsigned char>(parent_byte));
  }
  switch (Dimension()) {
    case cas::Dimension::PATH:  std::cout << "[P]"; break;
    case cas::Dimension::VALUE: std::cout << "[V]"; break;
    case cas::Dimension::LEAF:  std::cout << "[L]"; break;
  }
  std::cout << " [sp: ";
  for (size_t i = 0; i < LenPath(); ++i) {
    printf("%02X", static_cast<unsigned char>(Path()[i]));
    if (i < LenPath() - 1) {
      printf(" ");
    }
  }
  std::cout << "] [sv: ";
  for (size_t i = 0; i < LenValue(); ++i) {
    printf("%02X", static_cast<unsigned char>(Value()[i]));
    if (i < LenValue() - 1) {
      printf(" ");
    }
  }
  std::cout << "]\n";
  ForEachChild(0x00, 0xFF, [&](uint8_t byte, Node* child){
    child->DumpRecursive(byte, depth+1);
  });
  ForEachSuffix([&](const auto& suffix){
    for (int i = 0; i < depth+1; ++i) {
      std::cout << "  ";
    }
    std::cout << "  suffix: ";
    std::cout << " [sp: ";
    for (size_t i = 0; i < suffix.path_.size(); ++i) {
      printf("%02X", static_cast<unsigned char>(suffix.path_[i]));
      if (i < suffix.path_.size() - 1) {
        printf(" ");
      }
    }
    std::cout << "] [sv: ";
    for (size_t i = 0; i < suffix.value_.size(); ++i) {
      printf("%02X", static_cast<unsigned char>(suffix.value_[i]));
      if (i < suffix.value_.size() - 1) {
        printf(" ");
      }
    }
    std::cout << "] [ref: " << cas::ToString(suffix.ref_);
    std::cout << "]\n";
  });
}