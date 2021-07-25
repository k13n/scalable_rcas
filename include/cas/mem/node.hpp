#pragma once

#include "cas/inode.hpp"
#include "cas/binary_key.hpp"
#include <cstdint>
#include <cstring>
#include <vector>


namespace cas {
namespace mem {


class Node : public INode {
public:
  using RefCallback = std::function<void(const cas::ref_t&)>;

  cas::Dimension dimension_;
  uint16_t nr_children_ = 0;
  uint16_t separator_pos_ = 0;
  std::vector<uint8_t> prefixes_;

  Node(cas::Dimension dimension) : dimension_{dimension} {};
  virtual ~Node() = default;

  // meta information
  inline cas::Dimension Dimension() const override{
    return dimension_;
  }
  inline size_t LenPath() const override {
    return separator_pos_;
  }
  inline size_t LenValue() const override {
    return prefixes_.size() - separator_pos_;
  }
  const uint8_t* Path() const override {
    return &prefixes_[0];
  }
  const uint8_t* Value() const override {
    return &prefixes_[separator_pos_];
  }
  size_t NrChildren() const override {
    return nr_children_;
  }
  bool IsFull() const {
    return NodeWidth() == nr_children_;
  };

  // interface
  virtual int NodeWidth() const = 0;
  virtual Node* LocateChild(uint8_t key_byte) const = 0;
  virtual void Put(uint8_t key_byte, Node *child) = 0;
  virtual Node* Grow() = 0;
  virtual void ReplaceBytePointer(uint8_t key_byte, Node* child) = 0;

  virtual void ForEachSuffix(const RefCallback&) const {
    // NO-OP
  }

  void ForEachSuffix(const cas::INode::SuffixCallback& callback) const override {
    ForEachSuffix([&](const cas::ref_t& ref) {
      callback(0, nullptr, 0, nullptr, ref);
    });
  };

  // dumping
  void DumpRecursive(uint8_t parent_byte = 0, int depth = 0) const;
};


} // namespace mem
} // namespace cas
