#pragma once

#include "cas/dimension.hpp"
#include "cas/binary_key.hpp"
#include <cstdint>
#include <cstring>
#include <vector>
#include <functional>


namespace cas {
namespace mem {

class Node;
using ChildIterator = std::function<void(uint8_t, Node*)>;

using SuffixIterator = std::function<void(const cas::MemoryKey&)>;



class Node {
public:
  cas::Dimension dimension_;
  uint16_t nr_children_ = 0;
  uint16_t separator_pos_ = 0;
  std::vector<uint8_t> prefixes_;

  Node(cas::Dimension dimension);
  virtual ~Node() = default;

  // meta information
  cas::Dimension Dimension() const;
  size_t LenPath() const;
  size_t LenValue() const;
  const uint8_t* Path() const;
  const uint8_t* Value() const;
  bool IsFull() const;
  uint16_t NrChildren() const;
  virtual size_t NrSuffixes() const = 0;
  virtual int NodeWidth() const = 0;

  // traversing
  virtual Node* LocateChild(uint8_t key_byte) const = 0;
  virtual void ForEachChild(uint8_t low, uint8_t high, const ChildIterator& callback) const = 0;
  virtual void ForEachSuffix(const SuffixIterator& callback) const = 0;

  // updating
  virtual void Put(uint8_t key_byte, Node *child) = 0;
  virtual Node* Grow() = 0;
  virtual void ReplaceBytePointer(uint8_t key_byte, Node* child) = 0;

  // dumping
  void DumpRecursive(uint8_t parent_byte = 0, int depth = 0) const;
};


} // namespace mem
} // namespace cas
