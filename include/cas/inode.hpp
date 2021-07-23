#pragma once

#include "cas/dimension.hpp"
#include "cas/swh_pid.hpp"
#include <functional>


namespace cas {


class INode {
public:

  virtual ~INode() = default;

  // meta information
  virtual cas::Dimension Dimension() const = 0;
  virtual size_t LenPath() const = 0;
  virtual size_t LenValue() const = 0;
  virtual const uint8_t* Path() const = 0;
  virtual const uint8_t* Value() const = 0;
  virtual size_t NrChildren() const = 0;
  virtual size_t NrSuffixes() const = 0;

  inline bool IsInnerNode() const {
    return Dimension() != cas::Dimension::LEAF;
  }

  inline bool IsLeaf() const {
    return Dimension() == cas::Dimension::LEAF;
  }

  // traversing the node
  using ChildCallback = std::function<void(
      uint8_t byte, INode* child)>;

  using SuffixCallback = std::function<void(
      size_t len_p, const uint8_t* path,
      size_t len_v, const uint8_t* value,
      cas::ref_t ref)>;

  virtual void ForEachChild(const ChildCallback& callback) const = 0;
  virtual void ForEachSuffix(const SuffixCallback& callback) const = 0;
};


} // namespace cas
