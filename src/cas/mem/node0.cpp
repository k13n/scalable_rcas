#include "cas/mem/node0.hpp"


cas::mem::Node0::Node0()
  : cas::mem::Node(cas::Dimension::LEAF)
{ }


cas::mem::Node0::Node0(const cas::BinaryKey& bkey,
    size_t path_pos, size_t value_pos)
  : cas::mem::Node(cas::Dimension::LEAF)
{
  size_t len_path = bkey.LenPath() - path_pos;
  size_t len_value = bkey.LenValue() - value_pos;
  // fill prefix_ with the remaining path and value bytes
  prefixes_.resize(len_path + len_value);
  std::memcpy(&prefixes_[0], bkey.Path() + path_pos, len_path);
  std::memcpy(&prefixes_[0] + len_path, bkey.Value() + value_pos, len_value);
  separator_pos_ = len_path;
  // add the ref
  cas::MemoryKey memkey;
  memkey.ref_ = bkey.Ref();
  suffixes_.push_back(memkey);
}



void cas::mem::Node0::ForEachChild(
      uint8_t /* low */, uint8_t /* high */,
      const cas::mem::ChildIterator& /* callback */) const {
  // NO-OP
}


void cas::mem::Node0::ForEachSuffix(const cas::mem::SuffixIterator& callback) const {
  for (const auto& suffix : suffixes_) {
    callback(suffix);
  }
}


cas::mem::Node* cas::mem::Node0::LocateChild(uint8_t /* key_byte */) const {
  throw std::runtime_error{"calling LocateChild on Node0"};
}


void cas::mem::Node0::Put(uint8_t /* key_byte */, Node* /* child */) {
  throw std::runtime_error{"calling Put on Node0"};
}


void cas::mem::Node0::ReplaceBytePointer(uint8_t /* key_byte */, cas::mem::Node* /* child */) {
  throw std::runtime_error{"calling ReplaceBytePointer on Node0"};
}


cas::mem::Node* cas::mem::Node0::Grow() {
  throw std::runtime_error{"calling Grow on Node0"};
}
