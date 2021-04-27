#pragma once

#include "cas/binary_key.hpp"
#include "cas/key.hpp"
#include "cas/key_encoding.hpp"
#include "cas/search_key.hpp"


namespace cas {

template<class VType>
class KeyEncoder {
private:
  KeyEncoder(); // avoid instantiation

public:
  static void Encode(const Key<VType>& key, BinaryKey& bkey);
  static BinarySK Encode(const SearchKey<VType>& key, bool reversed = false);

private:
  static void EncodePath(const std::string& path, std::byte* buffer);
  static void EncodeQueryPath(const std::string& path, std::byte* buffer);
  static void EncodeReverseQueryPath(const std::string& path, std::byte* buffer);
  static void EncodeValue(const VType& value, std::byte* buffer);
  static size_t PathSize(const std::string& path);
  static size_t QueryPathSize(const std::string& path);
  static size_t ValueSize(const VType& value);
  static void MemCpyToBuffer(std::byte* buffer, int& offset,
    const void* value, std::size_t size);

};

} // namespace cas
