#pragma once

#include "cas/binary_key.hpp"
#include "cas/key.hpp"
#include "cas/key_encoding.hpp"
#include "cas/types.hpp"


namespace cas {

template<class VType>
class KeyDecoder {
private:
  KeyDecoder(); // avoid instantiation

public:
  static Key<VType> Decode(
      const QueryBuffer& buffer_path, size_t len_path,
      const QueryBuffer& buffer_value, size_t len_value,
      const ref_t& ref,
      bool reverse_paths = false);

  static std::string DecodePath(
      const QueryBuffer& buffer,
      size_t len,
      bool reverse_paths);

  static VType DecodeValue(
      const QueryBuffer& buffer,
      size_t len);

private:
  static std::string DecodeForwardPath(const QueryBuffer& buffer, size_t len);
  static std::string DecodeBackwardPath(const QueryBuffer& buffer, size_t len);

  static inline void MemCpyFromBuffer(
      const QueryBuffer& buffer,
      int& offset,
      void* value,
      size_t size) {
    std::memcpy(value, &buffer[offset], size);
    offset += size;
  }

};

} // namespace cas
