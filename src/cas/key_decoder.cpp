#include "cas/key_decoder.hpp"
#include <cstring>
#include <iostream>

template<class VType>
cas::Key<VType> cas::KeyDecoder<VType>::Decode(
      const cas::QueryBuffer& buffer_path, size_t len_path,
      const cas::QueryBuffer& buffer_value, size_t len_value,
      const cas::ref_t& ref,
      bool reverse_paths) {
  cas::Key<VType> key;
  key.path_  = DecodePath(buffer_path, len_path, reverse_paths);
  key.value_ = DecodeValue(buffer_value, len_value);
  key.ref_   = ref;
  return key;
}


template<class VType>
std::string cas::KeyDecoder<VType>::DecodePath(
      const cas::QueryBuffer& buffer,
      size_t len,
      bool reverse_paths) {
  return reverse_paths
    ?  DecodeBackwardPath(buffer, len)
    : DecodeForwardPath(buffer, len);
}


template<class VType>
std::string cas::KeyDecoder<VType>::DecodeForwardPath(
      const cas::QueryBuffer& buffer,
      size_t len) {
  --len; // we do not add the null byte to the path
  std::string path(len, '\0');
  int offset = 0;
  MemCpyFromBuffer(buffer, offset, &path[0], len);
  for (size_t i = 0; i < len; ++i) {
    if (path[i] == static_cast<char>(cas::kPathSep)) {
      path[i] = '/';
    }
  }
  return path;
}


template<class VType>
std::string cas::KeyDecoder<VType>::DecodeBackwardPath(
      const cas::QueryBuffer& buffer,
      size_t len) {
  --len; // we do not add the null byte to the path
  std::string path(len, '\0');
  auto pos = static_cast<long>(len-1);
  while (pos >= 0) {
    long start_pos = pos;
    while (pos >= 0 && buffer[pos] != cas::kPathSep) {
      --pos;
    }
    int offset = pos+1;
    MemCpyFromBuffer(buffer, offset, &path[len-start_pos-1], start_pos - pos);
    if (pos > 0) {
      path[len-pos-1] = '/';
    }
    --pos;
  }
  return path;
}


template<>
cas::vint32_t cas::KeyDecoder<cas::vint32_t>::DecodeValue(
    const cas::QueryBuffer& buffer, size_t len) {
  cas::vint32_t value;
  int offset = 0;
  MemCpyFromBuffer(buffer, offset, &value, len);
  value = __builtin_bswap32(value);
  value ^= cas::kMsbMask32;
  return value;
}


template<>
cas::vint64_t cas::KeyDecoder<cas::vint64_t>::DecodeValue(
    const cas::QueryBuffer& buffer, size_t len) {
  cas::vint64_t value;
  int offset = 0;
  MemCpyFromBuffer(buffer, offset, &value, len);
  value = __builtin_bswap64(value);
  value ^= cas::kMsbMask64;
  return value;
}


template<>
cas::vstring_t cas::KeyDecoder<cas::vstring_t>::DecodeValue(
    const cas::QueryBuffer& buffer, size_t len) {
  cas::vstring_t value(len, '\0');
  int offset = 0;
  MemCpyFromBuffer(buffer, offset, &value[0], len);
  return value;
}


// explicit instantiations to separate header from implementation
template class cas::KeyDecoder<cas::vint32_t>;
template class cas::KeyDecoder<cas::vint64_t>;
template class cas::KeyDecoder<cas::vstring_t>;
