#include "cas/key_encoder.hpp"
#include <cstring>
#include <string>
#include <cas/util.hpp>

using namespace cas;
template<class VType>
void cas::KeyEncoder<VType>::Encode(
    const cas::Key<VType>& key,
    cas::BinaryKey& bkey) {
  size_t len_p = PathSize(key.path_);
  size_t len_v = ValueSize(key.value_);
  bkey.LenPath(len_p);
  bkey.LenValue(len_v);
  EncodePath(key.path_, bkey.Path());
  EncodeValue(key.value_, bkey.Value());
  bkey.Ref(key.ref_);
};


template<class VType>
cas::BinarySK cas::KeyEncoder<VType>::Encode(
      const cas::SearchKey<VType>& key,
      bool reversed) {
  cas::BinarySK bkey;
  // do not encode the null-byte at the end of a query path
  bkey.path_ = std::vector<std::byte>(QueryPathSize(key.path_));
  bkey.low_  = std::vector<std::byte>(ValueSize(key.low_));
  bkey.high_ = std::vector<std::byte>(ValueSize(key.high_));
  EncodeValue(key.low_,  bkey.low_.data());
  EncodeValue(key.high_, bkey.high_.data());
  if (reversed) {
    EncodeQueryPath(cas::util::reversePath(key.path_), bkey.path_.data());
  } else {
    EncodeQueryPath(key.path_, bkey.path_.data());
  }
  return bkey;
}


template<class VType>
void cas::KeyEncoder<VType>::EncodePath(
    const std::string& path,
    std::byte* buffer) {
  int offset = 0;
  MemCpyToBuffer(buffer, offset, path.data(), path.size());
  buffer[offset++] = cas::kNullByte;
  for (int i = 0; i < offset; ++i) {
    const std::byte pathSeparator{0x2f};
    if (buffer[i] == pathSeparator) {
      buffer[i] = cas::kPathSep;
    }
  }
}


template<class VType>
void cas::KeyEncoder<VType>::EncodeQueryPath(
    const std::string& path,
    std::byte* buffer) {
  int pos = 0;
  bool escaped = false;
  for (char symbol : path) {
    if (escaped) {
        buffer[pos] = static_cast<std::byte>(symbol);
        ++pos;
        escaped = false;
    } else if (symbol == '\\') {
        escaped = true;
    } else if (symbol == '*') {
        buffer[pos] = cas::kByteChildAxis;
        ++pos;
    } else if (symbol == '/') {
        buffer[pos] = cas::kPathSep;
        ++pos;
    } else {
        buffer[pos] = static_cast<std::byte>(symbol);
        ++pos;
    }
  }
}


template<>
void cas::KeyEncoder<cas::vint32_t>::EncodeValue(
    const cas::vint32_t& value, std::byte* buffer) {
  int offset = 0;
  cas::vint32_t complement = static_cast<uint32_t>(value) ^ cas::kMsbMask32;
  complement = __builtin_bswap32(complement);
  MemCpyToBuffer(buffer, offset, &complement, sizeof(cas::vint32_t));
}

template<>
void cas::KeyEncoder<cas::vint64_t>::EncodeValue(
    const cas::vint64_t& value, std::byte* buffer) {
  int offset = 0;
  cas::vint64_t complement = static_cast<uint64_t>(value) ^ cas::kMsbMask64;
  complement = __builtin_bswap64(complement);
  MemCpyToBuffer(buffer, offset, &complement, sizeof(cas::vint64_t));
}

template<>
void cas::KeyEncoder<cas::vstring_t>::EncodeValue(
    const cas::vstring_t& value, std::byte* buffer) {
  int offset = 0;
  MemCpyToBuffer(buffer, offset, value.data(), value.size());
  buffer[offset++] = cas::kNullByte;
}


template<class VType>
size_t cas::KeyEncoder<VType>::PathSize(const std::string& path) {
  return path.size() + 1; // for the trailing null byte
}

template<class VType>
size_t cas::KeyEncoder<VType>::QueryPathSize(const std::string& path) {
    size_t s = 0;
    bool escaped = false;
    for (char symbol : path) {
        if (escaped) {
            ++s;
            escaped = false;
        } else if (symbol == '\\') {
            escaped = true;
        } else {
            ++s;
        }
    }
    return s;
}

template<class VType>
size_t cas::KeyEncoder<VType>::ValueSize(const VType& /* value */) {
  return sizeof(VType);
}

template<>
size_t cas::KeyEncoder<cas::vstring_t>::ValueSize(const cas::vstring_t& value) {
  return value.size() + 1; // for the trailing null byte
}


template<class VType>
inline void cas::KeyEncoder<VType>::MemCpyToBuffer(
    std::byte* buffer,
    int& offset,
    const void* value,
    std::size_t size) {
  std::memcpy(buffer + offset, value, size);
  offset += size;
}


// explicit instantiations to separate header from implementation
template class cas::KeyEncoder<cas::vint32_t>;
template class cas::KeyEncoder<cas::vint64_t>;
template class cas::KeyEncoder<cas::vstring_t>;
