#pragma once

#include <cstddef>
#include <cstdint>

namespace cas {

// special byte sequences used for encoding/decoding
const std::byte kNullByte{0x00};
const std::byte kPathSep{0xFF};
const std::byte kByteChildAxis{0xFD};
const std::byte kByteDescendantOrSelf{0xFE};

// bit-masks
const uint64_t kMsbMask64 = (1UL << (64-1));
const uint32_t kMsbMask32 = (1UL << (32-1));

} // namespace cas
