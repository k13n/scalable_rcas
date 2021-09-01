#pragma once

#include "cas/bulk_loader_stats.hpp"
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>


namespace cas::util {


std::string Exec(const char* cmd);
void ClearPageCache();


inline uint16_t EncodeSizes(size_t plen, size_t vlen) {
  uint16_t result = 0;
  result |= (static_cast<uint16_t>(plen <<  4) & 0xFFF0);
  result |= (static_cast<uint16_t>(vlen <<  0) & 0x000F);
  return result;
}


inline std::pair<uint16_t, uint16_t> DecodeSizes(uint16_t value) {
  uint16_t plen = 0;
  uint16_t vlen = 0;
  plen |= ((value >>  4) & 0x0FFF);
  vlen |= ((value >>  0) & 0x000F);
  return {plen, vlen};
}


void DumpHexValues(const std::vector<std::byte>& buffer);
void DumpHexValues(const std::vector<std::byte>& buffer, size_t size);
void DumpHexValues(const std::vector<std::byte>& buffer, size_t offset, size_t size);
void DumpHexValues(const std::byte* buffer, size_t size);
void DumpHexValues(const std::byte* buffer, size_t offset, size_t size);
void DumpHexValues(const uint8_t* buffer, size_t offset, size_t size);
std::string IndexName(bool reverse_paths);
std::string CreateLink(std::string revision);
template<size_t N>
void DumpHexValues(const std::array<std::byte, N>& buffer,
    size_t size) {
  for (size_t i = 0; i < size && i < buffer.size(); ++i) {
    printf("0x%02X", static_cast<unsigned char>(buffer[i])); // NOLINT
    if (i < buffer.size()-1) {
      printf(" "); // NOLINT
    }
  }
}


inline void AddToTimer(
    cas::Timer& timer,
    const std::chrono::time_point<std::chrono::high_resolution_clock>& start) {
  auto now = std::chrono::high_resolution_clock::now();
  ++timer.count_;
  timer.time_ += std::chrono::duration_cast<std::chrono::microseconds>(now - start);
}


inline void AddToTimer(
    cas::Timer& timer,
    const std::chrono::time_point<std::chrono::high_resolution_clock>& start,
    const std::chrono::time_point<std::chrono::high_resolution_clock>& end) {
  ++timer.count_;
  timer.time_ += std::chrono::duration_cast<std::chrono::microseconds>(end - start);
}


std::string CurrentIsoTime();
void Log(const std::string& msg);

std::string IsoToUnix(std::string isoTimestamp);
std::string UnixToIso(std::string unixTimestamp);
bool isUnixTimestampString(std::string unixTimestamp);
std::string calculateIndexDirection(std::string path);
std::string ReplaceString(std::string subject, const std::string& search,const std::string& replace);
std::string reversePath(std::string path);
bool isValidPath(std::string path);
bool isValidIsoTimestamp(std::string timestamp);
bool isValidDirection(std::string direction);
} // namespace cas::util
