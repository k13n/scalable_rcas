#pragma once

#include "cas/search_key.hpp"
#include "cas/types.hpp"
#include <cstdint>
#include <vector>


namespace cas::path_matcher {

enum class PrefixMatch {
  MATCH,
  MISMATCH,
  INCOMPLETE,
};

std::string ToString(PrefixMatch m);

struct State {
  uint16_t ppos_ = 0;
  uint16_t qpos_ = 0;
  uint16_t desc_ppos_ = 0;
  int16_t  desc_qpos_ = -1;
  uint16_t star_ppos_ = 0;
  int16_t  star_qpos_ = -1;

  void Dump() const;
};

PrefixMatch MatchPathIncremental(
    const cas::QueryBuffer& path,
    const std::vector<std::byte>& query_path,
    size_t len_path,
    State& state);

bool MatchPath(
    const cas::QueryBuffer& path,
    const std::vector<std::byte>& query_path,
    size_t len_path);

} // namespace cas::path_matcher
