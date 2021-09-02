#include "cas/linear_search.hpp"
#include "cas/memory_page.hpp"
#include "cas/path_matcher.hpp"
#include <chrono>


template<class VType>
cas::LinearSearch<VType>::LinearSearch(
    Partition& partition,
    const BinarySK& key,
    const cas::BinaryKeyEmitter& emitter)
    : partition_(partition)
    , key_(key)
    , emitter_(emitter)
    , buf_pat_(std::make_unique<QueryBuffer>())
    , buf_val_(std::make_unique<QueryBuffer>())
{}


template<class VType>
void cas::LinearSearch<VType>::Execute() {
  const auto& t_start = std::chrono::high_resolution_clock::now();
  std::array<std::byte, cas::PAGE_SZ> io_page_buffer;
  cas::MemoryPage io_page{&io_page_buffer[0]};
  auto cursor = partition_.Cursor(io_page);
  while (cursor.HasNext()) {
    auto& page = cursor.NextPage();
    for (auto bkey : page) {
      std::memcpy(buf_pat_->data(), bkey.Path(),  bkey.LenPath());
      std::memcpy(buf_val_->data(), bkey.Value(), bkey.LenValue());
      bool match_pat = MatchPath(bkey.LenPath());
      bool match_val = MatchValue(bkey.LenValue());
      if (match_pat && match_val) {
        ++stats_.nr_matches_;
        emitter_(
            *buf_pat_, bkey.LenPath(),
            *buf_val_, bkey.LenValue(),
            bkey.Ref());
      }
    }
  }
  const auto& t_end = std::chrono::high_resolution_clock::now();
  stats_.runtime_mus_ = std::chrono::duration_cast<std::chrono::microseconds>(t_end-t_start).count();
}


template<class VType>
bool cas::LinearSearch<VType>::MatchPath(uint16_t len_pat) {
  return cas::path_matcher::MatchPath(
      *buf_pat_, key_.path_, len_pat);
}


template<class VType>
bool cas::LinearSearch<VType>::MatchValue(uint16_t len_val) {
  uint16_t vl_pos = 0;
  uint16_t vh_pos = 0;

  // match as much as possible of key_.low_
  while (vl_pos < key_.low_.size() &&
         vl_pos < len_val &&
         buf_val_->at(vl_pos) == key_.low_[vl_pos]) {
    ++vl_pos;
  }
  // match as much as possible of key_.high_
  while (vh_pos < key_.high_.size() &&
         vh_pos < len_val &&
         buf_val_->at(vh_pos) == key_.high_[vh_pos]) {
    ++vh_pos;
  }

  if (vl_pos < key_.low_.size() && vl_pos < len_val &&
      buf_val_->at(vl_pos) < key_.low_[vl_pos]) {
    // buf_val_ < key_.low_
    return false;
  }

  if (vh_pos < key_.high_.size() && vh_pos < len_val &&
      buf_val_->at(vh_pos) > key_.high_[vh_pos]) {
    // buf_val_ > key_.high_
    return false;
  }

  return true;
}


// explicit instantiations to separate header from implementation
template class cas::LinearSearch<cas::vint64_t>;
