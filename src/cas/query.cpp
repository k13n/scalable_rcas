#include "cas/query.hpp"
#include "cas/node_reader.hpp"
#include "cas/key_decoder.hpp"
#include "cas/util.hpp"
/* #include "cas/page_buffer.hpp" */
#include <cassert>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <list>


template<class VType, size_t PAGE_SZ>
cas::Query<VType, PAGE_SZ>::Query(
        Pager<PAGE_SZ>& pager,
        const PageBuffer<PAGE_SZ>& page_buffer,
        const cas::BinarySK& key,
        const cas::BinaryKeyEmitter& emitter)
    : pager_(pager)
    , page_buffer_(page_buffer)
    , key_(key)
    , emitter_(emitter)
    , buf_pat_(std::make_unique<QueryBuffer>())
    , buf_val_(std::make_unique<QueryBuffer>())
{}


template<class VType, size_t PAGE_SZ>
std::list<cas::Key<VType>> cas::Query<VType, PAGE_SZ>::Execute() {
  const auto& t_start = std::chrono::high_resolution_clock::now();
  std::list<Key<VType>> resultSet;
  State initial_state;
  initial_state.is_root_ = true;
  initial_state.page_nr_ = cas::ROOT_IDX_PAGE_NR;
  initial_state.page_ = pager_.Fetch(cas::ROOT_IDX_PAGE_NR);
  initial_state.page_offset_ = 0;
  initial_state.parent_dimension_ = cas::Dimension::PATH; // doesn't matter
  initial_state.len_pat_ = 0;
  initial_state.len_val_ = 0;
  initial_state.vl_pos_ = 0;
  initial_state.vh_pos_ = 0;
  stack_.push_back(initial_state);

  while (!stack_.empty()) {
    State s = stack_.back();
    stack_.pop_back();

    auto [record,found] = FetchRecord(s);
    if (!found) {
      continue;
    }
    record.page_nr_ = s.page_nr_;

    UpdateStats(record);
    PrepareBuffer(s, record);

    switch (record.dimension_) {
      case cas::Dimension::PATH:
      case cas::Dimension::VALUE:
        EvaluateInnerNode(s, record);
        break;
      case cas::Dimension::LEAF:
        EvaluateLeafNode(s, record);
        break;
      default:
        throw std::runtime_error{"illegal dimension"};
    }
  }

  const auto& t_end = std::chrono::high_resolution_clock::now();
  stats_.runtime_mus_ =
    std::chrono::duration_cast<std::chrono::microseconds>(t_end-t_start).count();
  return resultSet;
}


template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::EvaluateInnerNode(State& s, const Record& record) {
  cas::path_matcher::PrefixMatch match_pat = MatchPathPrefix(s);
  cas::path_matcher::PrefixMatch match_val = MatchValuePrefix(s);

  if (match_pat == path_matcher::PrefixMatch::MATCH &&
      match_val == path_matcher::PrefixMatch::MATCH) {
    assert(record.dimension_ == cas::Dimension::LEAF); // NOLINT
    EmitMatches(s, record);
  } else if (match_pat != path_matcher::PrefixMatch::MISMATCH &&
             match_val != path_matcher::PrefixMatch::MISMATCH) {
    assert(record.dimension_ != cas::Dimension::LEAF); // NOLINT
    Descend(s, record);
  }
}



template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::EvaluateLeafNode(State& s,
    const Record& record) {
  cas::path_matcher::PrefixMatch match_pat = MatchPathPrefix(s);
  cas::path_matcher::PrefixMatch match_val = MatchValuePrefix(s);

  // we can immediately discard this node if one of its
  // common prefixes mismatches
  if (match_pat == path_matcher::PrefixMatch::MISMATCH ||
      match_val == path_matcher::PrefixMatch::MISMATCH) {
    return;
  }

  // check for a MATCH with each key in the leaf
  for (const auto& leaf_key : record.references_) {
    // we need to copy the state since it is mutated
    State leaf_state = s;
    PrepareLeafBuffer(leaf_state, leaf_key);
    match_pat = MatchPathPrefix(leaf_state);
    match_val = MatchValuePrefix(leaf_state);
    if (match_pat == path_matcher::PrefixMatch::MATCH &&
        match_val == path_matcher::PrefixMatch::MATCH) {
      EmitMatch(leaf_state, leaf_key);
    }
  }
}


template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::UpdateStats(const Record& record) {
  ++stats_.read_nodes_;
  switch (record.dimension_) {
  case cas::Dimension::PATH:
    ++stats_.read_path_nodes_;
    break;
  case cas::Dimension::VALUE:
    ++stats_.read_value_nodes_;
    break;
  case cas::Dimension::LEAF:
    ++stats_.read_leaf_nodes_;
    break;
  }
}


template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::PrepareBuffer(State& s, const cas::Record& record) {
  if (!s.is_root_) {
    switch (s.parent_dimension_) {
    case cas::Dimension::PATH:
      buf_pat_->at(s.len_pat_) = s.parent_byte_;
      ++s.len_pat_;
      break;
    case cas::Dimension::VALUE:
      buf_val_->at(s.len_val_) = s.parent_byte_;
      ++s.len_val_;
      break;
    case cas::Dimension::LEAF:
      assert(false); // NOLINT
      break;
    }
  }
  size_t node_pat_len = record.path_.size();
  size_t node_val_len = record.value_.size();
  std::memcpy(&buf_pat_->at(s.len_pat_), &record.path_[0], node_pat_len);
  std::memcpy(&buf_val_->at(s.len_val_), &record.value_[0], node_val_len);
  s.len_pat_ += node_pat_len;
  s.len_val_ += node_val_len;
}


template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::PrepareLeafBuffer(State& s,
    const cas::MemoryKey& leaf_key) {
  size_t node_pat_len = leaf_key.path_.size();
  size_t node_val_len = leaf_key.value_.size();
  std::memcpy(&buf_pat_->at(s.len_pat_), &leaf_key.path_[0], node_pat_len);
  std::memcpy(&buf_val_->at(s.len_val_), &leaf_key.value_[0], node_val_len);
  s.len_pat_ += node_pat_len;
  s.len_val_ += node_val_len;
}


template<class VType, size_t PAGE_SZ>
cas::path_matcher::PrefixMatch
cas::Query<VType, PAGE_SZ>::MatchPathPrefix(State& s) {
  return cas::path_matcher::MatchPathIncremental(*buf_pat_, key_.path_,
      s.len_pat_, s.pm_state_);
}


template<class VType, size_t PAGE_SZ>
cas::path_matcher::PrefixMatch
cas::Query<VType, PAGE_SZ>::MatchValuePrefix(State& s) {
  // match as much as possible of key_.low_
  while (s.vl_pos_ < key_.low_.size() &&
         s.vl_pos_ < s.len_val_ &&
         buf_val_->at(s.vl_pos_) == key_.low_[s.vl_pos_]) {
    ++s.vl_pos_;
  }
  // match as much as possible of key_.high_
  while (s.vh_pos_ < key_.high_.size() &&
         s.vh_pos_ < s.len_val_ &&
         buf_val_->at(s.vh_pos_) == key_.high_[s.vh_pos_]) {
    ++s.vh_pos_;
  }

  if (s.vl_pos_ < key_.low_.size() && s.vl_pos_ < s.len_val_ &&
      buf_val_->at(s.vl_pos_) < key_.low_[s.vl_pos_]) {
    // buf_val_ < key_.low_
    return path_matcher::PrefixMatch::MISMATCH;
  }

  if (s.vh_pos_ < key_.high_.size() && s.vh_pos_ < s.len_val_ &&
      buf_val_->at(s.vh_pos_) > key_.high_[s.vh_pos_]) {
    // buf_val_ > key_.high_
    return path_matcher::PrefixMatch::MISMATCH;
  }

  return IsCompleteValue(s) ? path_matcher::PrefixMatch::MATCH
                            : path_matcher::PrefixMatch::INCOMPLETE;
}


template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::Descend(const State& s, const cas::Record& record) {
  switch (record.dimension_) {
  case cas::Dimension::PATH:
    DescendPathNode(s, record);
    break;
  case cas::Dimension::VALUE:
    DescendValueNode(s, record);
    break;
  case cas::Dimension::LEAF:
    assert(false); // NOLINT
    break;
  }
}


template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::DescendPathNode(const State& s, const cas::Record& record) {
  // default is to descend all children of s.node_
  std::byte low{0x00};
  std::byte high{0xFF};
  // check if we are looking for exactly one child
  if (!(s.pm_state_.desc_qpos_ != -1 ||
      s.pm_state_.star_qpos_ != -1 ||
      key_.path_[s.pm_state_.qpos_] == cas::kByteChildAxis)) {
    low  = key_.path_[s.pm_state_.qpos_];
    high = key_.path_[s.pm_state_.qpos_];
  }
  DescendNode(s, record, low, high);
}


template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::DescendValueNode(const State& s, const cas::Record& record) {
  std::byte low  = (s.vl_pos_ == s.len_val_) ? key_.low_[s.vl_pos_]  : std::byte{0x00};
  std::byte high = (s.vh_pos_ == s.len_val_) ? key_.high_[s.vh_pos_] : std::byte{0xFF};
  DescendNode(s, record, low, high);
}


template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::DescendNode(const State& s,
    const cas::Record& record,
    std::byte low, std::byte high) {
  bool first = true;
  ForEachChild(record, low, high, [&](std::byte byte, cas::page_nr_t page_nr, uint16_t offset){
    std::shared_ptr<cas::IdxPage<PAGE_SZ>> page = nullptr;
    if (page_nr == s.page_nr_) {
      // this is a itra-page pointer, copy the shared pointer
      page = s.page_;
    } else if (page_nr != s.page_nr_) {
      // this is a inter-page pointer
      if (!first && page_nr == stack_.back().page_nr_) {
        // we already fetched this page for the previous sibling
        // copy the pointer
        page = stack_.back().page_;
      } else {
        // this is a new page that we haven't fetched yet, fetch it
        page = FetchPage(page_nr);
      }
    }
    cas::Query<VType, PAGE_SZ>::State copy = {
      .is_root_          = false,
      .page_nr_          = page_nr,
      .page_offset_      = offset,
      .page_             = std::move(page),
      .parent_dimension_ = record.dimension_,
      .parent_byte_      = byte,
      .len_pat_          = s.len_pat_,
      .len_val_          = s.len_val_,
      .pm_state_         = s.pm_state_,
      .vl_pos_           = s.vl_pos_,
      .vh_pos_           = s.vh_pos_,
      .depth_            = s.depth_+1,
    };
    stack_.push_back(std::move(copy));
    first = false;
  });
}


template<class VType, size_t PAGE_SZ>
std::shared_ptr<cas::IdxPage<PAGE_SZ>>
cas::Query<VType, PAGE_SZ>::FetchPage(cas::page_nr_t page_nr) {
  ++stats_.page_reads_;
  auto page = page_buffer_.GetPage(page_nr);
  if (page != nullptr) {
    ++stats_.page_reads_from_buffer_;
    return page;
  }
  ++stats_.page_reads_from_disk_;
  return pager_.Fetch(page_nr);
}


template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::EmitMatches(State& s,
      const cas::Record& record) {
  for (const auto& leaf_key : record.references_) {
    auto s_len_pat = s.len_pat_;
    auto s_len_val = s.len_val_;
    PrepareLeafBuffer(s, leaf_key);
    EmitMatch(s, leaf_key);
    s.len_pat_ = s_len_pat;
    s.len_val_ = s_len_val;
  }
}


template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::EmitMatch(const State& s,
      const cas::MemoryKey& leaf_key) {
  ++stats_.nr_matches_;
  stats_.sum_depth_ += s.depth_;
  emitter_(*buf_pat_, s.len_pat_, *buf_val_, s.len_val_, leaf_key.ref_);
}


template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::State::Dump() const {
  std::cout << "(QueryState)";
  std::cout << "\npage_nr_: " << page_nr_;
  std::cout << "\npage_offset_: " << page_offset_;
  std::cout << "\nparent_dimension_: ";
  switch (parent_dimension_) {
  case cas::Dimension::PATH:
    std::cout << "Path";
    break;
  case cas::Dimension::VALUE:
    std::cout << "Value";
    break;
  case cas::Dimension::LEAF:
    assert(false); // NOLINT
    break;
  }
  printf("\nparent_byte_: 0x%02X", static_cast<unsigned char>(parent_byte_)); // NOLINT
  std::cout << "\nlen_val_: " << len_val_;
  std::cout << "\nlen_pat_: " << len_pat_;
  std::cout << "\n";
  pm_state_.Dump();
  std::cout << "vl_pos_: " << vl_pos_;
  std::cout << "\nvh_pos_: " << vh_pos_;
  std::cout << "\n";
}


template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::DumpState(State& s) {
  std::cout << "(Query)";
  std::cout << "\nbuf_pat_: ";
  cas::util::DumpHexValues(*buf_pat_, s.len_pat_);
  std::cout << "\nbuf_val_: ";
  cas::util::DumpHexValues(*buf_val_, s.len_val_);
  std::cout << "\nState:" << "\n";
  s.Dump();
}


template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::ForEachChild(const cas::Record& record, std::byte low, std::byte high,
    const ChildCallback& callback) {
  for (int byte = static_cast<int>(low); byte <= static_cast<int>(high); ++byte) {
    auto byte_val = static_cast<std::byte>(byte);
    if (record.child_pages_[byte] != 0) {
      callback(byte_val, record.child_pages_[byte], 0);
    } else if (record.child_pointers_[byte] != 0) {
      callback(byte_val, record.page_nr_, record.child_pointers_[byte]);
    }
  }
}


template<class VType, size_t PAGE_SZ>
void cas::Query<VType, PAGE_SZ>::CopyFromPage(
    const cas::IdxPage<PAGE_SZ>& page,
    uint16_t& offset, void* dst, size_t count) {
  if (offset + count > page.size()) {
    auto msg = "page address violation: " + std::to_string(offset+count);
    throw std::out_of_range{msg};
  }
  std::memcpy(dst, page.begin()+offset, count);
  offset += count;
}




template<class VType, size_t PAGE_SZ>
std::pair<cas::Record,bool> cas::Query<VType, PAGE_SZ>::FetchRecord(const State& s) {
  const auto& page = s.page_;
  uint16_t offset = s.page_offset_;
  if (offset > 0) {
    return {cas::PageParser<PAGE_SZ>::PageToRecord(*page, offset, pager_), true};
  }

  // if the partition_size is 0 it means it's actually 256, since we
  // cannot represent 256 in uint8_t
  uint8_t partition_size = 0;
  CopyFromPage(*page, offset, &partition_size, sizeof(uint8_t));
  int real_partition_size = partition_size;
  if (partition_size == 0) {
    real_partition_size = 256;
  }

  const int payload_beginning = 1+(real_partition_size*3);
  while (offset < payload_beginning) {
    uint8_t byte = 0;
    uint16_t address = 0;
    CopyFromPage(*page, offset, &byte, sizeof(byte));
    CopyFromPage(*page, offset, &address, sizeof(address));
    if (byte == static_cast<uint8_t>(s.parent_byte_)) {
      offset = address;
      return {cas::PageParser<PAGE_SZ>::PageToRecord(*page, offset, pager_), true};
    }
  }

  Record empty;
  return {empty, false};
}


template<>
bool cas::Query<cas::vint64_t,cas::PAGE_SZ_4KB>::IsCompleteValue(State& s) {
  return s.len_val_ == sizeof(cas::vint64_t);
}
template<>
bool cas::Query<cas::vint64_t,cas::PAGE_SZ_8KB>::IsCompleteValue(State& s) {
  return s.len_val_ == sizeof(cas::vint64_t);
}
template<>
bool cas::Query<cas::vint64_t,cas::PAGE_SZ_16KB>::IsCompleteValue(State& s) {
  return s.len_val_ == sizeof(cas::vint64_t);
}
template<>
bool cas::Query<cas::vint64_t,cas::PAGE_SZ_32KB>::IsCompleteValue(State& s) {
  return s.len_val_ == sizeof(cas::vint64_t);
}
template<>
bool cas::Query<cas::vint64_t,cas::PAGE_SZ_64KB>::IsCompleteValue(State& s) {
  return s.len_val_ == sizeof(cas::vint64_t);
}


// explicit instantiations to separate header from implementation
template class cas::Query<cas::vint64_t, cas::PAGE_SZ_64KB>;
template class cas::Query<cas::vint64_t, cas::PAGE_SZ_32KB>;
template class cas::Query<cas::vint64_t, cas::PAGE_SZ_16KB>;
template class cas::Query<cas::vint64_t, cas::PAGE_SZ_8KB>;
template class cas::Query<cas::vint64_t, cas::PAGE_SZ_4KB>;
