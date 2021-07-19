#include "cas/query2.hpp"
#include "cas/key_decoder.hpp"
#include "cas/util.hpp"
#include <cassert>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <list>


template<class VType, size_t PAGE_SZ>
cas::Query2<VType, PAGE_SZ>::Query2(
        const uint8_t* file,
        const cas::BinarySK& key,
        const cas::BinaryKeyEmitter& emitter)
    : file_(file)
    , key_(key)
    , emitter_(emitter)
    , buf_pat_(std::make_unique<QueryBuffer>())
    , buf_val_(std::make_unique<QueryBuffer>())
{}


template<class VType, size_t PAGE_SZ>
std::list<cas::Key<VType>> cas::Query2<VType, PAGE_SZ>::Execute() {
  const auto& t_start = std::chrono::high_resolution_clock::now();
  std::list<Key<VType>> resultSet;
  State initial_state;
  initial_state.is_root_ = true;
  initial_state.idx_position_ = 0;
  initial_state.parent_dimension_ = cas::Dimension::PATH; // doesn't matter
  initial_state.len_pat_ = 0;
  initial_state.len_val_ = 0;
  initial_state.vl_pos_ = 0;
  initial_state.vh_pos_ = 0;
  stack_.push_back(initial_state);

  while (!stack_.empty()) {
    State s = stack_.back();
    stack_.pop_back();

    cas::NodeReader node{file_ + s.idx_position_};

    UpdateStats(node);
    PrepareBuffer(s, node);

    switch (node.Dimension()) {
      case cas::Dimension::PATH:
      case cas::Dimension::VALUE:
        EvaluateInnerNode(s, node);
        break;
      case cas::Dimension::LEAF:
        EvaluateLeafNode(s, node);
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
void cas::Query2<VType, PAGE_SZ>::EvaluateInnerNode(State& s, const NodeReader& node) {
  cas::path_matcher::PrefixMatch match_pat = MatchPathPrefix(s);
  cas::path_matcher::PrefixMatch match_val = MatchValuePrefix(s);

  if (match_pat == path_matcher::PrefixMatch::MATCH &&
      match_val == path_matcher::PrefixMatch::MATCH) {
    assert(node.Dimension() == cas::Dimension::LEAF); // NOLINT
    EmitMatches(s, node);
  } else if (match_pat != path_matcher::PrefixMatch::MISMATCH &&
             match_val != path_matcher::PrefixMatch::MISMATCH) {
    assert(node.Dimension() != cas::Dimension::LEAF); // NOLINT
    Descend(s, node);
  }
}



template<class VType, size_t PAGE_SZ>
void cas::Query2<VType, PAGE_SZ>::EvaluateLeafNode(State& s,
    const NodeReader& node) {
  cas::path_matcher::PrefixMatch match_pat = MatchPathPrefix(s);
  cas::path_matcher::PrefixMatch match_val = MatchValuePrefix(s);

  // we can immediately discard this node if one of its
  // common prefixes mismatches
  if (match_pat == path_matcher::PrefixMatch::MISMATCH ||
      match_val == path_matcher::PrefixMatch::MISMATCH) {
    return;
  }

  // check for a MATCH with each key in the leaf
  node.ForEachSuffix([&](
        uint8_t len_p, const uint8_t* path,
        uint8_t len_v, const uint8_t* value,
        cas::ref_t ref){
    // we need to copy the state since it is mutated
    State leaf_state = s;
    std::memcpy(&buf_pat_->at(leaf_state.len_pat_), path, len_p);
    std::memcpy(&buf_val_->at(leaf_state.len_val_), value, len_v);
    leaf_state.len_pat_ += len_p;
    leaf_state.len_val_ += len_v;
    match_pat = MatchPathPrefix(leaf_state);
    match_val = MatchValuePrefix(leaf_state);
    if (match_pat == path_matcher::PrefixMatch::MATCH &&
        match_val == path_matcher::PrefixMatch::MATCH) {
      EmitMatch(leaf_state, ref);
    }
  });
}


template<class VType, size_t PAGE_SZ>
void cas::Query2<VType, PAGE_SZ>::UpdateStats(const NodeReader& node) {
  ++stats_.read_nodes_;
  switch (node.Dimension()) {
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
void cas::Query2<VType, PAGE_SZ>::PrepareBuffer(State& s, const cas::NodeReader& node) {
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
  std::memcpy(&buf_pat_->at(s.len_pat_), node.Path(), node.LenPath());
  std::memcpy(&buf_val_->at(s.len_val_), node.Value(), node.LenValue());
  s.len_pat_ += node.LenPath();
  s.len_val_ += node.LenValue();
}


template<class VType, size_t PAGE_SZ>
cas::path_matcher::PrefixMatch
cas::Query2<VType, PAGE_SZ>::MatchPathPrefix(State& s) {
  return cas::path_matcher::MatchPathIncremental(*buf_pat_, key_.path_,
      s.len_pat_, s.pm_state_);
}


template<class VType, size_t PAGE_SZ>
cas::path_matcher::PrefixMatch
cas::Query2<VType, PAGE_SZ>::MatchValuePrefix(State& s) {
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
void cas::Query2<VType, PAGE_SZ>::Descend(const State& s, const cas::NodeReader& node) {
  switch (node.Dimension()) {
  case cas::Dimension::PATH:
    DescendPathNode(s, node);
    break;
  case cas::Dimension::VALUE:
    DescendValueNode(s, node);
    break;
  case cas::Dimension::LEAF:
    assert(false); // NOLINT
    break;
  }
}


template<class VType, size_t PAGE_SZ>
void cas::Query2<VType, PAGE_SZ>::DescendPathNode(const State& s, const cas::NodeReader& node) {
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
  DescendNode(s, node, low, high);
}


template<class VType, size_t PAGE_SZ>
void cas::Query2<VType, PAGE_SZ>::DescendValueNode(const State& s, const cas::NodeReader& node) {
  std::byte low  = (s.vl_pos_ == s.len_val_) ? key_.low_[s.vl_pos_]  : std::byte{0x00};
  std::byte high = (s.vh_pos_ == s.len_val_) ? key_.high_[s.vh_pos_] : std::byte{0xFF};
  DescendNode(s, node, low, high);
}


template<class VType, size_t PAGE_SZ>
void cas::Query2<VType, PAGE_SZ>::DescendNode(const State& s,
    const cas::NodeReader& node,
    std::byte low, std::byte high) {
  node.ForEachChild([&](uint8_t byte, size_t pos){
    if (static_cast<uint8_t>(low) <= byte && byte <= static_cast<uint8_t>(high)) {
      cas::Query2<VType, PAGE_SZ>::State copy = {
        .is_root_          = false,
        .idx_position_     = pos,
        .parent_dimension_ = node.Dimension(),
        .parent_byte_      = static_cast<std::byte>(byte),
        .len_pat_          = s.len_pat_,
        .len_val_          = s.len_val_,
        .pm_state_         = s.pm_state_,
        .vl_pos_           = s.vl_pos_,
        .vh_pos_           = s.vh_pos_,
        .depth_            = s.depth_+1,
      };
      stack_.push_back(std::move(copy));
    }
  });
}


template<class VType, size_t PAGE_SZ>
void cas::Query2<VType, PAGE_SZ>::EmitMatches(State& s,
      const cas::NodeReader& node) {
  node.ForEachSuffix([&](
        uint8_t len_p, const uint8_t* path,
        uint8_t len_v, const uint8_t* value,
        cas::ref_t ref){
    auto s_len_pat = s.len_pat_;
    auto s_len_val = s.len_val_;

    std::memcpy(&buf_pat_->at(s.len_pat_), path, len_p);
    std::memcpy(&buf_val_->at(s.len_val_), value, len_v);
    s.len_pat_ += len_p;
    s.len_val_ += len_v;
    EmitMatch(s, ref);

    s.len_pat_ = s_len_pat;
    s.len_val_ = s_len_val;
  });
}


template<class VType, size_t PAGE_SZ>
void cas::Query2<VType, PAGE_SZ>::EmitMatch(const State& s,
      const cas::ref_t& ref) {
  ++stats_.nr_matches_;
  stats_.sum_depth_ += s.depth_;
  emitter_(*buf_pat_, s.len_pat_, *buf_val_, s.len_val_, ref);
}


template<class VType, size_t PAGE_SZ>
void cas::Query2<VType, PAGE_SZ>::State::Dump() const {
  std::cout << "(QueryState)";
  std::cout << "\nidx_position_: " << idx_position_;
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
void cas::Query2<VType, PAGE_SZ>::DumpState(State& s) {
  std::cout << "(Query2)";
  std::cout << "\nbuf_pat_: ";
  cas::util::DumpHexValues(*buf_pat_, s.len_pat_);
  std::cout << "\nbuf_val_: ";
  cas::util::DumpHexValues(*buf_val_, s.len_val_);
  std::cout << "\nState:" << "\n";
  s.Dump();
}


template<class VType, size_t PAGE_SZ>
void cas::Query2<VType, PAGE_SZ>::CopyFromPage(
    const cas::IdxPage<PAGE_SZ>& page,
    uint16_t& offset, void* dst, size_t count) {
  if (offset + count > page.size()) {
    auto msg = "page address violation: " + std::to_string(offset+count);
    throw std::out_of_range{msg};
  }
  std::memcpy(dst, page.begin()+offset, count);
  offset += count;
}


template<>
bool cas::Query2<cas::vint64_t,cas::PAGE_SZ_4KB>::IsCompleteValue(State& s) {
  return s.len_val_ == sizeof(cas::vint64_t);
}
template<>
bool cas::Query2<cas::vint64_t,cas::PAGE_SZ_8KB>::IsCompleteValue(State& s) {
  return s.len_val_ == sizeof(cas::vint64_t);
}
template<>
bool cas::Query2<cas::vint64_t,cas::PAGE_SZ_16KB>::IsCompleteValue(State& s) {
  return s.len_val_ == sizeof(cas::vint64_t);
}
template<>
bool cas::Query2<cas::vint64_t,cas::PAGE_SZ_32KB>::IsCompleteValue(State& s) {
  return s.len_val_ == sizeof(cas::vint64_t);
}
template<>
bool cas::Query2<cas::vint64_t,cas::PAGE_SZ_64KB>::IsCompleteValue(State& s) {
  return s.len_val_ == sizeof(cas::vint64_t);
}


// explicit instantiations to separate header from implementation
template class cas::Query2<cas::vint64_t, cas::PAGE_SZ_64KB>;
template class cas::Query2<cas::vint64_t, cas::PAGE_SZ_32KB>;
template class cas::Query2<cas::vint64_t, cas::PAGE_SZ_16KB>;
template class cas::Query2<cas::vint64_t, cas::PAGE_SZ_8KB>;
template class cas::Query2<cas::vint64_t, cas::PAGE_SZ_4KB>;
