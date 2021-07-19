#include "cas/query.hpp"
#include "cas/key_decoder.hpp"
#include "cas/util.hpp"
#include <cassert>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <list>


template<class VType>
cas::Query<VType>::Query(
        const uint8_t* file,
        const cas::BinarySK& key,
        const cas::BinaryKeyEmitter& emitter)
    : file_(file)
    , key_(key)
    , emitter_(emitter)
    , buf_pat_(std::make_unique<QueryBuffer>())
    , buf_val_(std::make_unique<QueryBuffer>())
{}


template<class VType>
void cas::Query<VType>::Execute() {
  const auto& t_start = std::chrono::high_resolution_clock::now();
  State initial_state = {
    .is_root_ = true,
    .idx_position_ = 0,
    .parent_dimension_ = cas::Dimension::PATH, // doesn't matter
    .parent_byte_ = cas::kNullByte,
    .len_pat_ = 0,
    .len_val_ = 0,
    .pm_state_ = {},
    .vl_pos_ = 0,
    .vh_pos_ = 0,
  };
  stack_.push_back(initial_state);

  while (!stack_.empty()) {
    State s = stack_.back();
    stack_.pop_back();

    cas::NodeReader node{file_ + s.idx_position_};
    UpdateStats(node);
    PrepareBuffer(s, node);

    if (node.IsInnerNode()) {
      EvaluateInnerNode(s, node);
    } else {
      EvaluateLeafNode(s, node);
    }
  }

  const auto& t_end = std::chrono::high_resolution_clock::now();
  stats_.runtime_mus_ =
    std::chrono::duration_cast<std::chrono::microseconds>(t_end-t_start).count();
}


template<class VType>
void cas::Query<VType>::EvaluateInnerNode(State& s, const NodeReader& node) {
  cas::path_matcher::PrefixMatch match_pat = MatchPathPrefix(s);
  cas::path_matcher::PrefixMatch match_val = MatchValuePrefix(s);
  if (match_pat == path_matcher::PrefixMatch::MATCH &&
      match_val == path_matcher::PrefixMatch::MATCH) {
    throw std::runtime_error{"an inner node cannot MATCH"};
  }
  if (match_pat != path_matcher::PrefixMatch::MISMATCH &&
      match_val != path_matcher::PrefixMatch::MISMATCH) {
    Descend(s, node);
  }
}



template<class VType>
void cas::Query<VType>::EvaluateLeafNode(State& s,
    const NodeReader& node) {
  cas::path_matcher::PrefixMatch match_pat = MatchPathPrefix(s);
  cas::path_matcher::PrefixMatch match_val = MatchValuePrefix(s);

  // we can immediately discard this node if one of its
  // common prefixes mismatches
  if (match_pat == path_matcher::PrefixMatch::MISMATCH ||
      match_val == path_matcher::PrefixMatch::MISMATCH) {
    return;
  }

  // check for each suffix if it matches
  node.ForEachSuffix([&](
        uint16_t len_p, const uint8_t* path,
        uint16_t len_v, const uint8_t* value,
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


template<class VType>
void cas::Query<VType>::EmitMatch(const State& s,
      const cas::ref_t& ref) {
  ++stats_.nr_matches_;
  stats_.sum_depth_ += s.depth_;
  emitter_(*buf_pat_, s.len_pat_, *buf_val_, s.len_val_, ref);
}


template<class VType>
void cas::Query<VType>::UpdateStats(const NodeReader& node) {
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


template<class VType>
void cas::Query<VType>::PrepareBuffer(State& s, const cas::NodeReader& node) {
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


template<class VType>
cas::path_matcher::PrefixMatch
cas::Query<VType>::MatchPathPrefix(State& s) {
  return cas::path_matcher::MatchPathIncremental(*buf_pat_, key_.path_,
      s.len_pat_, s.pm_state_);
}


template<class VType>
cas::path_matcher::PrefixMatch
cas::Query<VType>::MatchValuePrefix(State& s) {
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


template<class VType>
void cas::Query<VType>::Descend(const State& s, const cas::NodeReader& node) {
  switch (node.Dimension()) {
  case cas::Dimension::PATH:
    DescendPathNode(s, node);
    break;
  case cas::Dimension::VALUE:
    DescendValueNode(s, node);
    break;
  case cas::Dimension::LEAF:
    throw std::runtime_error{"we cannot descend further on a LEAF node"};
  }
}


template<class VType>
void cas::Query<VType>::DescendPathNode(const State& s, const cas::NodeReader& node) {
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


template<class VType>
void cas::Query<VType>::DescendValueNode(const State& s, const cas::NodeReader& node) {
  std::byte low  = (s.vl_pos_ == s.len_val_) ? key_.low_[s.vl_pos_]  : std::byte{0x00};
  std::byte high = (s.vh_pos_ == s.len_val_) ? key_.high_[s.vh_pos_] : std::byte{0xFF};
  DescendNode(s, node, low, high);
}


template<class VType>
void cas::Query<VType>::DescendNode(const State& s,
    const cas::NodeReader& node, std::byte low, std::byte high) {
  node.ForEachChild([&](uint8_t byte, size_t child_pos){
    if (static_cast<uint8_t>(low) <= byte && byte <= static_cast<uint8_t>(high)) {
      State copy = {
        .is_root_          = false,
        .idx_position_     = child_pos,
        .parent_dimension_ = node.Dimension(),
        .parent_byte_      = static_cast<std::byte>(byte),
        .len_pat_          = s.len_pat_,
        .len_val_          = s.len_val_,
        .pm_state_         = s.pm_state_,
        .vl_pos_           = s.vl_pos_,
        .vh_pos_           = s.vh_pos_,
        .depth_            = s.depth_ + 1,
      };
      stack_.push_back(std::move(copy));
    }
  });
}


template<class VType>
void cas::Query<VType>::State::Dump() const {
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


template<class VType>
void cas::Query<VType>::DumpState(State& s) {
  std::cout << "(Query)";
  std::cout << "\nbuf_pat_: ";
  cas::util::DumpHexValues(*buf_pat_, s.len_pat_);
  std::cout << "\nbuf_val_: ";
  cas::util::DumpHexValues(*buf_val_, s.len_val_);
  std::cout << "\nState:" << "\n";
  s.Dump();
}


template<>
bool cas::Query<cas::vint64_t>::IsCompleteValue(State& s) {
  return s.len_val_ == sizeof(cas::vint64_t);
}


// explicit instantiations to separate header from implementation
template class cas::Query<cas::vint64_t>;
