#include "cas/query.hpp"
#include "cas/key_decoder.hpp"
#include "cas/util.hpp"
#include <cassert>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <list>


cas::Query::Query(
        const INode* root,
        const cas::BinarySK& key,
        const cas::BinaryKeyEmitter emitter)
    : root_(root)
    , key_(key)
    , emitter_(emitter)
    , buf_pat_(std::make_unique<QueryBuffer>())
    , buf_val_(std::make_unique<QueryBuffer>())
{}


void cas::Query::Execute() {
  if (root_ == nullptr) {
    return;
  }
  const auto& t_start = std::chrono::high_resolution_clock::now();
  State initial_state = {
    .is_root_ = true,
    .node_ = root_,
    .parent_dimension_ = cas::Dimension::PATH, // doesn't matter
    .parent_byte_ = cas::kNullByte,
    .len_pat_ = 0,
    .len_val_ = 0,
    .pm_state_ = {},
    .vl_pos_ = 0,
    .vh_pos_ = 0,
  };
  EvaluateQuery(initial_state);
  const auto& t_end = std::chrono::high_resolution_clock::now();
  stats_.runtime_mus_ =
    std::chrono::duration_cast<std::chrono::microseconds>(t_end-t_start).count();
}


void cas::Query::EvaluateQuery(State& s) {
  UpdateStats(s.node_);
  PrepareBuffer(s, s.node_);
  if (s.node_->IsInnerNode()) {
    EvaluateInnerNode(s, s.node_);
  } else {
    EvaluateLeafNode(s, s.node_);
  }
}


void cas::Query::EvaluateInnerNode(State& s, const INode* node) {
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


void cas::Query::EvaluateLeafNode(State& s,
    const INode* node) {
  cas::path_matcher::PrefixMatch match_pat = MatchPathPrefix(s);
  cas::path_matcher::PrefixMatch match_val = MatchValuePrefix(s);

  // we can immediately discard this node if one of its
  // common prefixes mismatches
  if (match_pat == path_matcher::PrefixMatch::MISMATCH ||
      match_val == path_matcher::PrefixMatch::MISMATCH) {
    return;
  }

  // check for each suffix if it matches
  node->ForEachSuffix([&](
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


void cas::Query::EmitMatch(const State& s,
      const cas::ref_t& ref) {
  ++stats_.nr_matches_;
  stats_.sum_depth_ += s.depth_;
  emitter_(*buf_pat_, s.len_pat_, *buf_val_, s.len_val_, ref);
}


void cas::Query::UpdateStats(const INode* node) {
  ++stats_.read_nodes_;
  switch (node->Dimension()) {
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


void cas::Query::PrepareBuffer(State& s, const cas::INode* node) {
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
  std::memcpy(&buf_pat_->at(s.len_pat_), node->Path(),  node->LenPath());
  std::memcpy(&buf_val_->at(s.len_val_), node->Value(), node->LenValue());
  s.len_pat_ += node->LenPath();
  s.len_val_ += node->LenValue();
}


cas::path_matcher::PrefixMatch
cas::Query::MatchPathPrefix(State& s) {
  return cas::path_matcher::MatchPathIncremental(*buf_pat_, key_.path_,
      s.len_pat_, s.pm_state_);
}


cas::path_matcher::PrefixMatch
cas::Query::MatchValuePrefix(State& s) {
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

  // TODO: here we assume the values are 64 bit numbers
  bool is_complete_value = s.len_val_ == sizeof(cas::vint64_t);

  return is_complete_value ? path_matcher::PrefixMatch::MATCH
                           : path_matcher::PrefixMatch::INCOMPLETE;
}


void cas::Query::Descend(const State& s, const cas::INode* node) {
  switch (node->Dimension()) {
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


void cas::Query::DescendPathNode(const State& s, const cas::INode* node) {
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


void cas::Query::DescendValueNode(const State& s, const cas::INode* node) {
  std::byte low  = (s.vl_pos_ == s.len_val_) ? key_.low_[s.vl_pos_]  : std::byte{0x00};
  std::byte high = (s.vh_pos_ == s.len_val_) ? key_.high_[s.vh_pos_] : std::byte{0xFF};
  DescendNode(s, node, low, high);
}


void cas::Query::DescendNode(const State& s,
    const cas::INode* node, std::byte low, std::byte high) {
  node->ForEachChild([&](uint8_t byte, cas::INode* child){
    if (static_cast<uint8_t>(low) <= byte && byte <= static_cast<uint8_t>(high)) {
      State copy = {
        .is_root_          = false,
        .node_             = child,
        .parent_dimension_ = node->Dimension(),
        .parent_byte_      = static_cast<std::byte>(byte),
        .len_pat_          = s.len_pat_,
        .len_val_          = s.len_val_,
        .pm_state_         = s.pm_state_,
        .vl_pos_           = s.vl_pos_,
        .vh_pos_           = s.vh_pos_,
        .depth_            = s.depth_ + 1,
      };
      EvaluateQuery(copy);
    }
  });
}


void cas::Query::State::Dump() const {
  std::cout << "(QueryState)";
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


void cas::Query::DumpState(State& s) {
  std::cout << "(Query)";
  std::cout << "\nbuf_pat_: ";
  cas::util::DumpHexValues(*buf_pat_, s.len_pat_);
  std::cout << "\nbuf_val_: ";
  cas::util::DumpHexValues(*buf_val_, s.len_val_);
  std::cout << "\nState:" << "\n";
  s.Dump();
}
