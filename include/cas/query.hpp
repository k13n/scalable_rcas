#pragma once

#include "cas/inode.hpp"
#include "cas/key_encoding.hpp"
#include "cas/key_decoder.hpp"
#include "cas/pager.hpp"
#include "cas/path_matcher.hpp"
#include "cas/query_stats.hpp"
#include "cas/search_key.hpp"

#include <deque>
#include <functional>
#include <string>
#include <set>
#include <list>


namespace cas {


using BinaryKeyEmitter = std::function<void(
    const QueryBuffer& path, size_t p_len,
    const QueryBuffer& value, size_t v_len,
    ref_t ref)>;

const cas::BinaryKeyEmitter kNullEmitter = [&](
      const cas::QueryBuffer& /* path */, size_t /* p_len */,
      const cas::QueryBuffer& /* value */, size_t /* v_len */,
      cas::ref_t /* ref */) -> void {
};

const cas::BinaryKeyEmitter kPrintEmitter = [&](
      const cas::QueryBuffer& path, size_t p_len,
      const cas::QueryBuffer& value, size_t v_len,
      cas::ref_t ref) -> void {
  KeyDecoder<cas::vint64_t>::Decode(path, p_len, value, v_len, ref).Dump();
};


class Query {
  struct State {
    bool is_root_;
    const INode* node_;
    Dimension parent_dimension_;
    std::byte parent_byte_ = cas::kNullByte;
    // length of the prefixes matched so far
    uint16_t len_pat_;
    uint16_t len_val_;
    // state needed for the path matching
    path_matcher::State pm_state_;
    // state needed for the value matching
    uint16_t vl_pos_;
    uint16_t vh_pos_;
    int depth_ = 0;

    void Dump() const;
  };

  const INode* root_;
  const BinarySK& key_;
  const BinaryKeyEmitter& emitter_;
  std::unique_ptr<QueryBuffer> buf_pat_;
  std::unique_ptr<QueryBuffer> buf_val_;
  QueryStats stats_;


public:
  Query(const INode* root,
      const BinarySK& key,
      const BinaryKeyEmitter& emitter);

  void Execute();

  const QueryStats& Stats() const {
    return stats_;
  }

private:
  void EvaluateQuery(State& s);
  void EvaluateInnerNode(State& s, const cas::INode* node);
  void EvaluateLeafNode(State& s, const cas::INode* node);
  void PrepareBuffer(State& s, const cas::INode* node);
  path_matcher::PrefixMatch MatchPathPrefix(State& s);
  path_matcher::PrefixMatch MatchValuePrefix(State& s);
  void Descend(const State& s, const cas::INode* node);
  void DescendPathNode(const State& s, const cas::INode* node);
  void DescendValueNode(const State& s, const cas::INode* node);
  void DescendNode(const State& s, const cas::INode* node,
      std::byte low, std::byte high);
  void EmitMatch(const State& s, const cas::ref_t& ref);
  void UpdateStats(const cas::INode* node);
  void DumpState(State& s);
};

} // namespace cas
