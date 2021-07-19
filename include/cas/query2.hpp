#pragma once

#include "cas/query.hpp"
#include "cas/key_encoding.hpp"
#include "cas/node_reader.hpp"
#include "cas/path_matcher.hpp"
#include "cas/query_stats.hpp"
#include "cas/search_key.hpp"

#include <deque>
#include <functional>
#include <string>
#include <set>
#include <list>


namespace cas {


/* using BinaryKeyEmitter = std::function<void( */
/*     const QueryBuffer& path, size_t p_len, */
/*     const QueryBuffer& value, size_t v_len, */
/*     ref_t ref)>; */

/* const cas::BinaryKeyEmitter kNullEmitter = [&]( */
/*       const cas::QueryBuffer& /1* path *1/, size_t /1* p_len *1/, */
/*       const cas::QueryBuffer& /1* value *1/, size_t /1* v_len *1/, */
/*       cas::ref_t /1* ref *1/) -> void { */
/* }; */


template<class VType, size_t PAGE_SZ>
class Query2 {
  struct State {
    bool is_root_;
    size_t idx_position_ = 0;
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
    int depth_=0;

    void Dump() const;
  };

  /* cas::LRUCache lru; */
  const uint8_t* file_;
  const BinarySK& key_;
  const BinaryKeyEmitter& emitter_;
  std::unique_ptr<QueryBuffer> buf_pat_;
  std::unique_ptr<QueryBuffer> buf_val_;
  std::deque<State> stack_;
  QueryStats stats_;


public:
  Query2(const uint8_t* file,
      const BinarySK& key,
      const BinaryKeyEmitter& emitter);

  std::list<Key<VType>> Execute();

  const QueryStats& Stats() const {
    return stats_;
  }

private:
  void EvaluateInnerNode(State& s, const cas::NodeReader& node);
  void EvaluateLeafNode(State& s, const cas::NodeReader& node);
  void PrepareBuffer(State& s, const cas::NodeReader& node);
  path_matcher::PrefixMatch MatchPathPrefix(State& s);
  path_matcher::PrefixMatch MatchValuePrefix(State& s);
  void Descend(const State& s, const cas::NodeReader& node);
  void DescendPathNode(const State& s, const cas::NodeReader& node);
  void DescendValueNode(const State& s, const cas::NodeReader& node);
  void DescendNode(const State& s, const cas::NodeReader& node,
      std::byte low, std::byte high);
  bool IsCompleteValue(State& s);
  void EmitMatches(State& s, const cas::NodeReader& node);
  void EmitMatch(const State& s, const cas::ref_t& ref);
  void UpdateStats(const cas::NodeReader& node);
  void DumpState(State& s);

  static void CopyFromPage(
      const cas::IdxPage<PAGE_SZ>& page,
      uint16_t& offset, void* dst, size_t count);
};

} // namespace cas
