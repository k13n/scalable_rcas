#pragma once

#include "cas/pager.hpp"
#include "cas/key_encoding.hpp"
#include "cas/record.hpp"
#include "cas/path_matcher.hpp"
#include "cas/query_stats.hpp"
#include "cas/search_key.hpp"
#include "cas/page_buffer.hpp"

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


using ChildCallback = std::function<void(
    std::byte byte, cas::page_nr_t page_nr, uint16_t offset)>;



template<class VType, size_t PAGE_SZ>
class Query {
  struct State {
    bool is_root_;
    cas::page_nr_t page_nr_;
    uint16_t page_offset_;
    std::shared_ptr<IdxPage<PAGE_SZ>> page_;
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
  Pager<PAGE_SZ>& pager_;
  const PageBuffer<PAGE_SZ>& page_buffer_;
  const BinarySK& key_;
  const BinaryKeyEmitter& emitter_;
  std::unique_ptr<QueryBuffer> buf_pat_;
  std::unique_ptr<QueryBuffer> buf_val_;
  std::deque<State> stack_;
  QueryStats stats_;


public:
  Query(Pager<PAGE_SZ>& pager,
      const PageBuffer<PAGE_SZ>& page_buffer,
      const BinarySK& key,
      const BinaryKeyEmitter& emitter);

  std::list<Key<VType>> Execute();

  const QueryStats& Stats() const {
    return stats_;
  }

private:
  void EvaluateInnerNode(State& s, const cas::Record& record);
  void EvaluateLeafNode(State& s, const cas::Record& record);
  void PrepareBuffer(State& s, const cas::Record& record);
  void PrepareLeafBuffer(State& s, const cas::MemoryKey& leaf_key);
  path_matcher::PrefixMatch MatchPathPrefix(State& s);
  path_matcher::PrefixMatch MatchValuePrefix(State& s);
  void Descend(const State& s, const cas::Record& record);
  void DescendPathNode(const State& s, const cas::Record& record);
  void DescendValueNode(const State& s, const cas::Record& record);
  void DescendNode(const State& s, const cas::Record& record,
      std::byte low, std::byte high);
  bool IsCompleteValue(State& s);
  void EmitMatches(State& s, const cas::Record& record);
  void EmitMatch(const State& s, const cas::MemoryKey& leaf_key);
  void UpdateStats(const cas::Record& record);
  void DumpState(State& s);
  void ForEachChild(const cas::Record& record, std::byte low, std::byte high,
      const ChildCallback& callback);
  std::pair<Record,bool> FetchRecord(const State& s);
  std::shared_ptr<IdxPage<PAGE_SZ>> FetchPage(cas::page_nr_t page_nr);

  static void CopyFromPage(
      const cas::IdxPage<PAGE_SZ>& page,
      uint16_t& offset, void* dst, size_t count);
};

} // namespace cas
