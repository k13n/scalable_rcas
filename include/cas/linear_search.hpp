#pragma once

#include "cas/partition.hpp"
#include "cas/query.hpp"
#include "cas/search_key.hpp"


namespace cas {



template<class VType>
class LinearSearch {
  Partition& partition_;
  const BinarySK& key_;
  const cas::BinaryKeyEmitter& emitter_;
  std::unique_ptr<QueryBuffer> buf_pat_;
  std::unique_ptr<QueryBuffer> buf_val_;
  QueryStats stats_;

public:
  LinearSearch(
      Partition& partition,
      const BinarySK& key,
      const cas::BinaryKeyEmitter& emitter);

  void Execute();

  const QueryStats& Stats() const {
    return stats_;
  }

private:
  bool MatchValue(uint16_t len_val);
  bool MatchPath(uint16_t len_pat);

};


} // namespace cas
