#pragma once

#include "cas/bulk_loader_stats.hpp"
#include "cas/context.hpp"
#include "cas/mem/node.hpp"
#include "cas/query.hpp"
#include "cas/search_key.hpp"
#include <functional>


namespace cas {


template<class VType>
class Index {
  const Context& context_;
  cas::BulkLoaderStats stats_;
  cas::mem::Node* root_ = nullptr;
  size_t nr_memory_keys_ = 0;

public:

  Index(const Context& context)
    : context_{context}
  { }

  void Insert(BinaryKey key);
  QueryStats Query(const SearchKey<VType>& key, const BinaryKeyEmitter emitter);
  QueryStats Query(const BinarySK& key, const BinaryKeyEmitter emitter);
  void BulkLoad();

  cas::BulkLoaderStats& Stats() {
    return stats_;
  }

  void FlushMemoryResidentKeys() {
    HandleOverflow();
  }

  void ClearPipelineFiles();

private:
  void HandleOverflow();
  void DeleteNodesRecursively(INode *node);
};


} // namespace cas
