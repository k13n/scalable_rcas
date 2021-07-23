#pragma once

#include "cas/context.hpp"
#include "cas/mem/node.hpp"
#include "cas/query.hpp"
#include "cas/search_key.hpp"
#include <functional>


namespace cas {


template<class VType, size_t PAGE_SZ>
class Index {
  const Context& context_;
  cas::mem::Node* root_ = nullptr;
  size_t nr_memory_keys_ = 0;

public:

  Index(const Context& context)
    : context_{context}
  { }

  void Insert(BinaryKey key);
  QueryStats Query(SearchKey<VType> key, const BinaryKeyEmitter emitter);
  void BulkLoad();

  void FlushMemoryResidentKeys() {
    HandleOverflow();
  }

private:
  void HandleOverflow();
  void DeleteNodesRecursively(INode *node);
};


} // namespace cas
