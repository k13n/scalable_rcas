#pragma once

#include "cas/query.hpp"

namespace cas {

template<class VType>
class QueryExecutor {
  const std::string idx_filename_;

public:
  QueryExecutor(const std::string& idx_filename);
  QueryStats Execute(const BinarySK& key, const BinaryKeyEmitter& emitter);
};

} // namespace cas
