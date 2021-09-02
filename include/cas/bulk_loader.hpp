#pragma once

#include "cas/binary_key.hpp"
#include "cas/bulk_loader_stats.hpp"
#include "cas/context.hpp"
#include "cas/key.hpp"
#include "cas/memory_pool.hpp"
#include "cas/partition.hpp"
#include "cas/partition_table.hpp"
#include "cas/pager.hpp"
#include <deque>
#include <iostream>
#include <chrono>

namespace cas {

template<class VType>
class BulkLoader {
  const Context& context_;
  BulkLoaderStats& stats_;
  MemoryPools mpool_;
  Pager pager_;
  long partition_counter_ = 0;
  std::array<std::unique_ptr<std::array<std::byte, cas::PAGE_SZ>>, cas::BYTE_MAX> ref_keys_;
  std::unique_ptr<std::array<std::byte, cas::PAGE_SZ>> shortened_key_buffer_;
  std::unique_ptr<std::array<uint8_t, 10'000'000>> serialization_buffer_;

  std::chrono::time_point<std::chrono::high_resolution_clock> start_time_global;


  struct Node {
    cas::Dimension dimension_ = cas::Dimension::PATH;
    std::vector<std::byte> path_;
    std::vector<std::byte> value_;
    std::vector<std::tuple<std::byte,size_t>> children_pointers_;
    std::vector<MemoryKey> suffixes_;

    size_t ByteSize(int nr_children) const;
    void Dump() const;
    bool IsLeaf() const { return !suffixes_.empty(); };
  };

public:
  BulkLoader(const Context& context, BulkLoaderStats& stats);
  void Load();
  void Load(Partition& partition);
  BulkLoaderStats& Stats() { return stats_; }

private:
  size_t Construct(
      cas::Partition& partition,
      cas::Dimension dimension,
      cas::Dimension par_dimension,
      int depth = 0,
      size_t offset = 0);

  void PsiPartition(
      PartitionTable& table,
      Partition& partition,
      const cas::Dimension dimension);

  void ConstructLeafNode(
      Node& node,
      cas::Partition& partition);

  size_t SerializeNode(Node& node);

  void CopyToSerializationBuffer(
      size_t& offset, const void* src, size_t count);


  void UpdatePartitionStats(const Partition& partition);

  void InitializeRootPartition(Partition& partition);

  void DscByte(Partition& partition);
  void DscByteByByte(Partition& partition);
};

}
