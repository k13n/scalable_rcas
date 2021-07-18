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

template<class VType, size_t PAGE_SZ>
class BulkLoader {
  MemoryPools<PAGE_SZ> mpool_;
  Pager<PAGE_SZ>& pager_;
  const Context& context_;
  long partition_counter_ = 0;
  std::array<std::unique_ptr<std::array<std::byte, PAGE_SZ>>, cas::BYTE_MAX> ref_keys_;
  std::unique_ptr<std::array<std::byte, PAGE_SZ>> shortened_key_buffer_;
  std::unique_ptr<std::array<uint8_t, 10'000'000>> serialization_buffer_;
  BulkLoaderStats stats_;

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
  BulkLoader(Pager<PAGE_SZ>& pager, const Context& context);
  void Load();
  BulkLoaderStats& Stats() { return stats_; }

private:
  size_t Construct(
      cas::Partition<PAGE_SZ>& partition,
      cas::Dimension dimension,
      cas::Dimension par_dimension,
      int depth = 0,
      size_t offset = 0);

  void PsiPartition(
      PartitionTable<PAGE_SZ>& table,
      Partition<PAGE_SZ>& partition,
      const cas::Dimension dimension);

  void ConstructLeafNode(
      Node& node,
      cas::Partition<PAGE_SZ>& partition);

  size_t SerializeNode(Node& node);

  void CopyToSerializationBuffer(
      size_t& offset, const void* src, size_t count);


  Key<VType> ProcessLine(const std::string& line, char delimiter);
  std::string ReversePath(const std::string& path);

  void UpdatePartitionStats(const Partition<PAGE_SZ>& partition);

  void InitializeRootPartition(Partition<PAGE_SZ>& partition);

  void DscByte(Partition<PAGE_SZ>& partition);
  void DscByteByByte(Partition<PAGE_SZ>& partition);
};

template<class VType>
VType ParseValue(std::string& value);

}
