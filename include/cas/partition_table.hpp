#pragma once

#include "cas/context.hpp"
#include "cas/partition.hpp"
#include "cas/types.hpp"
#include <array>

namespace cas {

class PartitionTable {
  typename std::array<std::unique_ptr<Partition>, BYTE_MAX> table_;
  long& partition_counter_;
  const cas::Context& context_;
  BulkLoaderStats& stats_;

public:
  explicit PartitionTable(
      long& partition_counter,
      const cas::Context& context,
      BulkLoaderStats& stats);
  ~PartitionTable() = default;

  /* delete copy/move constructors/assignments */
  PartitionTable(const PartitionTable& other) = delete;
  PartitionTable(const PartitionTable&& other) = delete;
  PartitionTable& operator=(const PartitionTable& other) = delete;
  PartitionTable& operator=(PartitionTable&& other) = delete;

  Partition& operator[](int position);
  void InitializePartition(int position, int dsc_p, int dsc_v);
  /* void Insert(int position, BinaryKey&& key); */
  /* void InsertUnoptimized(int position, BinaryKey&& key); */

  inline bool Exists(int position) { return table_[position] != nullptr; }
  inline bool Absent(int position) { return table_[position] == nullptr; }
  int NrPartitions();

  void Dump();
  void PrintMemoryAllocation();
};

} // namespace cas
