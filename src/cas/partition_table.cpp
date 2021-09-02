#include "cas/partition_table.hpp"
#include <iostream>
#include <string>


cas::PartitionTable::PartitionTable(
      long& partition_counter,
      const cas::Context& context,
      BulkLoaderStats& stats)
  : partition_counter_(partition_counter)
  , context_(context)
  , stats_(stats)
{}


void cas::PartitionTable::InitializePartition(int position, int dsc_p, int dsc_v) {
  if (Absent(position)) {
    std::string filename = context_.partition_folder_ + std::to_string(partition_counter_++);
    table_[position] = std::make_unique<cas::Partition>(
        filename, stats_, context_);
    table_[position]->DscP(dsc_p);
    table_[position]->DscV(dsc_v);
  }
}


cas::Partition& cas::PartitionTable::operator[](int position) {
  if (Absent(position)) {
    std::string error_msg = "partition at position " + std::to_string(position)
      + " does not exist";
    throw std::runtime_error{error_msg};
  }
  return *table_[position];
}


int cas::PartitionTable::NrPartitions() {
  int nr_partitions = 0;
  for (int i = 0x00; i <= 0xFF; ++i) {
    if (Exists(i)) {
      ++nr_partitions;
    }
  }
  return nr_partitions;
}


void cas::PartitionTable::Dump() {
  std::cout << "PartitionTable:\n";
  for (int i = 0x00; i <= 0xFF; ++i) {
    if (Exists(i)) {
      printf("Partition 0x%02X:\n", static_cast<unsigned char>(i)); // NOLINT
      table_[i]->Dump();
    }
  }
}


void cas::PartitionTable::PrintMemoryAllocation() {
  std::cout << "PartitionTable:\n";
  for (int i = 0x00; i <= 0xFF; ++i) {
    if (Exists(i)) {
      printf("Partition 0x%02X: (", static_cast<unsigned char>(i)); // NOLINT
      std::cout << table_[i]->NrMemoryPages() << ", ";
      std::cout << table_[i]->NrDiskPages() << ")\n";
    }
  }
  std::cout << "\n";
}
