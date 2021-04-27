#include "cas/partition_table.hpp"
#include <iostream>
#include <string>


template<size_t PAGE_SZ>
cas::PartitionTable<PAGE_SZ>::PartitionTable(
      long& partition_counter,
      const cas::Context& context,
      BulkLoaderStats& stats)
  : partition_counter_(partition_counter)
  , context_(context)
  , stats_(stats)
{}


template<size_t PAGE_SZ>
void cas::PartitionTable<PAGE_SZ>::InitializePartition(int position, int dsc_p, int dsc_v) {
  if (Absent(position)) {
    std::string filename = context_.partition_folder_ + std::to_string(partition_counter_++);
    table_[position] = std::make_unique<cas::Partition<PAGE_SZ>>(
        filename, stats_, context_);
    table_[position]->DscP(dsc_p);
    table_[position]->DscV(dsc_v);
  }
}


template<size_t PAGE_SZ>
cas::Partition<PAGE_SZ>& cas::PartitionTable<PAGE_SZ>::operator[](int position) {
  if (Absent(position)) {
    std::string error_msg = "partition at position " + std::to_string(position)
      + " does not exist";
    throw std::runtime_error{error_msg};
  }
  return *table_[position];
}


template<size_t PAGE_SZ>
int cas::PartitionTable<PAGE_SZ>::NrPartitions() {
  int nr_partitions = 0;
  for (int i = 0x00; i <= 0xFF; ++i) {
    if (Exists(i)) {
      ++nr_partitions;
    }
  }
  return nr_partitions;
}


template<size_t PAGE_SZ>
void cas::PartitionTable<PAGE_SZ>::Dump() {
  std::cout << "PartitionTable:\n";
  for (int i = 0x00; i <= 0xFF; ++i) {
    if (Exists(i)) {
      printf("Partition 0x%02X:\n", static_cast<unsigned char>(i)); // NOLINT
      table_[i]->Dump();
    }
  }
}


template<size_t PAGE_SZ>
void cas::PartitionTable<PAGE_SZ>::PrintMemoryAllocation() {
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


template class cas::PartitionTable<cas::PAGE_SZ_64KB>;
template class cas::PartitionTable<cas::PAGE_SZ_32KB>;
template class cas::PartitionTable<cas::PAGE_SZ_16KB>;
template class cas::PartitionTable<cas::PAGE_SZ_8KB>;
template class cas::PartitionTable<cas::PAGE_SZ_4KB>;
