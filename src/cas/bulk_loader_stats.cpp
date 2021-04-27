#include "cas/bulk_loader_stats.hpp"
#include <iostream>

void cas::BulkLoaderStats::Dump() const {
  std::cout << "BulkLoaderStats:";
  std::cout << "\nnr_input_keys_: " << nr_input_keys_;
  std::cout << "\npartitions_created_: " << partitions_created_;
  std::cout << "\npartitions_memory_only_: " << partitions_memory_only_;
  std::cout << "\npartitions_hybrid_: " << partitions_hybrid_;
  std::cout << "\npartitions_disk_only_: " << partitions_disk_only_;
  std::cout << "\nfiles_created_: " << files_created_;
  std::cout << "\nmem_pages_read_: " << mem_pages_read_;
  std::cout << "\nmem_pages_written_: " << mem_pages_written_;
  PrintByteSize("root_partition_bytes_read_", root_partition_bytes_read_);
  PrintByteSize("root_partition_bytes_written_", root_partition_bytes_written_);
  PrintByteSize("partition_bytes_read_", partition_bytes_read_);
  PrintByteSize("partition_bytes_written_", partition_bytes_written_);
  PrintByteSize("index_bytes_written_", index_bytes_written_);
  PrintByteSize("disk_io_", DiskIo());
  PrintByteSize("io_overhead_", IoOverhead());
  PrintRuntime("runtime_", runtime_);
  PrintRuntime("  runtime_root_partition_", runtime_root_partition_);
  PrintRuntime("  runtime_construction_", runtime_construction_);
  PrintRuntime("    runtime_partitioning_", runtime_partitioning_);
  PrintRuntime("      runtime_partitioning_mem_only_", runtime_partitioning_mem_only_);
  PrintRuntime("      runtime_partitioning_hybrid_", runtime_partitioning_hybrid_);
  PrintRuntime("      runtime_partitioning_disk_only_", runtime_partitioning_disk_only_);
  PrintRuntime("    runtime_clustering_", runtime_clustering_);
  PrintRuntime("      runtime_clustering_disk_write_", runtime_clustering_disk_write_);
  PrintRuntime("    runtime_construct_leaf_node_", runtime_construct_leaf_node_);
  PrintRuntime("runtime_partition_disk_read_", runtime_partition_disk_read_);
  PrintRuntime("runtime_partition_disk_write_", runtime_partition_disk_write_);
  PrintRuntime("runtime_dsc_computation_", runtime_dsc_computation_);
  std::cout << "\n";
}


size_t cas::BulkLoaderStats::IoOverhead() const {
  return + partition_bytes_read_
    + partition_bytes_written_;
}


size_t cas::BulkLoaderStats::DiskIo() const {
  return root_partition_bytes_read_
    + root_partition_bytes_written_
    + partition_bytes_read_
    + partition_bytes_written_
    + index_bytes_written_;
}


void cas::BulkLoaderStats::PrintRuntime(
      const std::string& message,
      const cas::Timer& timer) const {
  std::cout << "\n"
    << message << ": ("
    << std::chrono::duration_cast<std::chrono::milliseconds>(timer.time_).count()
    << " ms;  "
    << std::chrono::duration_cast<std::chrono::seconds>(timer.time_).count()
    << " s;  "
    << std::chrono::duration_cast<std::chrono::minutes>(timer.time_).count()
    << " m;  "
    << timer.count_
    << ")";
}


void cas::BulkLoaderStats::PrintByteSize(
      const std::string& message,
      const size_t size_bytes) const {
  size_t size_mb = size_bytes / 1'000'000;
  size_t size_gb = size_bytes / 1'000'000'000;
  size_t size_tb = size_bytes / 1'000'000'000'000;
  std::cout << "\n"
    << message << ": ("
    << size_bytes << " B; "
    << size_mb << " MB; "
    << size_gb << " GB; "
    << size_tb << " TB"
    << ")";
}
