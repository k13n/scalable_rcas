#pragma once

#include "cas/histogram.hpp"
#include <chrono>
#include <string>


namespace cas {

struct Timer {
  size_t count_{0};
  std::chrono::microseconds time_{0};
};


struct BulkLoaderStats {
  size_t nr_bulkloads_{0};
  size_t nr_input_keys_{0};
  size_t partitions_created_{0};
  size_t partitions_memory_only_{0};
  size_t partitions_hybrid_{0};
  size_t partitions_disk_only_{0};
  size_t files_created_{0};
  size_t root_partition_bytes_read_{0};
  size_t root_partition_bytes_written_{0};
  size_t partition_bytes_read_{0};
  size_t partition_bytes_written_{0};
  size_t index_bytes_written_{0};
  size_t mem_pages_read_{0};
  size_t mem_pages_written_{0};
  Timer runtime_;
  Timer runtime_root_partition_;
  Timer runtime_construction_;
  Timer runtime_partitioning_;
  Timer runtime_partitioning_mem_only_;
  Timer runtime_partitioning_hybrid_;
  Timer runtime_partitioning_disk_only_;
  Timer runtime_partition_disk_read_;
  Timer runtime_partition_disk_write_;
  Timer runtime_construct_leaf_node_;
  Timer runtime_dsc_computation_;
  Timer runtime_insertion_;
  Timer runtime_collect_keys_;
  Histogram node_fanout_;
  Histogram page_fanout_ptrs_;

  size_t IoOverhead() const;
  size_t DiskIo() const;

  void Dump() const;

private:
  void PrintRuntime(const std::string& message,
      const Timer& timer) const;

  void PrintByteSize(const std::string& message,
      const size_t size_bytes) const;
};

} // namespace cas
