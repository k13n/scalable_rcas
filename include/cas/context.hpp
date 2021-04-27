#pragma once

#include "cas/types.hpp"
#include <iostream>


namespace cas {

struct Context {
  std::string input_filename_ = "";
  std::string partition_folder_ = "partitions/";
  std::string index_file_ = "index.bin";
  size_t mem_size_bytes_ = 1'000'000'000;
  size_t mem_capacity_bytes_ = 0;
  size_t dataset_size_ = 0;
  bool use_lazy_interleaving_ = true;
  bool reverse_paths_ = false;
  bool use_direct_io_ = false;
  ClusteringAlgorithm clustering_algorithm_ = cas::ClusteringAlgorithm::RS;
  DscComputation dsc_computation_ = cas::DscComputation::Proactive;
  MemoryPlacement memory_placement_ = cas::MemoryPlacement::FrontLoading;
  bool compute_page_utilization_ = false;
  bool compute_fanout_ = false;
  bool compute_nodes_per_page_ = false;
  bool print_root_partition_table_allocation_ = false;
  bool use_root_dsc_bytes_ = false;
  int root_dsc_P_ = 0;
  int root_dsc_V_ = 0;

  void Dump() {
    std::cout << "Context:";
    std::cout << "\ninput_filename_: " << input_filename_;
    std::cout << "\npartition_folder_: " << partition_folder_;
    std::cout << "\nindex_file_: " << index_file_;
    std::cout << "\nmem_size_bytes_: " << mem_size_bytes_;
    std::cout << "\nmem_capacity_bytes_: " << mem_capacity_bytes_;
    std::cout << "\ndataset_size_: " << dataset_size_;
    std::cout << "\nuse_lazy_interleaving_: " << use_lazy_interleaving_;
    std::cout << "\nreverse_paths_: " << reverse_paths_;
    std::cout << "\nuse_direct_io_: " << use_direct_io_;
    std::cout << "\nclustering_algorithm_: " << ToString(clustering_algorithm_);
    std::cout << "\ndsc_computation_: " << ToString(dsc_computation_);
    std::cout << "\nmemory_placement_: " << ToString(memory_placement_);
    std::cout << "\ncompute_page_utilization_: " << compute_page_utilization_;
    std::cout << "\ncompute_fanout_: " << compute_fanout_;
    std::cout << "\ncompute_nodes_per_page_: " << compute_nodes_per_page_;
    std::cout << "\nprint_root_partition_table_allocation_: "
              << print_root_partition_table_allocation_;
    std::cout << "\nuse_root_dsc_bytes_: " << use_root_dsc_bytes_;
    std::cout << "\nroot_dsc_P_: " << root_dsc_P_;
    std::cout << "\nroot_dsc_V_: " << root_dsc_V_;
    std::cout << "\n";
  }
};

} // namespace cas
