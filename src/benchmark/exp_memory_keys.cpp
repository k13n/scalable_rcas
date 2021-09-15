#include "benchmark/exp_memory_keys.hpp"
#include "cas/bulk_loader.hpp"
#include "cas/index.hpp"
#include "cas/util.hpp"
#include <filesystem>
#include <sstream>


template<class VType>
benchmark::ExpMemoryKeys<VType>::ExpMemoryKeys(
      const cas::Context& context,
      const std::vector<size_t>& nr_memory_keys)
  : context_(context)
  , nr_memory_keys_(nr_memory_keys)
{ }


template<class VType>
void benchmark::ExpMemoryKeys<VType>::Execute() {
  cas::util::Log("Experiment ExpMemoryKeys\n\n");
  for (auto nr_keys : nr_memory_keys_) {
    Execute(nr_keys);
  }
  PrintOutput();
}


template<class VType>
void benchmark::ExpMemoryKeys<VType>::Execute(size_t nr_memory_keys)
{
  auto context_copy = context_;
  context_copy.max_memory_keys_ = nr_memory_keys;
  /* int avg_key_size = 80; */
  /* context_copy.mem_size_bytes_ = nr_memory_keys * avg_key_size; */

  // print configuration
  std::cout << "nr_memory_keys: " << nr_memory_keys << "\n";
  context_copy.Dump();
  std::cout << "\n" << std::flush;

  cas::Index<VType> index{context_copy};
  index.ClearPipelineFiles();

  // prepare cursor to read remaining keys
  cas::Partition partition{context_copy.input_filename_, index.Stats(), context_copy};
  std::vector<std::byte> page_buffer;
  page_buffer.resize(cas::PAGE_SZ);
  cas::MemoryPage io_page{&page_buffer[0]};
  auto cursor = partition.Cursor(io_page);

  size_t file_size = context_copy.dataset_size_ > 0
    ? context_copy.dataset_size_
    : std::filesystem::file_size(context_copy.input_filename_);
  size_t nr_total_pages = file_size / cas::PAGE_SZ;

  // insert remaining keys
  auto start = std::chrono::high_resolution_clock::now();
  size_t nr_inserted_keys = 0;
  auto print_progress = [&start,&nr_inserted_keys]() -> void {
    auto now = std::chrono::high_resolution_clock::now();
    auto time = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
    std::stringstream ss;
    ss << "(nr_keys, runtime_ms): ("
      << nr_inserted_keys << ", "
      << time << ")\n";
    cas::util::Log(ss.str());
    std::cout << std::flush;
  };
  size_t i = 0;
  while (cursor.HasNext() && i < nr_total_pages) {
    for (auto key : io_page) {
      index.Insert(key);
      ++nr_inserted_keys;
      if (nr_inserted_keys % context_copy.max_memory_keys_ == 0) {
        print_progress();
      }
    }
    ++i;
  }
  index.FlushMemoryResidentKeys();
  print_progress();

  // print results
  std::cout << "\nResults:\n";
  index.Stats().Dump();
  std::cout << "\n\n\n" << std::flush;

  auto stats = index.Stats();
  auto now = std::chrono::high_resolution_clock::now();
  stats.runtime_.time_ = std::chrono::duration_cast<std::chrono::milliseconds>(now - start);
  stats.runtime_.count_ = 0;

  results_.emplace_back(nr_memory_keys, stats);
}


template<class VType>
void benchmark::ExpMemoryKeys<VType>::PrintOutput() {
  std::cout << "\n\n\n";
  cas::util::Log("Summary:\n\n");
  std::cout << "nr_memory_keys;runtime_ms;runtime_m;runtime_h;disk_overhead_b;disk_overhead_gb;disk_io_gb\n";
  for (const auto& [nr_memory_keys, stats] : results_) {
    auto runtime_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stats.runtime_.time_).count();
    auto runtime_m  = std::chrono::duration_cast<std::chrono::minutes>(stats.runtime_.time_).count();
    auto runtime_h  = std::chrono::duration_cast<std::chrono::hours>(stats.runtime_.time_).count();
    auto disk_overhead_b  = stats.IoOverhead();
    auto disk_overhead_gb = disk_overhead_b / 1'000'000'000.0;
    std::cout << nr_memory_keys << ";";
    std::cout << runtime_ms << ";";
    std::cout << runtime_m << ";";
    std::cout << runtime_h << ";";
    std::cout << disk_overhead_b << ";";
    std::cout << disk_overhead_gb << ";";
    std::cout << stats.DiskIo() / 1'000'000'000.0 << "\n";
  }
}

template class benchmark::ExpMemoryKeys<cas::vint64_t>;
