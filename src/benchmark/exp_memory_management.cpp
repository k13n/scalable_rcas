#include "benchmark/exp_memory_management.hpp"
#include "cas/bulk_loader.hpp"
#include "cas/util.hpp"


template<class VType>
benchmark::ExpMemoryManagement<VType>::ExpMemoryManagement(
      const cas::Context& context,
      const std::vector<cas::MemoryPlacement>& approaches,
      const std::vector<size_t>& memory_sizes)
  : context_(context)
  , approaches_(approaches)
  , memory_sizes_(memory_sizes)
{
}


template<class VType>
void benchmark::ExpMemoryManagement<VType>::Execute() {
  cas::util::Log("Experiment ExpMemoryManagement\n\n");
  for (const auto& memory_size : memory_sizes_) {
    for (const auto& approach : approaches_) {
      ExecuteMethod(approach, memory_size);
    }
  }
  PrintOutput();
}


template<class VType>
void benchmark::ExpMemoryManagement<VType>::ExecuteMethod(
      cas::MemoryPlacement method, size_t memory_size)
{
  // copy the context;
  auto context = context_;
  context.memory_placement_ = method;
  context.mem_size_bytes_ = memory_size;

  // print input
  cas::util::Log("Configuration\n");
  context.Dump();
  std::cout << "\n" << std::flush;

  // run benchmark
  cas::BulkLoaderStats stats;
  cas::BulkLoader<VType> bulk_loader{context, stats};
  bulk_loader.Load();

  // print output
  cas::util::Log("Output:\n\n");
  stats.Dump();
  std::cout << "\n\n\n" << std::flush;

  results_.push_back(stats);
}


template<class VType>
void benchmark::ExpMemoryManagement<VType>::PrintOutput() {
  std::cout << "\n\n\n";
  cas::util::Log("Summary:\n\n");
  std::cout << "approach;memory_size_;runtime_ms;runtime_h;disk_overhead_b;disk_overhead_gb;disk_io_b;disk_io_gb\n";
  int count = 0;
  for (const auto& memory_size : memory_sizes_) {
    for (const auto& approach : approaches_) {
      const auto& stats = results_[count++];
      auto runtime_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stats.runtime_.time_).count();
      auto runtime_h  = runtime_ms / (1000.0 * 60.0 * 60.0);
      auto disk_overhead_b  = stats.IoOverhead();
      auto disk_overhead_gb = disk_overhead_b / 1'000'000'000.0;
      auto disk_io_gb = stats.DiskIo() / 1'000'000'000.0;
      std::cout << cas::ToString(approach) << ";";
      std::cout << memory_size << ";";
      std::cout << runtime_ms << ";";
      std::cout << runtime_h << ";";
      std::cout << disk_overhead_b << ";";
      std::cout << disk_overhead_gb << ";";
      std::cout << stats.DiskIo() << ";";
      std::cout << disk_io_gb << "\n";
    }
  }
  std::cout << "\n\n\n";
}

template class benchmark::ExpMemoryManagement<cas::vint64_t>;
