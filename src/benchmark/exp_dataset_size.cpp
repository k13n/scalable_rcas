#include "benchmark/exp_dataset_size.hpp"
#include "cas/bulk_loader.hpp"
#include "cas/util.hpp"


template<class VType, size_t PAGE_SZ>
benchmark::ExpDatasetSize<VType, PAGE_SZ>::ExpDatasetSize(
      const cas::Context& context,
      const std::vector<size_t>& dataset_sizes)
  : context_(context)
  , dataset_sizes_(dataset_sizes)
{
}


template<class VType, size_t PAGE_SZ>
void benchmark::ExpDatasetSize<VType, PAGE_SZ>::Execute() {
  cas::util::Log("Experiment ExpDatasetSize\n\n");
  for (const auto& dataset_size : dataset_sizes_) {
    Execute(dataset_size);
  }
  PrintOutput();
}


template<class VType, size_t PAGE_SZ>
void benchmark::ExpDatasetSize<VType, PAGE_SZ>::Execute(size_t dataset_size)
{
  // copy the context;
  auto context = context_;
  context.dataset_size_ = dataset_size;

  // print input
  cas::util::Log("Configuration\n");
  context.Dump();
  std::cout << "\n" << std::flush;

  // run benchmark
  cas::BulkLoaderStats stats;
  cas::BulkLoader<VType, PAGE_SZ> bulk_loader{context, stats};
  bulk_loader.Load();

  // print output
  cas::util::Log("Output:\n\n");
  stats.Dump();
  std::cout << "\n\n\n";

  results_.push_back(stats);
}


template<class VType, size_t PAGE_SZ>
void benchmark::ExpDatasetSize<VType, PAGE_SZ>::PrintOutput() {
  std::cout << "\n\n\n";
  cas::util::Log("Summary:\n\n");
  std::cout << "dataset_size_b;nr_input_keys;runtime_ms;runtime_m;runtime_h;disk_overhead_b;disk_overhead_gb;disk_io_gb\n";
  int count = 0;
  for (const auto& dataset_size : dataset_sizes_) {
    const auto& stats = results_[count++];
    auto runtime_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stats.runtime_.time_).count();
    auto runtime_m  = std::chrono::duration_cast<std::chrono::minutes>(stats.runtime_.time_).count();
    auto runtime_h  = std::chrono::duration_cast<std::chrono::hours>(stats.runtime_.time_).count();
    auto disk_overhead_b  = stats.IoOverhead();
    auto disk_overhead_gb = disk_overhead_b / 1'000'000'000.0;
    std::cout << dataset_size << ";";
    std::cout << stats.nr_input_keys_ << ";";
    std::cout << runtime_ms << ";";
    std::cout << runtime_m << ";";
    std::cout << runtime_h << ";";
    std::cout << disk_overhead_b << ";";
    std::cout << disk_overhead_gb << ";";
    std::cout << stats.DiskIo() / 1'000'000'000.0 << "\n";
  }
}

template class benchmark::ExpDatasetSize<cas::vint64_t, cas::PAGE_SZ_64KB>;
template class benchmark::ExpDatasetSize<cas::vint64_t, cas::PAGE_SZ_32KB>;
template class benchmark::ExpDatasetSize<cas::vint64_t, cas::PAGE_SZ_16KB>;
template class benchmark::ExpDatasetSize<cas::vint64_t, cas::PAGE_SZ_8KB>;
template class benchmark::ExpDatasetSize<cas::vint64_t, cas::PAGE_SZ_4KB>;
