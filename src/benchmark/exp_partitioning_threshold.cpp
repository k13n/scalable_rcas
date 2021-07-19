#include "benchmark/exp_partitioning_threshold.hpp"
#include "cas/bulk_loader.hpp"
#include "cas/util.hpp"


template<class VType, size_t PAGE_SZ>
benchmark::ExpPartitioningThreshold<VType, PAGE_SZ>::ExpPartitioningThreshold(
      const cas::Context& context,
      const std::vector<size_t>& thresholds,
      const std::vector<size_t>& dataset_sizes)
  : context_(context)
  , thresholds_(thresholds)
  , dataset_sizes_(dataset_sizes)
{
}


template<class VType, size_t PAGE_SZ>
void benchmark::ExpPartitioningThreshold<VType, PAGE_SZ>::Execute() {
  cas::util::Log("Experiment ExpPartitioningThreshold\n\n");
  for (const auto& threshold : thresholds_) {
    for (const auto& dataset_size : dataset_sizes_) {
      Execute(threshold, dataset_size);
    }
  }
  PrintOutput();
}


template<class VType, size_t PAGE_SZ>
void benchmark::ExpPartitioningThreshold<VType, PAGE_SZ>::Execute(
    size_t threshold,
    size_t dataset_size)
{
  // copy the context;
  auto context = context_;
  context.partitioning_threshold_ = threshold;
  context.dataset_size_ = dataset_size;

  // print input
  cas::util::Log("Configuration\n");
  context.Dump();
  std::cout << "\n" << std::flush;

  // run benchmark
  cas::Pager<PAGE_SZ> pager{context.index_file_};
  cas::BulkLoader<VType, PAGE_SZ> bulk_loader{pager, context};
  bulk_loader.Load();

  // print output
  cas::util::Log("Output:\n\n");
  auto stats = bulk_loader.Stats();
  stats.Dump();
  std::cout << "\n\n\n";

  // save input and output
  results_.emplace_back(context, stats);
}


template<class VType, size_t PAGE_SZ>
void benchmark::ExpPartitioningThreshold<VType, PAGE_SZ>::PrintOutput() {
  std::cout << "\n\n\n";
  cas::util::Log("Summary:\n\n");
  std::cout << "threshold;dataset_size_b;nr_input_keys;runtime_ms;runtime_m;runtime_h;disk_overhead_b;disk_overhead_gb;disk_io_gb;partitions_created;index_bytes_written_b;index_bytes_written_gb\n";
  for (const auto& [context, stats] : results_) {
    auto runtime_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stats.runtime_.time_).count();
    auto runtime_m  = std::chrono::duration_cast<std::chrono::minutes>(stats.runtime_.time_).count();
    auto runtime_h  = std::chrono::duration_cast<std::chrono::hours>(stats.runtime_.time_).count();
    auto disk_overhead_b  = stats.IoOverhead();
    auto disk_overhead_gb = disk_overhead_b / 1'000'000'000.0;
    std::cout << context.partitioning_threshold_ << ";";
    std::cout << context.dataset_size_ << ";";
    std::cout << stats.nr_input_keys_ << ";";
    std::cout << runtime_ms << ";";
    std::cout << runtime_m << ";";
    std::cout << runtime_h << ";";
    std::cout << disk_overhead_b << ";";
    std::cout << disk_overhead_gb << ";";
    std::cout << stats.DiskIo() / 1'000'000'000.0 << ";";
    std::cout << stats.partitions_created_ << ";";
    std::cout << stats.index_bytes_written_ << ";";
    std::cout << stats.index_bytes_written_ / 1'000'000'000.0 << "\n";
  }
}


template class benchmark::ExpPartitioningThreshold<cas::vint64_t, cas::PAGE_SZ_64KB>;
template class benchmark::ExpPartitioningThreshold<cas::vint64_t, cas::PAGE_SZ_32KB>;
template class benchmark::ExpPartitioningThreshold<cas::vint64_t, cas::PAGE_SZ_16KB>;
template class benchmark::ExpPartitioningThreshold<cas::vint64_t, cas::PAGE_SZ_8KB>;
template class benchmark::ExpPartitioningThreshold<cas::vint64_t, cas::PAGE_SZ_4KB>;
