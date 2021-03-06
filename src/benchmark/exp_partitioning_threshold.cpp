#include "benchmark/exp_partitioning_threshold.hpp"
#include "benchmark/exp_querying.hpp"
#include "cas/bulk_loader.hpp"
#include "cas/query_executor.hpp"
#include "cas/key_encoder.hpp"
#include "cas/util.hpp"
#include <filesystem>


template<class VType>
benchmark::ExpPartitioningThreshold<VType>::ExpPartitioningThreshold(
      const cas::Context& context,
      const std::vector<size_t>& thresholds,
      const std::vector<size_t>& dataset_sizes,
      const std::vector<cas::SearchKey<VType>>& queries)
  : context_(context)
  , thresholds_(thresholds)
  , dataset_sizes_(dataset_sizes)
  , queries_(queries)
{
}


template<class VType>
void benchmark::ExpPartitioningThreshold<VType>::Execute() {
  cas::util::Log("Experiment ExpPartitioningThreshold\n\n");
  for (const auto& threshold : thresholds_) {
    for (const auto& dataset_size : dataset_sizes_) {
      Execute(threshold, dataset_size);
    }
  }
  PrintOutput();
}


template<class VType>
void benchmark::ExpPartitioningThreshold<VType>::Execute(
    size_t threshold,
    size_t dataset_size)
{
  std::string pipeline_dir = context_.pipeline_dir_
    + "/threshold" + std::to_string(threshold);
  std::filesystem::create_directories(pipeline_dir);

  // copy the context;
  auto context = context_;
  context.partitioning_threshold_ = threshold;
  context.dataset_size_ = dataset_size;
  context.pipeline_dir_ = pipeline_dir;
  context.index_file_ = pipeline_dir + "/index.bin";

  // print input
  cas::util::Log("Configuration\n");
  context.Dump();
  std::cout << "\n" << std::flush;

  // contains statistics
  Stats stats;

  // bulk-load index
  cas::BulkLoader<VType> bulk_loader{context, stats.bulk_stats_};
  bulk_loader.Load();

  // print bulk-loading stats
  cas::util::Log("Output:\n\n");
  stats.bulk_stats_.Dump();
  std::cout << "\n\n";

  // run query experiment
  bool clear_page_cache = true;
  bool do_warmup = false;
  int nr_repetitions = 10;
  benchmark::ExpQuerying<VType> query_experiment{
    pipeline_dir,
    queries_,
    clear_page_cache,
    do_warmup,
    nr_repetitions
  };
  query_experiment.Execute();
  stats.query_stats_ = cas::QueryStats::Sum(query_experiment.Results());

  // save input and output
  results_.emplace_back(context, stats);
}


template<class VType>
void benchmark::ExpPartitioningThreshold<VType>::PrintOutput() {
  std::cout << "\n\n\n";
  cas::util::Log("Summary:\n\n");
  std::cout << "threshold;dataset_size_b;nr_input_keys;runtime_ms;runtime_m;runtime_h;disk_overhead_b;disk_overhead_gb;disk_io_gb;partitions_created;index_bytes_written_b;index_bytes_written_gb;nr_matches;read_nodes;query_mus;query_ms\n";
  for (const auto& [context, stats] : results_) {
    const auto& bstats = stats.bulk_stats_;
    const auto& qstats = stats.query_stats_;
    auto runtime_ms = std::chrono::duration_cast<std::chrono::milliseconds>(bstats.runtime_.time_).count();
    auto runtime_m  = std::chrono::duration_cast<std::chrono::minutes>(bstats.runtime_.time_).count();
    auto runtime_h  = std::chrono::duration_cast<std::chrono::hours>(bstats.runtime_.time_).count();
    auto disk_overhead_b  = bstats.IoOverhead();
    auto disk_overhead_gb = disk_overhead_b / 1'000'000'000.0;
    std::cout << context.partitioning_threshold_ << ";";
    std::cout << context.dataset_size_ << ";";
    std::cout << bstats.nr_input_keys_ << ";";
    std::cout << runtime_ms << ";";
    std::cout << runtime_m << ";";
    std::cout << runtime_h << ";";
    std::cout << disk_overhead_b << ";";
    std::cout << disk_overhead_gb << ";";
    std::cout << bstats.DiskIo() / 1'000'000'000.0 << ";";
    std::cout << bstats.partitions_created_ << ";";
    std::cout << bstats.index_bytes_written_ << ";";
    std::cout << bstats.index_bytes_written_ / 1'000'000'000.0 << ";";
    std::cout << qstats.nr_matches_ << ";";
    std::cout << qstats.read_nodes_ << ";";
    std::cout << qstats.runtime_mus_ << ";";
    std::cout << qstats.runtime_mus_ / 1000 << "\n";
  }
}


template class benchmark::ExpPartitioningThreshold<cas::vint64_t>;
