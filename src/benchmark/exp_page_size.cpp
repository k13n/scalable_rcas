#include "benchmark/exp_page_size.hpp"
#include "cas/bulk_loader.hpp"
#include "cas/util.hpp"

template<class VType>
benchmark::ExpPageSize<VType>::ExpPageSize(
      const cas::Context& context)
  : context_(context)
{
}


template<class VType, size_t PAGE_SZ>
cas::BulkLoaderStats benchmark::ExpPageSizeExecutor<VType, PAGE_SZ>::Run(cas::Context context) {
  // print input
  cas::util::Log("Configuration\n");
  std::cout << "page_size_: " << PAGE_SZ << "\n";
  context.Dump();
  std::cout << "\n\n" << std::flush;

  // run benchmark
  cas::Pager<PAGE_SZ> pager{context.index_file_};
  cas::BulkLoader<VType, PAGE_SZ> bulk_loader{pager, context};
  bulk_loader.Load();

  // print output
  std::cout << "\n";
  cas::util::Log("Output:\n\n");
  auto stats = bulk_loader.Stats();
  stats.Dump();
  std::cout << "\n\n\n";

  return stats;
}


template<class VType>
void benchmark::ExpPageSize<VType>::Execute() {
  cas::util::Log("Experiment ExpPageSize\n\n");

  // FIXME
  const std::string& folder = "./";

  context_.input_filename_ = folder + "/dataset.50GB.4096.partition";
  results_[cas::PAGE_SZ_4KB] = ExpPageSizeExecutor<VType, cas::PAGE_SZ_4KB>().Run(context_);

  context_.input_filename_ = folder + "/dataset.50GB.8192.partition";
  results_[cas::PAGE_SZ_8KB] = ExpPageSizeExecutor<VType, cas::PAGE_SZ_8KB>().Run(context_);

  context_.input_filename_ = folder + "/dataset.50GB.16384.partition";
  results_[cas::PAGE_SZ_16KB] = ExpPageSizeExecutor<VType, cas::PAGE_SZ_16KB>().Run(context_);

  context_.input_filename_ = folder + "/dataset.50GB.32768.partition";
  results_[cas::PAGE_SZ_32KB] = ExpPageSizeExecutor<VType, cas::PAGE_SZ_32KB>().Run(context_);

  context_.input_filename_ = folder + "/dataset.50GB.65536.partition";
  results_[cas::PAGE_SZ_64KB] = ExpPageSizeExecutor<VType, cas::PAGE_SZ_64KB>().Run(context_);

  PrintOutput();
}


template<class VType>
void benchmark::ExpPageSize<VType>::PrintOutput() {
  std::cout << "\n\n\n";
  cas::util::Log("Summary:\n\n");
  std::cout << "page_size;runtime_ms;runtime_h;disk_overhead_b;disk_overhead_gb\n";
  for (const auto& [page_size, stats] : results_) {
    auto runtime_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stats.runtime_.time_).count();
    auto runtime_h  = std::chrono::duration_cast<std::chrono::hours>(stats.runtime_.time_).count();
    auto disk_overhead_b  = stats.IoOverhead();
    auto disk_overhead_gb = disk_overhead_b / 1'000'000'000.0;
    std::cout << page_size << ";";
    std::cout << runtime_ms << ";";
    std::cout << runtime_h << ";";
    std::cout << disk_overhead_b << ";";
    std::cout << disk_overhead_gb << "\n";
  }
}


template class benchmark::ExpPageSize<cas::vint64_t>;
