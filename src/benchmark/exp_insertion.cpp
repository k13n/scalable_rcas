#include "benchmark/exp_insertion.hpp"
#include "cas/bulk_loader.hpp"
#include "cas/index.hpp"
#include "cas/util.hpp"
#include <filesystem>


template<class VType, size_t PAGE_SZ>
benchmark::ExpInsertion<VType, PAGE_SZ>::ExpInsertion(
      const cas::Context& context,
      const std::vector<double>& bulkload_fractions)
  : context_(context)
  , bulkload_fractions_(bulkload_fractions)
{ }


template<class VType, size_t PAGE_SZ>
void benchmark::ExpInsertion<VType, PAGE_SZ>::Execute() {
  cas::util::Log("Experiment ExpInsertion\n\n");
  for (const auto& bulkload_fraction : bulkload_fractions_) {
    Execute(bulkload_fraction);
  }
  PrintOutput();
}


template<class VType, size_t PAGE_SZ>
void benchmark::ExpInsertion<VType, PAGE_SZ>::Execute(double bulkload_fraction)
{
  // determine size that needs to be bulk-loaded
  auto context_copy = context_;
  size_t file_size = context_copy.dataset_size_ > 0
    ? context_copy.dataset_size_
    : std::filesystem::file_size(context_copy.input_filename_);
  size_t nr_total_pages = file_size / PAGE_SZ;
  size_t nr_pages_bulkload = nr_total_pages * bulkload_fraction;
  context_copy.dataset_size_ = nr_pages_bulkload * PAGE_SZ;

  cas::Index<VType, PAGE_SZ> index{context_copy};
  index.ClearPipelineFiles();

  // print configuration
  std::cout << "bulkload_fraction: " << bulkload_fraction << "\n";
  context_copy.Dump();
  std::cout << "\n" << std::flush;

  // bulk-load first chunk
  if (context_copy.dataset_size_ > 0) {
    index.BulkLoad();
  }

  // prepare cursor to read remaining keys
  cas::Partition<PAGE_SZ> partition{context_copy.input_filename_, index.Stats(), context_copy};
  partition.FptrCursorFirstPageNr(nr_pages_bulkload);
  std::vector<std::byte> page_buffer;
  page_buffer.resize(PAGE_SZ);
  cas::MemoryPage<PAGE_SZ> io_page{&page_buffer[0]};
  auto cursor = partition.Cursor(io_page);

  // insert remaining keys
  size_t i = nr_pages_bulkload;
  while (cursor.HasNext() && i < nr_total_pages) {
    for (auto key : io_page) {
      index.Insert(key);
    }
    ++i;
  }
  index.FlushMemoryResidentKeys();

  // print results
  std::cout << "Results:\n";
  index.Stats().Dump();
  std::cout << "\n\n" << std::flush;

  results_.emplace_back(bulkload_fraction, index.Stats());
}


template<class VType, size_t PAGE_SZ>
void benchmark::ExpInsertion<VType, PAGE_SZ>::PrintOutput() {
  std::cout << "\n\n\n";
  cas::util::Log("Summary:\n\n");
  std::cout << "bulkload_fraction;runtime_ms;runtime_m;runtime_h;disk_overhead_b;disk_overhead_gb;disk_io_gb\n";
  for (const auto& [bulkload_fraction, stats] : results_) {
    auto runtime_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stats.runtime_.time_).count();
    auto runtime_m  = std::chrono::duration_cast<std::chrono::minutes>(stats.runtime_.time_).count();
    auto runtime_h  = std::chrono::duration_cast<std::chrono::hours>(stats.runtime_.time_).count();
    auto disk_overhead_b  = stats.IoOverhead();
    auto disk_overhead_gb = disk_overhead_b / 1'000'000'000.0;
    std::cout << bulkload_fraction << ";";
    std::cout << runtime_ms << ";";
    std::cout << runtime_m << ";";
    std::cout << runtime_h << ";";
    std::cout << disk_overhead_b << ";";
    std::cout << disk_overhead_gb << ";";
    std::cout << stats.DiskIo() / 1'000'000'000.0 << "\n";
  }
}

template class benchmark::ExpInsertion<cas::vint64_t, cas::PAGE_SZ_64KB>;
template class benchmark::ExpInsertion<cas::vint64_t, cas::PAGE_SZ_32KB>;
template class benchmark::ExpInsertion<cas::vint64_t, cas::PAGE_SZ_16KB>;
template class benchmark::ExpInsertion<cas::vint64_t, cas::PAGE_SZ_8KB>;
template class benchmark::ExpInsertion<cas::vint64_t, cas::PAGE_SZ_4KB>;
