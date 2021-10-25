#include "benchmark/exp_mem_insertion.hpp"
#include "cas/bulk_loader.hpp"
#include "cas/index.hpp"
#include "cas/util.hpp"
#include "cas/mem/insertion.hpp"
#include <filesystem>
#include <sstream>


template<class VType>
benchmark::ExpMemInsertion<VType>::ExpMemInsertion(
      const std::string& dataset_path,
      const size_t nr_keys)
  : dataset_path_(dataset_path)
  , nr_keys_(nr_keys)
{ }


template<class VType>
void benchmark::ExpMemInsertion<VType>::Execute() {
  cas::util::Log("Experiment ExpMemInsertion\n\n");

  cas::BulkLoaderStats stats;
  cas::Context context;

  // prepare cursor to read remaining keys
  cas::Partition partition{dataset_path_, stats, context};
  partition.FptrCursorFirstPageNr(0);
  std::vector<std::byte> page_buffer;
  page_buffer.resize(cas::PAGE_SZ);
  cas::MemoryPage io_page{&page_buffer[0]};
  auto cursor = partition.Cursor(io_page);

  // root of the in-memory RCAS index
  cas::mem::Node* idx_root_ = nullptr;
  size_t nr_inserted_keys = 0;

  // insert remaining keys
  auto start = std::chrono::high_resolution_clock::now();
  auto print_progress = [&start,&nr_inserted_keys]() -> void {
    auto now = std::chrono::high_resolution_clock::now();
    auto time = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
    std::stringstream ss;
    ss << "(nr_keys, runtime_ms): ("
      << nr_inserted_keys << ", "
      << time
      << ")\n";
    cas::util::Log(ss.str());
    std::cout << std::flush;
  };
  while (cursor.HasNext() && nr_inserted_keys <= nr_keys_) {
    for (auto key : io_page) {
      if (nr_inserted_keys > nr_keys_) {
        break;
      }
      size_t partitioning_threshold = 1;
      cas::mem::Insertion insertion{&idx_root_, key, partitioning_threshold};
      insertion.Execute();
      ++nr_inserted_keys;
      if (nr_inserted_keys % 10'000'000 == 0) {
        print_progress();
      }
    }
  }
  print_progress();
}


template class benchmark::ExpMemInsertion<cas::vint64_t>;
