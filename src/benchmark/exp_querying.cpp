#include "benchmark/exp_querying.hpp"
#include "cas/key_encoder.hpp"
#include "cas/util.hpp"
#include <fstream>
#include <sstream>
#include <string>


template<class VType, size_t PAGE_SZ>
benchmark::ExpQuerying<VType, PAGE_SZ>::ExpQuerying(
      const std::string& index_file,
      const std::vector<cas::SearchKey<VType>>& queries,
      int nr_repetitions)
  : index_file_(index_file)
  , queries_(queries)
  , nr_repetitions_(nr_repetitions)
  , pager_(index_file)
  , page_buffer_(0)
{
  bool reverse_paths = false;
  for (const auto& query : queries_) {
    encoded_queries_.push_back(cas::KeyEncoder<VType>::Encode(query, reverse_paths));
  }
}


template<class VType, size_t PAGE_SZ>
void benchmark::ExpQuerying<VType, PAGE_SZ>::Execute() {
  cas::util::Log("Experiment ExpQuerying\n");
  std::cout << "index_file: " << index_file_ << "\n\n";

  results_.reserve(nr_repetitions_ * encoded_queries_.size());
  for (int i = 0; i < nr_repetitions_; ++i) {
    cas::util::Log("Repetition " + std::to_string(i));
    for (const auto& search_key : encoded_queries_) {
      cas::Query<VType, PAGE_SZ> query{pager_, page_buffer_, search_key, cas::kNullEmitter};
      query.Execute();
      results_.push_back(query.Stats());
    }
  }

  PrintOutput();
}


template<class VType, size_t PAGE_SZ>
void benchmark::ExpQuerying<VType, PAGE_SZ>::PrintOutput() {
  std::cout << "\n";
  cas::util::Log("Summary:\n\n");

  double runtime_mus = 0;
  double page_reads = 0;
  double read_nodes = 0;
  double nr_matches = 0;

  for (const auto& stat : results_) {
    runtime_mus += stat.runtime_mus_;
    page_reads += stat.page_reads_;
    read_nodes += stat.read_nodes_;
    nr_matches += stat.nr_matches_;
  }

  double runtime_ms = runtime_mus / 1000;
  double runtime_s = runtime_ms / 1000;

  std::cout << "Totals:\n";
  std::cout << std::fixed << "runtime_mus: " << runtime_mus << "\n";
  std::cout << std::fixed << "runtime_ms: " << runtime_ms << "\n";
  std::cout << std::fixed << "runtime_s: " << runtime_s << "\n";
  std::cout << std::fixed << "page_reads: " << page_reads << "\n";
  std::cout << std::fixed << "read_nodes: " << read_nodes << "\n";
  std::cout << std::fixed << "nr_matches: " << nr_matches << "\n";

  runtime_mus /= results_.size();
  runtime_ms /= results_.size();
  runtime_s /= results_.size();
  page_reads /= results_.size();
  read_nodes /= results_.size();
  nr_matches /= results_.size();

  std::cout << "\nAverages:\n";
  std::cout << std::fixed << "runtime_mus: " << runtime_mus << "\n";
  std::cout << std::fixed << "runtime_ms: " << runtime_ms << "\n";
  std::cout << std::fixed << "runtime_s: " << runtime_s << "\n";
  std::cout << std::fixed << "page_reads: " << page_reads << "\n";
  std::cout << std::fixed << "read_nodes: " << read_nodes << "\n";
  std::cout << std::fixed << "nr_matches: " << nr_matches << "\n";

  std::cout << "\n\n";
  std::cout << std::flush;
}


template class benchmark::ExpQuerying<cas::vint64_t, cas::PAGE_SZ_64KB>;
template class benchmark::ExpQuerying<cas::vint64_t, cas::PAGE_SZ_32KB>;
template class benchmark::ExpQuerying<cas::vint64_t, cas::PAGE_SZ_16KB>;
template class benchmark::ExpQuerying<cas::vint64_t, cas::PAGE_SZ_8KB>;
template class benchmark::ExpQuerying<cas::vint64_t, cas::PAGE_SZ_4KB>;
