#include "benchmark/exp_querying.hpp"
#include "cas/key_encoder.hpp"
#include "cas/query_executor.hpp"
#include "cas/util.hpp"
#include <fstream>
#include <sstream>
#include <string>


template<class VType>
benchmark::ExpQuerying<VType>::ExpQuerying(
      const std::string& index_file,
      const std::vector<cas::SearchKey<VType>>& queries,
      int nr_repetitions)
  : index_file_(index_file)
  , queries_(queries)
  , nr_repetitions_(nr_repetitions)
{
  bool reverse_paths = false;
  for (const auto& query : queries_) {
    encoded_queries_.push_back(cas::KeyEncoder<VType>::Encode(query, reverse_paths));
  }
}


template<class VType>
void benchmark::ExpQuerying<VType>::Execute() {
  cas::util::Log("Experiment ExpQuerying\n");
  std::cout << "index_file: " << index_file_ << "\n\n";

  results_.reserve(nr_repetitions_ * encoded_queries_.size());
  for (int i = 0; i < nr_repetitions_; ++i) {
    cas::util::Log("Repetition " + std::to_string(i));
    for (const auto& search_key : encoded_queries_) {
      const cas::BinaryKeyEmitter emitter = [](
          const cas::QueryBuffer& /* path */, size_t /* p_len */,
          const cas::QueryBuffer& /* value */, size_t /* v_len */,
          cas::ref_t ref) -> void {
        cas::ToString(ref);
      };
      cas::QueryExecutor query{index_file_};
      auto stats = query.Execute(search_key, emitter);
      results_.push_back(stats);
    }
  }

  PrintOutput();
}


template<class VType>
void benchmark::ExpQuerying<VType>::PrintOutput() {
  std::cout << "\n";
  cas::util::Log("Results per query:\n\n");

  int i = 0;
  for (const auto& stat : results_) {
    std::cout << "Q" << i << ";"
      << stat.nr_matches_ << ";"
      << (stat.runtime_mus_ / 1000) << "\n";
    ++i;
  }
  std::cout << "\n";

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


template class benchmark::ExpQuerying<cas::vint64_t>;
