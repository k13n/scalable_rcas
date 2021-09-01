#include "benchmark/exp_querying.hpp"
#include "cas/key_encoder.hpp"
#include "cas/index.hpp"
#include "cas/util.hpp"
#include <fstream>
#include <sstream>
#include <string>


template<class VType>
benchmark::ExpQuerying<VType>::ExpQuerying(
      const std::string& pipeline_dir,
      const std::vector<cas::SearchKey<VType>>& queries,
      bool clear_page_cache,
      int nr_repetitions)
  : pipeline_dir_(pipeline_dir)
  , queries_(queries)
  , clear_page_cache_(clear_page_cache)
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
  std::cout << "pipeline_dir: " << pipeline_dir_ << "\n";
  std::cout << "clear_page_cache: " << clear_page_cache_ << "\n\n";

  results_.reserve(nr_repetitions_ * encoded_queries_.size());
  for (int i = 0; i < nr_repetitions_; ++i) {
    cas::util::Log("Repetition " + std::to_string(i));
    cas::Context context;
    context.pipeline_dir_ = pipeline_dir_;
    cas::Index<VType, cas::PAGE_SZ_16KB> index{context};
    for (const auto& search_key : encoded_queries_) {
      if (clear_page_cache_) {
        cas::util::ClearPageCache();
      }
      const cas::BinaryKeyEmitter emitter = [](
          const cas::QueryBuffer& /* path */, size_t /* p_len */,
          const cas::QueryBuffer& /* value */, size_t /* v_len */,
          cas::ref_t ref) -> void {
        cas::ToString(ref);
      };
      auto stats = index.Query(search_key, emitter);
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
      << stat.read_nodes_ << ";"
      << (stat.runtime_mus_ / 1000) << "\n";
    ++i;
  }
  std::cout << "\n";

  double runtime_mus = 0;
  double read_nodes = 0;
  double nr_matches = 0;

  for (const auto& stat : results_) {
    runtime_mus += stat.runtime_mus_;
    read_nodes += stat.read_nodes_;
    nr_matches += stat.nr_matches_;
  }

  double runtime_ms = runtime_mus / 1000;
  double runtime_s = runtime_ms / 1000;

  std::cout << "Totals:\n";
  std::cout << std::fixed << "runtime_mus: " << runtime_mus << "\n";
  std::cout << std::fixed << "runtime_ms: " << runtime_ms << "\n";
  std::cout << std::fixed << "runtime_s: " << runtime_s << "\n";
  std::cout << std::fixed << "read_nodes: " << read_nodes << "\n";
  std::cout << std::fixed << "nr_matches: " << nr_matches << "\n";

  runtime_mus /= results_.size();
  runtime_ms /= results_.size();
  runtime_s /= results_.size();
  read_nodes /= results_.size();
  nr_matches /= results_.size();

  std::cout << "\nAverages:\n";
  std::cout << std::fixed << "runtime_mus: " << runtime_mus << "\n";
  std::cout << std::fixed << "runtime_ms: " << runtime_ms << "\n";
  std::cout << std::fixed << "runtime_s: " << runtime_s << "\n";
  std::cout << std::fixed << "read_nodes: " << read_nodes << "\n";
  std::cout << std::fixed << "nr_matches: " << nr_matches << "\n";

  std::cout << "\n\n";
  std::cout << std::flush;
}


template class benchmark::ExpQuerying<cas::vint64_t>;
