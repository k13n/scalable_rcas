#include "benchmark/exp_partitioning_threshold.hpp"
#include "benchmark/option_parser.hpp"
#include "cas/util.hpp"

int main_(int argc, char** argv) {
  using VType = cas::vint64_t;
  using Exp = benchmark::ExpPartitioningThreshold<VType>;

  cas::Context context;
  benchmark::option_parser::Parse(argc, argv, context);

  std::vector<size_t> partitioning_thresholds = {
    10000,
    1000,
    100,
    10,
    1,
  };
  std::vector<size_t> dataset_sizes = {
     /* 500'000'000, */
     /* 1'000'000'000, */
     /* 10'000'000'000, */
     /* 25'000'000'000, */
     /* 50'000'000'000, */
     /* 75'000'000'000, */
    100'000'000'000,
  };

  // parse queries
  std::string query_file = "";
  auto queries = cas::util::ParseQueryFile(query_file, ',');

  Exp bm{context, partitioning_thresholds, dataset_sizes, queries};
  bm.Execute();

  return 0;
}

int main(int argc, char** argv) {
  try {
    return main_(argc, argv);
  } catch (std::exception& e) {
    std::cerr << "Standard exception. What: " << e.what() << std::endl;
    return 10;
  } catch (...) {
    std::cerr << "Unknown exception." << std::endl;
    return 11;
  }
}
