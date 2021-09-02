#include "benchmark/exp_partitioning_threshold.hpp"
#include "benchmark/option_parser.hpp"

int main_(int argc, char** argv) {
  using VType = cas::vint64_t;
  using Exp = benchmark::ExpPartitioningThreshold<VType>;

  cas::Context context;
  benchmark::option_parser::Parse(argc, argv, context);

  std::vector<size_t> partitioning_thresholds = {
    1,
    200,
    1000,
  };
  std::vector<size_t> dataset_sizes = {
     25'000'000'000,
     50'000'000'000,
     75'000'000'000,
    100'000'000'000,
  };

  Exp bm{context, partitioning_thresholds, dataset_sizes};
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
