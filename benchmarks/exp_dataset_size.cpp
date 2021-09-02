#include "benchmark/exp_dataset_size.hpp"
#include "benchmark/option_parser.hpp"

int main_(int argc, char** argv) {
  using VType = cas::vint64_t;
  using Exp = benchmark::ExpDatasetSize<VType>;

  cas::Context context;
  benchmark::option_parser::Parse(argc, argv, context);

  std::vector<size_t> dataset_sizes = {
     25'000'000'000,
     50'000'000'000,
     75'000'000'000,
    100'000'000'000,
    125'000'000'000,
    150'000'000'000,
    200'000'000'000,
    300'000'000'000,
    400'000'000'000,
                  0,
  };

  Exp bm{context, dataset_sizes};
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
