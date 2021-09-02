#include "benchmark/exp_insertion.hpp"
#include "benchmark/option_parser.hpp"

int main_(int argc, char** argv) {
  using VType = cas::vint64_t;
  using Exp = benchmark::ExpInsertion<VType>;

  cas::Context context;
  benchmark::option_parser::Parse(argc, argv, context);

  std::vector<double> bulkload_fractions = {
    1.0,
    0.5,
    0.0,
  };

  Exp bm{context, bulkload_fractions};
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
