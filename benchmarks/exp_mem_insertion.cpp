#include "benchmark/exp_mem_insertion.hpp"
#include "benchmark/option_parser.hpp"

int main_(int argc, char** argv) {
  using VType = cas::vint64_t;
  using Exp = benchmark::ExpMemInsertion<VType>;

  cas::Context context;
  benchmark::option_parser::Parse(argc, argv, context);

  size_t nr_keys = 100'000'000;
  Exp bm{context.input_filename_, nr_keys};
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
