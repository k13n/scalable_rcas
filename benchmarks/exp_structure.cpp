#include "benchmark/exp_structure.hpp"
#include "benchmark/option_parser.hpp"

int main_(int argc, char** argv) {
  using VType = cas::vint64_t;
  constexpr auto PAGE_SZ = cas::PAGE_SZ_16KB;
  using Exp = benchmark::ExpStructure<VType, PAGE_SZ>;

  cas::Context context;
  benchmark::option_parser::Parse(argc, argv, context);

  Exp bm{context};
  bm.Execute();

  return 0;
}

int main(int argc, char** argv) {
  try {
    return main_(argc, argv);
  } catch (std::exception& e) {
    std::cerr << "Standard exception. What: " << e.what() << "\n";
    return 10;
  } catch (...) {
    std::cerr << "Unknown exception.\n";
    return 11;
  }
}
