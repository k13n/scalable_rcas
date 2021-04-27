#include "benchmark/exp_dsc_computation.hpp"
#include "benchmark/option_parser.hpp"

int main_(int argc, char** argv) {
  using VType = cas::vint64_t;
  constexpr auto PAGE_SZ = cas::PAGE_SZ_16KB;
  using Exp = benchmark::ExpDscComputation<VType, PAGE_SZ>;

  cas::Context context;
  benchmark::option_parser::Parse(argc, argv, context);

  std::vector<cas::DscComputation> approaches = {
    cas::DscComputation::Proactive,
    cas::DscComputation::TwoPass,
  };

  std::vector<size_t> memory_sizes = {
     16'000'000'000,
     32'000'000'000,
     64'000'000'000,
    128'000'000'000,
    256'000'000'000,
  };

  Exp bm{context, approaches, memory_sizes};
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
