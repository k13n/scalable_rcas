#include "benchmark/exp_memory_management.hpp"
#include "benchmark/option_parser.hpp"

int main_(int argc, char** argv) {
  using VType = cas::vint64_t;
  using Exp = benchmark::ExpMemoryManagement<VType>;

  /* cas::Context context = { */
  /*   .use_direct_io_ = true, */
  /* }; */
  cas::Context context;
  benchmark::option_parser::Parse(argc, argv, context);

  std::vector<cas::MemoryPlacement> approaches = {
    /* cas::MemoryPlacement::FrontLoading, */
    cas::MemoryPlacement::AllOrNothing,
  };

  std::vector<size_t> memory_sizes = {
     16'000'000'000, //  200M keys
     32'000'000'000, //  400M keys
     64'000'000'000, //  800M keys
    128'000'000'000, // 1600M keys
    256'000'000'000, // 3200M keys
  };

  Exp bm{context, approaches, memory_sizes};
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
