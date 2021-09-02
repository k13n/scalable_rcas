#include "benchmark/exp_dataset_size.hpp"
#include "benchmark/exp_memory_management.hpp"
#include "benchmark/option_parser.hpp"


void VaryInputSize(cas::Context context) {
  using VType = cas::vint64_t;
  using Exp = benchmark::ExpDatasetSize<VType>;

  // fix memory size at 50GB
  context.mem_size_bytes_ = 50'000'000'000;

  std::vector<size_t> dataset_sizes = {
    100'000'000'000,
    200'000'000'000,
    300'000'000'000,
    400'000'000'000,
                  0,
  };

  Exp bm{context, dataset_sizes};
  bm.Execute();
}


void VaryMemorySize(cas::Context context) {
  using VType = cas::vint64_t;
  using Exp = benchmark::ExpMemoryManagement<VType>;

  std::vector<cas::MemoryPlacement> approaches = {
    cas::MemoryPlacement::FrontLoading,
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
}


int main_(int argc, char** argv) {
  cas::Context context;
  benchmark::option_parser::Parse(argc, argv, context);

  VaryInputSize(context);
  VaryMemorySize(context);

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
