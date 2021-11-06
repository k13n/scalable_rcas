#include "benchmark/exp_dataset_size.hpp"
#include "benchmark/option_parser.hpp"

int main_(int argc, char** argv) {
  using VType = cas::vint64_t;
  using Exp = benchmark::ExpDatasetSize<VType>;

  cas::Context context;
  benchmark::option_parser::Parse(argc, argv, context);

  std::vector<size_t> dataset_sizes = {
        797'474'816, // ca.  10M keys (?)
      7'974'846'464, // ca. 100M keys (   99'887'918)
     79'748'497'408, // ca.   1B keys (  998'859'390)
    239'245'508'608, // ca.   3B keys (2'996'513'870)
    398'742'519'808, // ca.   5B keys (4'994'193'029)
                  0, // full dataset  (6'891'972'832)
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
