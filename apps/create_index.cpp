#include "benchmark/option_parser.hpp"
#include "cas/bulk_loader.hpp"
#include <iostream>

int main_(int argc, char** argv) {
  using VType = cas::vint64_t;
  constexpr auto PAGE_SZ = cas::PAGE_SZ_16KB;

  // read input configuration
  auto context = benchmark::option_parser::Parse(argc, argv);
  std::cout << "Configuration:\n";
  context.Dump();

  // create index
  cas::BulkLoaderStats stats;
  cas::BulkLoader<VType, PAGE_SZ> bulk_loader{context, stats};
  bulk_loader.Load();

  // print output
  std::cout << "Output:\n";
  stats.Dump();

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
