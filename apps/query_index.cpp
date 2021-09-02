#include "benchmark/option_parser.hpp"
#include "cas/key_encoder.hpp"
#include "cas/query_executor.hpp"
#include "cas/search_key.hpp"
#include "cas/context.hpp"
#include "cas/pager.hpp"
#include "cas/query.hpp"
#include <iostream>

int main_(int argc, char** argv) {
  using VType = cas::vint64_t;

  // read input configuration
  auto context = benchmark::option_parser::Parse(argc, argv);
  std::cout << "Configuration:\n";
  context.Dump();

  std::string path = "/arch/arm/boot/**";
  /* std::string path = "/drivers/hid/**"; */
  VType low  = 1455182876;
  VType high = 1555182879;

  cas::SearchKey<VType> skey{path, low, high};
  auto bkey = cas::KeyEncoder<VType>::Encode(skey, false);

  cas::QueryExecutor query{context.index_file_};
  auto stats = query.Execute(bkey, cas::kNullEmitter);
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
