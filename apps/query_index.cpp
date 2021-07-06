#include "benchmark/option_parser.hpp"
#include "cas/key_encoder.hpp"
#include "cas/query_stats.hpp"
#include "cas/search_key.hpp"
#include "cas/context.hpp"
#include "cas/page_buffer.hpp"
#include "cas/pager.hpp"
#include "cas/query.hpp"
#include <iostream>

int main_(int argc, char** argv) {
  using VType = cas::vint64_t;
  constexpr auto PAGE_SZ = cas::PAGE_SZ_16KB;

  // read input configuration
  auto context = benchmark::option_parser::Parse(argc, argv);
  std::cout << "Configuration:\n";
  context.Dump();

  cas::Pager<PAGE_SZ> pager{context.index_file_};
  cas::PageBuffer<PAGE_SZ> page_buffer{0};

  std::string path = "/arch/arm/boot/**";
  /* std::string path = "/drivers/hid/**"; */
  VType low  = 1455182876;
  VType high = 1555182879;

  cas::SearchKey<VType> skey{path, low, high};
  auto bkey = cas::KeyEncoder<VType>::Encode(skey, false);

  cas::Query<VType, PAGE_SZ> query{pager, page_buffer, bkey, cas::kNullEmitter};
  query.Execute();
  query.Stats().Dump();

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
