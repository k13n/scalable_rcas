#include <cas/key.hpp>
#include <cas/key_encoder.hpp>
#include <cas/query.hpp>
#include <fstream>
#include <sstream>
#include <string>
#include <filesystem>


cas::SearchKey<cas::vint64_t> ParseQuery(
      const std::string& line,
      char delimiter) {
  std::string spath;
  std::string slow;
  std::string shigh;

  std::stringstream line_stream(line);
  std::getline(line_stream, spath,  delimiter);
  std::getline(line_stream, slow, delimiter);
  std::getline(line_stream, shigh, delimiter);

  cas::vint64_t low  = std::stoll(slow);
  cas::vint64_t high = std::stoll(shigh);
  return cas::SearchKey<cas::vint64_t>{std::move(spath), low, high};
}



template<class VType, size_t PAGE_SZ>
class SelectivityComputer {
  size_t nr_keys_;
  cas::Pager<PAGE_SZ> pager_;
  cas::PageBuffer<PAGE_SZ> page_buffer_;

public:
  SelectivityComputer(
        const std::string& index_file,
        size_t nr_keys)
    : nr_keys_{nr_keys}
    , pager_{index_file}
    , page_buffer_{0}
  {}

  void Execute(cas::SearchKey<VType> skey) {
    cas::SearchKey<VType> values_only{"/**", skey.low_, skey.high_};
    cas::SearchKey<VType> paths_only{skey.path_, 0, cas::VINT64_MAX};

    /* ExecuteQuery(skey); */
    /* ExecuteQuery(values_only); */
    /* ExecuteQuery(paths_only); */

    ExecuteConcise(skey);
    ExecuteConcise(values_only);
    ExecuteConcise(paths_only);
    std::cout << "\n";
  }

private:
  void ExecuteQuery(cas::SearchKey<VType> skey) {
    skey.Dump();
    bool reverse_paths = false;
    auto bkey = cas::KeyEncoder<cas::vint64_t>::Encode(skey, reverse_paths);
    cas::Query<VType, PAGE_SZ> query{pager_, page_buffer_, bkey, cas::kNullEmitter};
    query.Execute();

    std::cout << "\n";
    query.Stats().Dump();
    std::cout << std::fixed << "selectivity: " << query.Stats().nr_matches_ / static_cast<double>(nr_keys_) << "\n";
    std::cout << "\n\n";
  }

  void ExecuteConcise(cas::SearchKey<VType> skey) {
    bool reverse_paths = false;
    auto bkey = cas::KeyEncoder<cas::vint64_t>::Encode(skey, reverse_paths);
    cas::Query<VType, PAGE_SZ> query{pager_, page_buffer_, bkey, cas::kNullEmitter};
    query.Execute();

    std::printf("%10zu (%f)  ",
        query.Stats().nr_matches_,
        query.Stats().nr_matches_ / static_cast<double>(nr_keys_));
  }
};


int main_(int argc, char** argv) {
  using VType = cas::vint64_t;
  constexpr auto PAGE_SZ = cas::PAGE_SZ_16KB;

  if (argc < 4) {
    std::cerr << "Three arguments expected!" << std::endl;
    return -1;
  }

  std::string index_file{argv[1]};
  std::string query_file{argv[2]};
  size_t nr_keys;
  if (sscanf(argv[3], "%zu", &nr_keys) != 1) {
    std::cerr << "Could not parse option nr_keys";
  }

  SelectivityComputer<VType, PAGE_SZ> computer{index_file, nr_keys};

  // parse queries
  std::vector<cas::SearchKey<VType>> queries;
  std::ifstream infile(query_file);
  std::string line;
  while (std::getline(infile, line)) {
    queries.push_back(ParseQuery(line, ';'));
  }

  int i = 1;
  for (auto& query : queries) {
    printf("Query %3d: ", i);
    computer.Execute(query);
    std::cout << std::flush;
    ++i;
  }

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
