#include <cas/key.hpp>
#include <cas/key_encoder.hpp>
#include <cas/index.hpp>
#include <iostream>
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
  return cas::SearchKey<cas::vint64_t>{spath, low, high};
}



template<class VType>
class SelectivityComputer {
  cas::Index<VType, cas::PAGE_SZ_16KB> index_;
  size_t nr_keys_;

public:
  SelectivityComputer(
        const cas::Context& context,
        size_t nr_keys)
    : index_{context}
    , nr_keys_{nr_keys}
  {}

  void Execute(const cas::SearchKey<VType>& skey) {
    cas::SearchKey<VType> values_only{"/**", skey.low_, skey.high_};
    cas::SearchKey<VType> paths_only{skey.path_, cas::VINT64_MIN, cas::VINT64_MAX};

    ExecuteQuery(skey);
    ExecuteQuery(values_only);
    ExecuteQuery(paths_only);

    /* ExecuteConcise(skey); */
    /* ExecuteConcise(values_only); */
    /* ExecuteConcise(paths_only); */
    std::cout << "\n" << std::flush;
  }

private:
  void ExecuteQuery(const cas::SearchKey<VType>& skey) {
    skey.Dump();
    bool reverse_paths = false;
    auto bkey = cas::KeyEncoder<cas::vint64_t>::Encode(skey, reverse_paths);
    auto stats = index_.Query(bkey, cas::kNullEmitter);

    std::cout << "\n";
    stats.Dump();
    std::cout << std::fixed << "selectivity: " << stats.nr_matches_ / static_cast<double>(nr_keys_) << "\n";
    std::cout << "\n\n";
  }

  void ExecuteConcise(const cas::SearchKey<VType>& skey) {
    bool reverse_paths = false;
    auto bkey = cas::KeyEncoder<cas::vint64_t>::Encode(skey, reverse_paths);
    auto stats = index_.Query(bkey, cas::kNullEmitter);

    std::printf("%zu;", stats.nr_matches_);
    std::cout << std::flush;
  }
};


int main_(int argc, char** argv) {
  using VType = cas::vint64_t;
  if (argc < 4) {
    std::cerr << "Three arguments expected!" << std::endl;
    return -1;
  }

  std::string pipeline_dir{argv[1]};
  std::string query_file{argv[2]};
  size_t nr_keys;
  if (sscanf(argv[3], "%zu", &nr_keys) != 1) {
    std::cerr << "Could not parse option nr_keys";
  }

  cas::Context context;
  context.pipeline_dir_ = pipeline_dir;
  SelectivityComputer<VType> computer{context, nr_keys};

  // parse queries
  std::vector<cas::SearchKey<VType>> queries;
  std::ifstream infile(query_file);
  std::string line;
  while (std::getline(infile, line)) {
    queries.push_back(ParseQuery(line, ';'));
  }

  int i = 0;
  for (auto& query : queries) {
    /* printf("Query %3d: ", i); */
    printf("Q%d;", i);
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
