#include "benchmark/exp_querying.hpp"
#include "benchmark/option_parser.hpp"
#include "cas/search_key.hpp"
#include "cas/key_encoder.hpp"
#include "cas/pager.hpp"
#include "cas/query.hpp"
#include "cas/util.hpp"
#include <fstream>
#include <sstream>
#include <string>
#include <filesystem>
#include <getopt.h>

void ExecuteExperiment(
      const std::string& pipeline_dir,
      const std::string& query_file,
      bool clear_page_cache,
      int nr_repetitions)
{
  using VType = cas::vint64_t;
  using Exp = benchmark::ExpQuerying<VType>;

  std::cout << "Input:\n";
  std::cout << "pipeline_dir: " << pipeline_dir << "\n";
  std::cout << "query_file: " << query_file << "\n";
  std::cout << "clear_page_cache: " << clear_page_cache << "\n";

  // parse queries
  auto queries = cas::util::ParseQueryFile(query_file, ',');

  // execute experiment
  Exp bm{pipeline_dir, queries, clear_page_cache, nr_repetitions};
  bm.Execute();
}


int main_(int argc, char** argv) {
  const int OPT_PIPELINE_DIR = 1;
  const int OPT_QUERY_FILE = 2;
  const int OPT_NR_REPETITIONS = 3;
  const int OPT_CLEAR_PAGE_CACHE = 4;
  static struct option long_options[] = {
    {"pipeline_dir",     required_argument, nullptr, OPT_PIPELINE_DIR},
    {"query_file",       required_argument, nullptr, OPT_QUERY_FILE},
    {"nr_repetitions",   required_argument, nullptr, OPT_NR_REPETITIONS},
    {"clear_page_cache", required_argument, nullptr, OPT_CLEAR_PAGE_CACHE},
    {0, 0, 0, 0}
  };

  std::string pipeline_dir;
  std::string query_file;
  int nr_repetitions = 1;
  bool clear_page_cache = false;
  while (true) {
    int option_index;
    int c = getopt_long(argc, argv, "", long_options, &option_index);
    if (c == -1) {
      break;
    }
    std::string optvalue{optarg};
    switch (c) {
      case OPT_PIPELINE_DIR:
        pipeline_dir = optvalue;
        break;
      case OPT_QUERY_FILE:
        query_file = optvalue;
        break;
      case OPT_NR_REPETITIONS:
        if (sscanf(optarg, "%d", &nr_repetitions) != 1) {
          std::cerr << "Could not parse option --nr_repetitions (positive integer expected)\n";
          return 1;
        }
        break;
      case OPT_CLEAR_PAGE_CACHE:
        clear_page_cache = (optvalue == "1" || optvalue == "t");
        break;
    }
  }

  if (!std::filesystem::exists(pipeline_dir)) {
    std::cerr << "specify valid pipeline_dir with --pipeline_dir\n";
    return 1;
  }
  if (!std::filesystem::exists(query_file)) {
    std::cerr << "specify valid query file with --query_file\n";
    return 1;
  }
  if (nr_repetitions < 0) {
    std::cerr << "--nr_repetitions must be a positive integer\n";
    return 1;
  }

  ExecuteExperiment(pipeline_dir, query_file, clear_page_cache, nr_repetitions);
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
