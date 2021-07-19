#include "benchmark/exp_querying.hpp"
#include "benchmark/option_parser.hpp"
#include "cas/search_key.hpp"
#include "cas/key_encoder.hpp"
#include "cas/page_buffer.hpp"
#include "cas/pager.hpp"
#include "cas/query.hpp"
#include <fstream>
#include <sstream>
#include <string>
#include <filesystem>
#include <getopt.h>

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


void ExecuteExperiment(
      const std::string& index_file,
      const std::string& query_file,
      int nr_repetitions)
{
  using VType = cas::vint64_t;
  using Exp = benchmark::ExpQuerying<VType>;

  std::cout << "Input:\n";
  std::cout << "index_file: " << index_file << "\n";
  std::cout << "query_file: " << query_file << "\n";

  // parse queries
  std::vector<cas::SearchKey<VType>> queries;
  std::ifstream infile(query_file);
  std::string line;
  while (std::getline(infile, line)) {
    queries.push_back(ParseQuery(line, ';'));
  }

  // execute experiment
  Exp bm{index_file, queries, nr_repetitions};
  bm.Execute();
}


int main_(int argc, char** argv) {
  const int OPT_INDEX_FILE = 1;
  const int OPT_QUERY_FILE = 2;
  const int OPT_NR_REPETITIONS = 3;
  static struct option long_options[] = {
    {"index_file",     required_argument, nullptr, OPT_INDEX_FILE},
    {"query_file",     required_argument, nullptr, OPT_QUERY_FILE},
    {"nr_repetitions", required_argument, nullptr, OPT_NR_REPETITIONS},
    {0, 0, 0, 0}
  };

  std::string index_file;
  std::string query_file;
  int nr_repetitions = 1;
  while (true) {
    int option_index;
    int c = getopt_long(argc, argv, "", long_options, &option_index);
    if (c == -1) {
      break;
    }
    std::string optvalue{optarg};
    switch (c) {
      case OPT_INDEX_FILE:
        index_file = optvalue;
        break;
      case OPT_QUERY_FILE:
        query_file = optvalue;
        break;
      case OPT_NR_REPETITIONS:
        if (sscanf(optarg, "%d", &nr_repetitions) != 1) {
          std::cerr << "Could not parse option --nr_repetitions (positive integer expected)\n";
          return 1;
        }
    }
  }

  if (!std::filesystem::exists(index_file)) {
    std::cerr << "specify valid input file with --index_file\n";
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

  ExecuteExperiment(index_file, query_file, nr_repetitions);
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
