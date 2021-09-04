#pragma once

#include <cas/context.hpp>
#include <iostream>
#include <getopt.h>


namespace benchmark {
namespace option_parser {


void ParseBool(
      const std::string& optvalue,
      bool& target,
      const char* option_name)
{
  if (optvalue != "0" && optvalue != "1") {
    std::cerr << "Could not parse option --"
      << std::string{option_name}
      << "=" << optvalue << " (expected 1 or 0)\n";
    exit(-1);
  }
  target = (optvalue == "1");
}


void ParseSizeT(
      const char* optarg,
      size_t& target,
      const char* option_name)
{
  if (sscanf(optarg, "%zu", &target) != 1) {
    std::cerr << "Could not parse option --"
      << std::string{option_name}
      << "=" << std::string{optarg} << " (integer of type size_t expected)\n";
    exit(-1);
  }
}


void Parse(int argc, char** argv, cas::Context& context) {
  const int OPT_INPUT_FILENAME = 1;
  const int OPT_PARTITION_FOLDER = 2;
  const int OPT_INDEX_FILE = 3;
  const int OPT_PIPELINE_DIR = 4;
  const int OPT_MEM_SIZE = 5;
  const int OPT_INPUT_SIZE = 6;
  const int OPT_PARTITIONING_THRESHOLD = 7;
  const int OPT_DIRECT_IO = 8;
  const int OPT_PARTITIONING_DSC = 9;
  const int OPT_MEMORY_PLACEMENT = 10;
  static struct option long_options[] = {
    {"input_filename",         required_argument, nullptr, OPT_INPUT_FILENAME},
    {"partition_folder",       required_argument, nullptr, OPT_PARTITION_FOLDER},
    {"index_file",             required_argument, nullptr, OPT_INDEX_FILE},
    {"pipeline_dir",           required_argument, nullptr, OPT_PIPELINE_DIR},
    {"mem_size",               required_argument, nullptr, OPT_MEM_SIZE},
    {"input_size",             required_argument, nullptr, OPT_INPUT_SIZE},
    {"partitioning_threshold", required_argument, nullptr, OPT_PARTITIONING_THRESHOLD},
    {"direct_io",              required_argument, nullptr, OPT_DIRECT_IO},
    {"partitioning_dsc",       required_argument, nullptr, OPT_PARTITIONING_DSC},
    {"memory_placement",       required_argument, nullptr, OPT_MEMORY_PLACEMENT},
    {0, 0, 0, 0}
  };

  while (true) {
    int option_index;
    int c = getopt_long(argc, argv, "", long_options, &option_index);
    if (c == -1) {
      break;
    }
    std::string optvalue{optarg};
    switch (c) {
      case OPT_INPUT_FILENAME:
        context.input_filename_ = optvalue;
        break;
      case OPT_PARTITION_FOLDER:
        context.partition_folder_ = optvalue;
        break;
      case OPT_INDEX_FILE:
        context.index_file_ = optvalue;
        break;
      case OPT_PIPELINE_DIR:
        context.pipeline_dir_ = optvalue;
        break;
      case OPT_MEM_SIZE:
        ParseSizeT(optarg, context.mem_size_bytes_, long_options[option_index].name);
        break;
      case OPT_INPUT_SIZE:
        ParseSizeT(optarg, context.dataset_size_, long_options[option_index].name);
        break;
      case OPT_PARTITIONING_THRESHOLD:
        ParseSizeT(optarg, context.partitioning_threshold_, long_options[option_index].name);
        break;
      case OPT_DIRECT_IO:
        ParseBool(optvalue, context.use_direct_io_, long_options[option_index].name);
        break;
      case OPT_PARTITIONING_DSC:
        if (optvalue == "proactive") {
          context.dsc_computation_ = cas::DscComputation::Proactive;
        } else if (optvalue == "twopass") {
          context.dsc_computation_ = cas::DscComputation::TwoPass;
        } else if (optvalue == "bytebybyte") {
          context.dsc_computation_ = cas::DscComputation::ByteByByte;
        } else {
          std::cerr << "Could not parse option --"
            << std::string{long_options[option_index].name}
            << "=" << optvalue << " (expected {proactive,twopass,bytebybyte})\n";
          exit(-1);
        }
        break;
      case OPT_MEMORY_PLACEMENT:
        if (optvalue == "frontloading") {
          context.memory_placement_ = cas::MemoryPlacement::FrontLoading;
        } else if (optvalue == "allornothing") {
          context.memory_placement_ = cas::MemoryPlacement::AllOrNothing;
        } else if (optvalue == "uniform") {
          context.memory_placement_ = cas::MemoryPlacement::Uniform;
        } else {
          std::cerr << "Could not parse option --"
            << std::string{long_options[option_index].name}
            << "=" << optvalue << " (expected {frontloading,allornothing,uniform})\n";
          exit(-1);
        }
        break;
    }
  }
}


cas::Context Parse(int argc, char** argv) {
  cas::Context context;
  Parse(argc, argv, context);
  return context;
}


}; // namespace option_parser
}; // namespace benchmark
