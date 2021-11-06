#include <cas/key.hpp>
#include <cas/key_decoder.hpp>
#include <cas/memory_page.hpp>
#include <cas/swh_pid.hpp>
#include <cas/types.hpp>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>


int main_(int argc, char** argv) {
  // check if input is coming from stdin or file
  if (argc < 3) {
    std::cerr << "count_pages <path> <nr_keys>\n";
    return 1;
  }

  std::string input_filename = std::string{argv[1]};
  long nr_target_keys = atol(argv[2]);

  // create page as buffer
  std::vector<std::byte> buffer{cas::PAGE_SZ, std::byte{0}};
  cas::MemoryPage page{&buffer[0]};

  // open correct input file
  std::FILE* infile = fopen(input_filename.c_str(), "rb");
  if (infile == nullptr) {
    throw std::runtime_error{"file cannot be opened: " + input_filename};
  }

  // process input
  size_t nr_keys = 0;
  size_t nr_pages = 0;
  while (nr_keys < nr_target_keys) {
    size_t bytes_read = fread(page.Data(), 1, cas::PAGE_SZ, infile);
    if (bytes_read < cas::PAGE_SZ) {
      break;
    }
    for (const auto& bkey : page) {
      ++nr_keys;
    }
    ++nr_pages;
  }

  fclose(infile);

  std::cout << "nr_keys:  " << nr_keys << "\n";
  std::cout << "nr_pages: " << nr_pages << "\n";
  std::cout << "input_size: " << (nr_pages * cas::PAGE_SZ) << "\n";

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
