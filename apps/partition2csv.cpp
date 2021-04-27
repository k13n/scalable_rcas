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
  size_t page_sz = 0;
  std::string input_filename = "";
  bool read_from_file = false;

  // read page size
  if (argc < 2) {
    std::cerr << "First parameter: page size missing\n";
    return 1;
  } else {
    if (1 != std::sscanf(argv[1], "%zu", &page_sz)) {
      std::cerr << "First parameter: page size (integer)\n";
      return 1;
    }
    if (page_sz % 4096 != 0) {
      std::cerr << "Page size must be a multiple of 4096\n";
      return 1;
    }
  }
  // check if input is coming from stdin or file
  if (argc >= 3) {
    read_from_file = true;
    input_filename = std::string{argv[2]};
  }

  // create page as buffer
  std::vector<std::byte> buffer{page_sz, std::byte{0}};
  cas::MemoryPage<4096> page{&buffer[0]};
  // buffer to store path&values
  cas::QueryBuffer buf_path;
  cas::QueryBuffer buf_value;

  // open correct input file
  std::FILE* infile = nullptr;
  if (read_from_file) {
    infile = fopen(input_filename.c_str(), "rb");
    if (infile == nullptr) {
      throw std::runtime_error{"file cannot be opened: " + input_filename};
    }
  } else if ((infile = freopen(nullptr, "rb", stdin)) == nullptr) {
    std::cerr << "Cannot open stdin for binary reading\n";
    return 1;
  }

  // process input
  while (true) {
    size_t bytes_read = fread(page.Data(), 1, page_sz, infile);
    if (bytes_read < page_sz) {
      break;
    }
    for (const auto& bkey : page) {
      std::memcpy(buf_path.data(), bkey.Path(), bkey.LenPath());
      std::memcpy(buf_value.data(), bkey.Value(), bkey.LenValue());
      auto key = cas::KeyDecoder<cas::vint64_t>::Decode(
          buf_path, bkey.LenPath(),
          buf_value, bkey.LenValue(),
          bkey.Ref());
      // printing key
      std::cout
        << key.path_ << ";"
        << key.value_ << ";"
        << cas::ToString(key.ref_)
        << "\n";
    }
  }

  if (read_from_file) {
    fclose(infile);
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
