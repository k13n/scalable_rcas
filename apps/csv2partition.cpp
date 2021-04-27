#include <cas/key.hpp>
#include <cas/key_encoder.hpp>
#include <cas/memory_page.hpp>
#include <cas/swh_pid.hpp>
#include <cas/types.hpp>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>
#include <csignal>

volatile sig_atomic_t g_signal_status = 0;
void signal_handler(int signal) {
  g_signal_status = signal;
}


cas::Key<cas::vint64_t> ParseInputLine(
      const std::string& line,
      char delimiter) {
  cas::Key<cas::vint64_t> key;

  std::string path;
  std::string value;
  std::string ref;
  std::stringstream line_stream(line);
  std::getline(line_stream, path,  delimiter);
  std::getline(line_stream, value, delimiter);
  std::getline(line_stream, ref,   delimiter);

  key.path_  = std::move(path);
  key.value_ = std::stoll(value);
  key.ref_   = cas::ParseSwhPid(ref);

  return key;
}


template<size_t PAGE_SZ>
void Csv2Partition() {
  // create page as buffer
  std::vector<std::byte> buffer{PAGE_SZ, std::byte{0}};
  cas::MemoryPage<PAGE_SZ> page{&buffer[0]};
  std::array<std::byte, PAGE_SZ> key_buffer;
  cas::BinaryKey bkey(key_buffer.data());
  std::array<std::byte, PAGE_SZ> ref_key_buffer;
  cas::BinaryKey ref_bkey(ref_key_buffer.data());

  // this is a conservatively high estimate
  // we need:
  // - page nr_keys:  2B
  // - key len_path:  2B
  // - key len_value: 2B
  const size_t key_header_size = 32;

  size_t nr_keys  = 0;
  size_t nr_pages = 0;
  size_t dsc_P = 0;
  size_t dsc_V = 0;

  // read input until file is completely processed or
  // the process is interrupted by the user
  char delimiter = ';';
  std::string line;
  while (std::getline(std::cin, line) && g_signal_status == 0) {
    auto key = ParseInputLine(line, delimiter);

    // there can be revisions in SWH that actually did not change any
    // files; let's ignore those keys
    if (key.path_.empty()) {
      continue;
    }

    // skip keys that are close to 4KB long
    // this makes sure that every key fits into a page
    if (key.Size() + key_header_size > cas::PAGE_SZ_4KB) {
      continue;
    }

    // filter out revisions before 1950 or after 2025
    // (just a sanity check, shouldn't exist in the first place)
    if (key.value_ < -633916800 || key.value_ > 1732924800) {
      continue;
    }

    cas::KeyEncoder<cas::vint64_t>::Encode(key, bkey);
    if (bkey.ByteSize() > page.FreeSpace()) {
      // write page to stdout since it is full
      std::fwrite(page.Data(), 1, PAGE_SZ, stdout);
      page.Reset();
      ++nr_pages;
    }
    page.Push(bkey);

    // update stats
    ++nr_keys;
    if (nr_keys == 1) {
      cas::KeyEncoder<cas::vint64_t>::Encode(key, ref_bkey);
      dsc_P = ref_bkey.LenPath();
      dsc_V = ref_bkey.LenValue();
    } else {
      size_t g_P = 0;
      size_t g_V = 0;
      while (g_P < dsc_P && bkey.Path()[g_P] == ref_bkey.Path()[g_P]) {
        ++g_P;
      }
      while (g_V < dsc_V && bkey.Value()[g_V] == ref_bkey.Value()[g_V]) {
        ++g_V;
      }
      dsc_P = g_P;
      dsc_V = g_V;
      if (g_P == 0) {
        std::cerr << bkey.LenPath() << std::endl;
        std::cerr << static_cast<int>(bkey.Path()[0]) << std::endl;
        exit(1);
      }
    }
  }

  // flush last page to stdout
  std::fwrite(page.Data(), 1, PAGE_SZ, stdout);
  ++nr_pages;

  // write statistics to stderr
  std::cerr << "nr_keys:  " << nr_keys << "\n";
  std::cerr << "nr_pages: " << nr_pages << "\n";
  std::cerr << "dsc_P: " << dsc_P << "\n";
  std::cerr << "dsc_V: " << dsc_V << "\n";
  std::cerr << std::flush;
}


int main_(int argc, char** argv) {
  signal(SIGINT, signal_handler);

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

  std::ifstream infile(input_filename);
  if (read_from_file) {
    if (!infile.is_open()) {
      throw std::runtime_error{"file cannot be opened: " + input_filename};
    }
    std::cin.rdbuf(infile.rdbuf());
  }

  // open stdout for binary writing
  if (freopen(nullptr, "wb", stdout) == nullptr) {
    std::cerr << "Cannot open stdout for binary writing\n";
    return 1;
  }

  switch (page_sz) {
    case cas::PAGE_SZ_4KB:
      Csv2Partition<cas::PAGE_SZ_4KB>();
      break;
    case cas::PAGE_SZ_8KB:
      Csv2Partition<cas::PAGE_SZ_8KB>();
      break;
    case cas::PAGE_SZ_16KB:
      Csv2Partition<cas::PAGE_SZ_16KB>();
      break;
    case cas::PAGE_SZ_32KB:
      Csv2Partition<cas::PAGE_SZ_32KB>();
      break;
    case cas::PAGE_SZ_64KB:
      Csv2Partition<cas::PAGE_SZ_64KB>();
      break;
    default:
      std::cerr << "Page size invalid" << std::endl;
  }

  if (infile.is_open()) {
    infile.close();
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
