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


template<size_t IN_PAGE_SZ, size_t OUT_PAGE_SZ>
void Repartition(std::FILE* infile) {
  // create page as buffer
  std::vector<std::byte> in_buffer{IN_PAGE_SZ, std::byte{0}};
  cas::MemoryPage<IN_PAGE_SZ> in_page{&in_buffer[0]};
  std::vector<std::byte> out_buffer{OUT_PAGE_SZ, std::byte{0}};
  cas::MemoryPage<OUT_PAGE_SZ> out_page{&out_buffer[0]};

  // some stats
  size_t nr_keys  = 0;
  size_t nr_pages = 0;

  // process input
  while (g_signal_status == 0) {
    size_t bytes_read = fread(in_page.Data(), 1, IN_PAGE_SZ, infile);
    if (bytes_read < IN_PAGE_SZ) {
      break;
    }
    for (const auto& bkey : in_page) {
      if (bkey.ByteSize() > out_page.FreeSpace()) {
        // write page to stdout since it is full
        std::fwrite(out_page.Data(), 1, OUT_PAGE_SZ, stdout);
        out_page.Reset();
        ++nr_pages;
      }
      out_page.Push(bkey);
      ++nr_keys;
    }
  }

  // flush last page to stdout
  std::fwrite(out_page.Data(), 1, OUT_PAGE_SZ, stdout);
  ++nr_pages;

  // write statistics to stderr
  std::cerr << "nr_keys:  " << nr_keys << "\n";
  std::cerr << "nr_pages: " << nr_pages << "\n";
  std::cerr << std::flush;
}


int main_(int argc, char** argv) {
  signal(SIGINT, signal_handler);

  size_t in_page_sz = 0;
  size_t out_page_sz = 0;
  std::string input_filename = "";
  bool read_from_file = false;

  // read page size
  if (argc < 2) {
    std::cerr << "page sizes missing\n";
    return 1;
  } else {
    if (1 != std::sscanf(argv[1], "%zu", &in_page_sz)) {
      std::cerr << "First parameter: in page size (integer)\n";
      return 1;
    }
    if (1 != std::sscanf(argv[2], "%zu", &out_page_sz)) {
      std::cerr << "Second parameter: out page size (integer)\n";
      return 1;
    }
    if (in_page_sz % 4096 != 0 || out_page_sz % 4096 != 0) {
      std::cerr << "Page size must be a multiple of 4096\n";
      return 1;
    }
    if (in_page_sz == out_page_sz) {
      std::cerr << "In and out page sizes must be different\n";
      return 1;
    }
  }
  // check if input is coming from stdin or file
  if (argc >= 4) {
    read_from_file = true;
    input_filename = std::string{argv[3]};
  }

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

  // open stdout for binary writing
  if (freopen(nullptr, "wb", stdout) == nullptr) {
    std::cerr << "Cannot open stdout for binary writing\n";
    return 1;
  }

  if (in_page_sz == cas::PAGE_SZ_4KB) {
    if      (out_page_sz == cas::PAGE_SZ_8KB)  {Repartition<cas::PAGE_SZ_4KB, cas::PAGE_SZ_8KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_16KB) {Repartition<cas::PAGE_SZ_4KB, cas::PAGE_SZ_16KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_32KB) {Repartition<cas::PAGE_SZ_4KB, cas::PAGE_SZ_32KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_64KB) {Repartition<cas::PAGE_SZ_4KB, cas::PAGE_SZ_64KB>(infile);}
  }
  else if (in_page_sz == cas::PAGE_SZ_8KB) {
    if      (out_page_sz == cas::PAGE_SZ_4KB)  {Repartition<cas::PAGE_SZ_8KB, cas::PAGE_SZ_4KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_16KB) {Repartition<cas::PAGE_SZ_8KB, cas::PAGE_SZ_16KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_32KB) {Repartition<cas::PAGE_SZ_8KB, cas::PAGE_SZ_32KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_64KB) {Repartition<cas::PAGE_SZ_8KB, cas::PAGE_SZ_64KB>(infile);}
  }
  else if (in_page_sz == cas::PAGE_SZ_16KB) {
    if      (out_page_sz == cas::PAGE_SZ_4KB)  {Repartition<cas::PAGE_SZ_16KB, cas::PAGE_SZ_4KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_8KB)  {Repartition<cas::PAGE_SZ_16KB, cas::PAGE_SZ_8KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_32KB) {Repartition<cas::PAGE_SZ_16KB, cas::PAGE_SZ_32KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_64KB) {Repartition<cas::PAGE_SZ_16KB, cas::PAGE_SZ_64KB>(infile);}
  }
  else if (in_page_sz == cas::PAGE_SZ_32KB) {
    if      (out_page_sz == cas::PAGE_SZ_4KB)  {Repartition<cas::PAGE_SZ_32KB, cas::PAGE_SZ_4KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_8KB)  {Repartition<cas::PAGE_SZ_32KB, cas::PAGE_SZ_8KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_16KB) {Repartition<cas::PAGE_SZ_32KB, cas::PAGE_SZ_16KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_64KB) {Repartition<cas::PAGE_SZ_32KB, cas::PAGE_SZ_64KB>(infile);}
  }
  else if (in_page_sz == cas::PAGE_SZ_64KB) {
    if      (out_page_sz == cas::PAGE_SZ_4KB)  {Repartition<cas::PAGE_SZ_16KB, cas::PAGE_SZ_4KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_8KB)  {Repartition<cas::PAGE_SZ_16KB, cas::PAGE_SZ_8KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_16KB) {Repartition<cas::PAGE_SZ_16KB, cas::PAGE_SZ_16KB>(infile);}
    else if (out_page_sz == cas::PAGE_SZ_32KB) {Repartition<cas::PAGE_SZ_16KB, cas::PAGE_SZ_32KB>(infile);}
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
