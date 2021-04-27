#include "cas/pager.hpp"

#include <filesystem>
#include <iostream>
#include <memory>
#include <vector>


template<size_t PAGE_SZ>
cas::Pager<PAGE_SZ>::Pager(const std::string& filename)
  : filename_(filename) {
  OpenFile();
}


template<size_t PAGE_SZ>
cas::Pager<PAGE_SZ>::~Pager() {
  CloseFile();
}


template<size_t PAGE_SZ>
void cas::Pager<PAGE_SZ>::OpenFile() {
  if (!std::filesystem::exists(filename_)) {
    file_.open(filename_, std::ios::app);
    file_.close();
  }
  file_.open(filename_, std::ios::in | std::ios::out | std::ios::binary);
  if (!file_.is_open()) {
    throw std::runtime_error{"failed to open file: " + filename_};
  }
}


template<size_t PAGE_SZ>
void cas::Pager<PAGE_SZ>::CloseFile() {
  if (file_.is_open()) {
    try {
      file_.close();
    } catch (std::exception& e) {
      std::cerr << "error while closing file\n";
    }
  }
}


template<size_t PAGE_SZ>
void cas::Pager<PAGE_SZ>::Clear() {
  CloseFile();
  std::filesystem::remove(filename_);
  OpenFile();
}


template<size_t PAGE_SZ>
std::shared_ptr<cas::IdxPage<PAGE_SZ>>
cas::Pager<PAGE_SZ>::Fetch(cas::page_nr_t page_nr) {
  auto page = NewIdxPage();
  file_.seekg(page_nr * PAGE_SZ);
  file_.read(reinterpret_cast<char*>(&page->front()), PAGE_SZ); // NOLINT
  if (!file_.good()) {
    throw std::runtime_error{"failed reading page " + std::to_string(page_nr)};
  }
  return page;
}


template<size_t PAGE_SZ>
void cas::Pager<PAGE_SZ>::Write(const IdxPage<PAGE_SZ>& page, cas::page_nr_t page_nr) {
  file_.seekp(page_nr * PAGE_SZ);
  file_.write(reinterpret_cast<const char*>(&page.front()), PAGE_SZ); // NOLINT
  if (!file_.good()) {
    throw std::runtime_error{"failed writing page " + std::to_string(page_nr)};
  }
}


template class cas::Pager<cas::PAGE_SZ_64KB>;
template class cas::Pager<cas::PAGE_SZ_32KB>;
template class cas::Pager<cas::PAGE_SZ_16KB>;
template class cas::Pager<cas::PAGE_SZ_8KB>;
template class cas::Pager<cas::PAGE_SZ_4KB>;
