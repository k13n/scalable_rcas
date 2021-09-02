#include "cas/pager.hpp"

#include <filesystem>
#include <iostream>
#include <memory>
#include <vector>


cas::Pager::Pager(const std::string& filename)
  : filename_(filename) {
  OpenFile();
}


cas::Pager::~Pager() {
  CloseFile();
}


void cas::Pager::OpenFile() {
  if (!std::filesystem::exists(filename_)) {
    file_.open(filename_, std::ios::app);
    file_.close();
  }
  file_.open(filename_, std::ios::in | std::ios::out | std::ios::binary);
  if (!file_.is_open()) {
    throw std::runtime_error{"failed to open file: " + filename_};
  }
}


void cas::Pager::CloseFile() {
  if (file_.is_open()) {
    try {
      file_.close();
    } catch (std::exception& e) {
      std::cerr << "error while closing file\n";
    }
  }
}


void cas::Pager::Clear() {
  CloseFile();
  std::filesystem::remove(filename_);
  OpenFile();
}


std::shared_ptr<cas::IdxPage>
cas::Pager::Fetch(cas::page_nr_t page_nr) {
  auto page = NewIdxPage();
  file_.seekg(page_nr * cas::PAGE_SZ);
  file_.read(reinterpret_cast<char*>(&page->front()), cas::PAGE_SZ); // NOLINT
  if (!file_.good()) {
    throw std::runtime_error{"failed reading page " + std::to_string(page_nr)};
  }
  return page;
}


void cas::Pager::Write(const IdxPage& page, cas::page_nr_t page_nr) {
  file_.seekp(page_nr * cas::PAGE_SZ);
  file_.write(reinterpret_cast<const char*>(&page.front()), cas::PAGE_SZ); // NOLINT
  if (!file_.good()) {
    throw std::runtime_error{"failed writing page " + std::to_string(page_nr)};
  }
}


void cas::Pager::Write(uint8_t* src, size_t size, size_t offset) {
  file_.seekp(offset);
  file_.write(reinterpret_cast<const char*>(src), size); // NOLINT
  if (!file_.good()) {
    throw std::runtime_error{"failed writing content " + std::to_string(offset)};
  }
}
