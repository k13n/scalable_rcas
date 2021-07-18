#pragma once

#include "cas/types.hpp"
#include <fstream>
#include <memory>

namespace cas {


template<size_t PAGE_SZ>
using IdxPage = std::array<std::byte, PAGE_SZ>;



template<size_t PAGE_SZ>
class Pager {
  const std::string filename_;
  std::fstream file_;

public:

  explicit Pager(const std::string& filename);
  ~Pager();

  // delete copy/move constructors/assignments
  Pager(const Pager& other) = delete;
  Pager(const Pager&& other) = delete;
  Pager& operator=(const Pager& other) = delete;
  Pager& operator=(Pager&& other) = delete;

  void Clear();

  inline std::shared_ptr<IdxPage<PAGE_SZ>> NewIdxPage() const {
    return std::make_shared<IdxPage<PAGE_SZ>>();
  }

  std::shared_ptr<IdxPage<PAGE_SZ>> Fetch(cas::page_nr_t page_nr);

  void Write(const IdxPage<PAGE_SZ>& page, cas::page_nr_t page_nr);

  void Write(uint8_t* src, size_t size, size_t offset);

private:
  void OpenFile();
  void CloseFile();
};

} // namespace cas
