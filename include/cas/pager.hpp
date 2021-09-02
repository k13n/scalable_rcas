#pragma once

#include "cas/types.hpp"
#include <fstream>
#include <memory>

namespace cas {


using IdxPage = std::array<std::byte, cas::PAGE_SZ>;



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

  inline std::shared_ptr<IdxPage> NewIdxPage() const {
    return std::make_shared<IdxPage>();
  }

  std::shared_ptr<IdxPage> Fetch(cas::page_nr_t page_nr);

  void Write(const IdxPage& page, cas::page_nr_t page_nr);

  void Write(uint8_t* src, size_t size, size_t offset);

private:
  void OpenFile();
  void CloseFile();
};

} // namespace cas
