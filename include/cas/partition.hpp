#pragma once

#include "cas/context.hpp"
#include "cas/memory_page.hpp"
#include "cas/memory_pool.hpp"
#include "cas/bulk_loader_stats.hpp"
#include "cas/types.hpp"
#include <fstream>
#include <memory>
#include <forward_list>

namespace cas {

template<size_t PAGE_SZ>
class Partition {
  /* core data, see paper */
  int dsc_p_;
  int dsc_v_;
  std::forward_list<MemoryPage<PAGE_SZ>> mptr_;
  int fptr_ = -1;
  /* file meta information */
  std::string filename_;
  size_t fptr_write_page_nr_ = 0;
  /* record statistics */
  size_t nr_keys_ = 0;
  size_t nr_pages_ = 0;
  size_t nr_memory_pages_ = 0;
  size_t nr_disk_pages_ = 0;
  BulkLoaderStats& stats_;
  const Context& context_;
  /* needed only for the root partition */
  size_t fptr_cursor_first_page_nr_ = 0;
  size_t fptr_cursor_last_page_nr_ = std::numeric_limits<std::size_t>::max();
  bool is_root_partition_ = false;

public:
  explicit Partition(const std::string& filename, BulkLoaderStats& stats,
      const Context& context);
  ~Partition() = default;

  /* delete copy/move constructors/assignments */
  Partition(const Partition& other) = delete;
  Partition(const Partition&& other) = delete;
  Partition& operator=(const Partition& other) = delete;
  Partition& operator=(Partition&& other) = delete;

  bool HasMemoryPage();
  void PushToMemory(MemoryPage<PAGE_SZ>&& page);
  MemoryPage<PAGE_SZ> PopFromMemory();
  void PushToDisk(const MemoryPage<PAGE_SZ>& page);

  /* Getters/Setters */
  void DscP(int dsc_p) { dsc_p_ = dsc_p; }
  void DscV(int dsc_v) { dsc_v_ = dsc_v; }
  int DscP() const { return dsc_p_; }
  int DscV() const { return dsc_v_; }
  void FptrCursorFirstPageNr(size_t val) {
    fptr_cursor_first_page_nr_ = val;
  }
  void FptrCursorLastPageNr(size_t val) {
    fptr_cursor_last_page_nr_ = val;
  }
  void IsRootPartition(bool is_root_partition) {
    is_root_partition_ = is_root_partition;
  }
  bool IsRootPartition() const {
    return is_root_partition_;
  }
  BulkLoaderStats& Stats() const {
    return stats_;
  }

  inline size_t& NrKeys() { return nr_keys_; }
  inline size_t& NrPages() { return nr_pages_; }
  inline size_t& NrMemoryPages() { return nr_memory_pages_; }
  inline size_t& NrDiskPages() { return nr_disk_pages_; }
  inline const std::string& Filename() const { return filename_; }

  void Close() { CloseFile(); }
  void DeleteFile();
  void Dump();
  void DumpDetailed(MemoryPage<PAGE_SZ>& io_page);


public:
  class Cursor {
    Partition<PAGE_SZ>& p_;
    typename std::forward_list<MemoryPage<PAGE_SZ>>::iterator mptr_it_;
    typename std::forward_list<MemoryPage<PAGE_SZ>>::iterator mptr_it_prev_;
    MemoryPage<PAGE_SZ>& io_page_{nullptr};
    size_t fptr_read_page_nr_;
    size_t fptr_last_page_nr_;

  public:
    Cursor(Partition<PAGE_SZ>& partition,
        MemoryPage<PAGE_SZ>& io_page,
        size_t fptr_read_page_nr,
        size_t fptr_last_page_nr);

    bool HasNext();
    bool FetchNextDiskPage();
    MemoryPage<PAGE_SZ>& NextPage();
    MemoryPage<PAGE_SZ> RemoveNextPage();
  };

  Cursor Cursor(MemoryPage<PAGE_SZ>& io_page) {
    return {
      *this,
      io_page,
      fptr_cursor_first_page_nr_,
      fptr_cursor_last_page_nr_
    };
  }

  bool IsMemoryOnly() const;
  bool IsDiskOnly() const;
  bool IsHybrid() const;


private:
  void OpenFile();
  void CloseFile();
  int FWritePage(const MemoryPage<PAGE_SZ>& page, size_t page_nr);
  int FReadPage(MemoryPage<PAGE_SZ>& page, size_t page_nr);
};

} // namespace cas
