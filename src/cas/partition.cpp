#include "cas/partition.hpp"
#include "cas/util.hpp"
#include <iostream>
#include <iterator>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>


template<size_t PAGE_SZ>
cas::Partition<PAGE_SZ>::Partition(const std::string& filename,
    BulkLoaderStats& stats, const cas::Context& context)
  : filename_(filename)
  , stats_(stats)
  , context_(context)
{}



template<size_t PAGE_SZ>
bool cas::Partition<PAGE_SZ>::HasMemoryPage() {
  return !mptr_.empty();
}


template<size_t PAGE_SZ>
cas::MemoryPage<PAGE_SZ> cas::Partition<PAGE_SZ>::PopFromMemory() {
  --nr_memory_pages_;
  auto page = std::move(mptr_.front());
  mptr_.pop_front();
  return page;
}


template<size_t PAGE_SZ>
void cas::Partition<PAGE_SZ>::PushToMemory(
    cas::MemoryPage<PAGE_SZ>&& page) {
  ++nr_memory_pages_;
  mptr_.push_front(std::move(page));
}


template<size_t PAGE_SZ>
void cas::Partition<PAGE_SZ>::PushToDisk(
    const MemoryPage<PAGE_SZ>& page) {
  ++nr_disk_pages_;
  if (fptr_ == -1) {
    OpenFile();
  }
  int rt = FWritePage(page, fptr_write_page_nr_);
  if (rt == 0) {
    ++fptr_write_page_nr_;
  } else {
    std::string error_msg = "failed to write to file '" + filename_
      + "' at page: " + std::to_string(fptr_write_page_nr_);
    throw std::runtime_error{error_msg};
  }
}


template<size_t PAGE_SZ>
void cas::Partition<PAGE_SZ>::OpenFile() {
  if (!std::filesystem::exists(filename_)) {
    ++stats_.files_created_;
  }
  int flags = O_CREAT | O_RDWR;
  if (context_.use_direct_io_) {
    flags |= O_DIRECT;
  }
  fptr_ = open(filename_.c_str(), flags, 0666);
  if (fptr_ == -1) {
    std::string error_msg = "failed to open file '" + filename_ + "'";
    throw std::runtime_error{error_msg};
  }
}


template<size_t PAGE_SZ>
void cas::Partition<PAGE_SZ>::CloseFile() {
  if (fptr_ == -1) {
    return;
  }
  // flushing data to disk
  auto start = std::chrono::high_resolution_clock::now();
  fsync(fptr_);
  cas::util::AddToTimer(stats_.runtime_partition_disk_write_, start);
  // closing file
  if (close(fptr_) == -1) {
    throw std::runtime_error{"error while closing file '" + filename_ + "'"};
  } else {
    fptr_ = -1;
  }
}


template<size_t PAGE_SZ>
void cas::Partition<PAGE_SZ>::DeleteFile() {
  CloseFile();
  std::filesystem::remove(filename_);
}


template<size_t PAGE_SZ>
int cas::Partition<PAGE_SZ>::FWritePage(
    const MemoryPage<PAGE_SZ>& page,
    size_t page_nr) {
  auto start = std::chrono::high_resolution_clock::now();
  int err;

  int bytes_written = pwrite(fptr_, page.Data(), PAGE_SZ, page_nr * PAGE_SZ);
  if (bytes_written == PAGE_SZ) {
    stats_.partition_bytes_written_ += PAGE_SZ;
    ++stats_.mem_pages_written_;
    err = 0;
  } else {
    err = -1;
  }

  cas::util::AddToTimer(stats_.runtime_partition_disk_write_, start);
  return err;
}


template<size_t PAGE_SZ>
int cas::Partition<PAGE_SZ>::FReadPage(
    MemoryPage<PAGE_SZ>& page,
    size_t page_nr) {
  auto start = std::chrono::high_resolution_clock::now();
  int err;

  int bytes_read = pread(fptr_, page.Data(), PAGE_SZ, page_nr * PAGE_SZ);
  if (bytes_read == PAGE_SZ) {
    stats_.partition_bytes_read_ += PAGE_SZ;
    ++stats_.mem_pages_read_;
    err = 0;
  } else {
    err = -1;
  }

  cas::util::AddToTimer(stats_.runtime_partition_disk_read_, start);
  return err;
}



template<size_t PAGE_SZ>
cas::Partition<PAGE_SZ>::Cursor::Cursor(
        cas::Partition<PAGE_SZ>& partition,
        MemoryPage<PAGE_SZ>& io_page,
        size_t fptr_read_page_nr,
        size_t fptr_last_page_nr)
  : p_(partition)
  , mptr_it_(p_.mptr_.begin())
  , mptr_it_prev_(p_.mptr_.before_begin())
  , io_page_(io_page)
  , fptr_read_page_nr_(fptr_read_page_nr)
  , fptr_last_page_nr_(fptr_last_page_nr)
{
  if (std::filesystem::exists(p_.filename_) && p_.fptr_ == -1) {
    p_.OpenFile();
  }
}


template<size_t PAGE_SZ>
bool cas::Partition<PAGE_SZ>::Cursor::HasNext() {
  if (mptr_it_ != p_.mptr_.end()) {
    return true;
  }
  return FetchNextDiskPage();
}


template<size_t PAGE_SZ>
bool cas::Partition<PAGE_SZ>::Cursor::FetchNextDiskPage() {
  if (p_.fptr_ != -1 && fptr_read_page_nr_ < fptr_last_page_nr_) {
    int rt = p_.FReadPage(io_page_, fptr_read_page_nr_);
    if (rt == 0) {
      ++fptr_read_page_nr_;
      return true;
    }
  }
  return false;
}


template<size_t PAGE_SZ>
cas::MemoryPage<PAGE_SZ>& cas::Partition<PAGE_SZ>::Cursor::NextPage() {
  if (mptr_it_ != p_.mptr_.end()) {
    auto& page = *mptr_it_;
    ++mptr_it_;
    ++mptr_it_prev_;
    return page;
  } else {
    // the io_page_ was already fetched in HasNext()
    return io_page_;
  }
}


 template<size_t PAGE_SZ>
 cas::MemoryPage<PAGE_SZ> cas::Partition<PAGE_SZ>::Cursor::RemoveNextPage() {
   if (mptr_it_ != p_.mptr_.end()) {
     auto page = std::move(*mptr_it_);
     mptr_it_++;
     p_.mptr_.erase_after(mptr_it_prev_);
     return page;
   } else {
     // the io_page_ was already fetched in HasNext()
     return std::move(io_page_);
   }
 }


template<size_t PAGE_SZ>
void cas::Partition<PAGE_SZ>::Dump() {
  std::cout << "DscP: " << dsc_p_;
  std::cout << "\nDscV: " << dsc_v_;
  std::cout << "\nmptr: " << (mptr_.empty()   ? "<nil>" : "<exists>");
  std::cout << "\nfptr: " << (fptr_ != -1 ? "<exists>" : "<nil>");
  std::cout << "\n";
}


template<size_t PAGE_SZ>
void cas::Partition<PAGE_SZ>::DumpDetailed(cas::MemoryPage<PAGE_SZ>& io_page) {
  Dump();
  auto cursor = Cursor(io_page);
  while (cursor.HasNext()) {
    auto& page = cursor.NextPage();
    for (auto key : page) {
      key.Dump();
    }
  }
}


template<size_t PAGE_SZ>
bool cas::Partition<PAGE_SZ>::IsMemoryOnly() const {
  return !mptr_.empty() && !std::filesystem::exists(filename_);
}


template<size_t PAGE_SZ>
bool cas::Partition<PAGE_SZ>::IsDiskOnly() const {
  return mptr_.empty() && std::filesystem::exists(filename_);
}


template<size_t PAGE_SZ>
bool cas::Partition<PAGE_SZ>::IsHybrid() const {
  return !mptr_.empty() && std::filesystem::exists(filename_);
}


template class cas::Partition<cas::PAGE_SZ_64KB>;
template class cas::Partition<cas::PAGE_SZ_32KB>;
template class cas::Partition<cas::PAGE_SZ_16KB>;
template class cas::Partition<cas::PAGE_SZ_8KB>;
template class cas::Partition<cas::PAGE_SZ_4KB>;
