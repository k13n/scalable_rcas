#include "cas/query_executor.hpp"
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


template<class VType>
cas::QueryExecutor<VType>::QueryExecutor(const std::string& idx_filename)
  : idx_filename_(idx_filename)
{}


template<class VType>
cas::QueryStats cas::QueryExecutor<VType>::Execute(
    const BinarySK& key,
    const BinaryKeyEmitter& emitter) {
  int fd = open(idx_filename_.c_str(), O_RDONLY, S_IRUSR);
  if (fd == -1) {
    std::string error_msg = "failed to open file '" + idx_filename_ + "'";
    throw std::runtime_error{error_msg};
  }
  size_t file_size = std::filesystem::file_size(idx_filename_);

  auto* file = static_cast<uint8_t*>(mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
  cas::Query<VType> query{file, key, emitter};
  query.Execute();

  int rt = munmap(file, file_size);
  if (rt < 0) {
    std::cerr << "could not munmap allocated memory" << std::endl;
    std::terminate();
  }
  if (close(fd) == -1) {
    throw std::runtime_error{"error while closing file '" + idx_filename_ + "'"};
  }

  return query.Stats();
}


// explicit instantiations to separate header from implementation
template class cas::QueryExecutor<cas::vint64_t>;
