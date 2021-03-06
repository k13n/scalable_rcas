#include "cas/query_executor.hpp"
#include "cas/node_reader.hpp"
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


cas::QueryExecutor::QueryExecutor(const std::string& idx_filename)
  : idx_filename_(idx_filename)
{}


cas::QueryStats cas::QueryExecutor::Execute(
    const BinarySK& key,
    const BinaryKeyEmitter& emitter) {
  int fd = open(idx_filename_.c_str(), O_RDONLY, S_IRUSR);
  if (fd == -1) {
    std::string error_msg = "failed to open file '" + idx_filename_ + "'";
    throw std::runtime_error{error_msg};
  }
  size_t file_size = std::filesystem::file_size(idx_filename_);

  auto* file = static_cast<uint8_t*>(mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
  if (file == MAP_FAILED) {
    std::string error_msg = "mmap of file '" + idx_filename_ + "' failed";
    throw std::runtime_error{error_msg};
  }
  cas::NodeReader root{file, 0};

  cas::Query query{&root, key, emitter};
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
