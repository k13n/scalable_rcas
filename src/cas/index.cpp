#include "cas/index.hpp"
#include "cas/query.hpp"
#include "cas/key_encoder.hpp"
#include "cas/key_decoder.hpp"
#include "cas/bulk_loader.hpp"
#include "cas/query_executor.hpp"
#include "cas/mem/insertion.hpp"
#include "cas/util.hpp"
#include <filesystem>
#include <random>


template<class VType>
void cas::Index<VType>::Insert(cas::BinaryKey key) {
  cas::mem::Insertion insertion{&root_, key, context_.partitioning_threshold_};
  auto start = std::chrono::high_resolution_clock::now();
  insertion.Execute();
  cas::util::AddToTimer(stats_.runtime_insertion_, start);
  cas::util::AddToTimer(stats_.runtime_, start);
  ++nr_memory_keys_;
  if (nr_memory_keys_ >= context_.max_memory_keys_) {
    HandleOverflow();
  }
}


template<class VType>
void cas::Index<VType>::HandleOverflow() {
  if (nr_memory_keys_ == 0) {
    return;
  }

  auto start = std::chrono::high_resolution_clock::now();

  // create root partition with random name
  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_int_distribution<std::mt19937::result_type> dist(1,1'000'000'000);
  std::string partition_file = context_.partition_folder_
    + "tmp_root_partition_"
    + std::to_string(dist(rng));;

  Partition partition{partition_file, stats_, context_};
  partition.IsRootPartition(true);

  // data needed to compute discriminative bytes of the new partition
  cas::MemoryKey ref_key;
  int dsc_p = 0;
  int dsc_v = 0;
  bool first_key = true;

  // create a function to store keys in the partition
  std::vector<std::byte> key_buffer;
  key_buffer.resize(cas::PAGE_SZ);
  std::vector<std::byte> page_buffer;
  page_buffer.resize(cas::PAGE_SZ);
  cas::MemoryPage io_page{&page_buffer[0]};
  const auto emitter = [&](
          const cas::QueryBuffer& path, size_t p_len,
          const cas::QueryBuffer& value, size_t v_len,
          cas::ref_t ref) -> void {
    if (first_key) {
      ref_key.ref_ = ref;
      ref_key.path_.resize(p_len);
      ref_key.value_.resize(v_len);
      std::memcpy(&ref_key.path_[0], &path[0], p_len);
      std::memcpy(&ref_key.value_[0], &value[0], v_len);
      dsc_p = p_len;
      dsc_v = v_len;
      first_key = false;
    }
    // create temporary key
    cas::BinaryKey tmp_key{&key_buffer[0]};
    tmp_key.LenPath(static_cast<uint16_t>(p_len));
    tmp_key.LenValue(static_cast<uint16_t>(v_len));
    std::memcpy(tmp_key.Path(), &path[0], p_len);
    std::memcpy(tmp_key.Value(), &value[0], v_len);
    tmp_key.Ref(ref);
    // proactively compute dsc bytes
    int gP = 0, gV = 0;
    while (gP < dsc_p && ref_key.path_[gP]  == static_cast<uint8_t>(tmp_key.Path()[gP]))  { ++gP; }
    while (gV < dsc_v && ref_key.value_[gV] == static_cast<uint8_t>(tmp_key.Value()[gV])) { ++gV; }
    dsc_p = gP;
    dsc_v = gV;
    // add key to io_page
    if (tmp_key.ByteSize() > io_page.FreeSpace()) {
      partition.PushToDisk(io_page);
      ++partition.NrPages();
      io_page.Reset();
    }
    io_page.Push(tmp_key);
  };

  // create query that matches all keys
  std::string path = "/**";
  VType low  = cas::VINT64_MIN;
  VType high = cas::VINT64_MAX;
  cas::SearchKey<VType> skey{path, low, high};
  auto search_key = cas::KeyEncoder<VType>::Encode(skey, false);

  // query the in-memory index
  cas::Query query{root_, search_key, emitter};
  query.Execute();

  // look for the first index that does not exist
  int idx_number = 0;
  while (true) {
    std::string filename = context_.pipeline_dir_ + "/index.bin" + std::to_string(idx_number);
    if (!std::filesystem::exists(filename)) {
      break;
    }
    ++idx_number;
  }

  // collect keys in all indexes before the idx_number
  for (int i = 0; i < idx_number; ++i) {
    std::string filename = context_.pipeline_dir_ + "/index.bin" + std::to_string(i);
    cas::QueryExecutor query{filename};
    query.Execute(search_key, emitter);
    stats_.index_bytes_read_ += std::filesystem::file_size(filename);
  }

  // flush dirty page to disk
  partition.PushToDisk(io_page);

  // take runtime of collecting keys
  cas::util::AddToTimer(stats_.runtime_collect_keys_, start);
  cas::util::AddToTimer(stats_.runtime_, start);

  // bulk-load new index for idx_number
  cas::Context context_copy = context_;
  context_copy.index_file_ = context_.pipeline_dir_ + "/index.bin" + std::to_string(idx_number);
  context_copy.dataset_size_ = 0;
  context_copy.use_root_dsc_bytes_ = true;
  context_copy.root_dsc_P_ = dsc_p;
  context_copy.root_dsc_V_ = dsc_v;
  context_copy.delete_root_partition_ = true;
  cas::BulkLoader<VType> bulk_loader{context_copy, stats_};
  bulk_loader.Load(partition);

  // delete in-memory index
  start = std::chrono::high_resolution_clock::now();
  DeleteNodesRecursively(root_);
	root_ = nullptr;
  nr_memory_keys_ = 0;

  // delete disk-based indexes < idx_number
  for (int i = 0; i < idx_number; ++i) {
    std::string filename = context_.pipeline_dir_ + "/index.bin" + std::to_string(i);
    std::filesystem::remove(filename);
  }
  cas::util::AddToTimer(stats_.runtime_, start);
}


template<class VType>
void cas::Index<VType>::BulkLoad() {
  ClearPipelineFiles();
  Context context_copy = context_;

  // create index file with random name
  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_int_distribution<std::mt19937::result_type> dist(1,1'000'000'000);
  context_copy.index_file_ = context_copy.pipeline_dir_
    + "tmp_index_"
    + std::to_string(dist(rng));;

  // bulk-load index
  cas::BulkLoader<VType> bulk_loader{context_copy, stats_};
  stats_.nr_input_keys_ = 0;
  bulk_loader.Load();

  // change the name of the index file
  int k = std::ceil(std::log2(
        stats_.nr_input_keys_ /
        static_cast<double>(context_copy.max_memory_keys_)));
  std::string new_index_file = context_copy.pipeline_dir_
    + "index.bin"
    + std::to_string(k);
  std::filesystem::rename(context_copy.index_file_, new_index_file);
}


template<class VType>
cas::QueryStats cas::Index<VType>::Query(
    const cas::SearchKey<VType>& key,
    const cas::BinaryKeyEmitter emitter)
{
  bool reversed = false;
  auto search_key = cas::KeyEncoder<VType>::Encode(key, reversed);
  return Query(search_key, emitter);
}


template<class VType>
cas::QueryStats cas::Index<VType>::Query(
    const cas::BinarySK& key,
    const cas::BinaryKeyEmitter emitter)
{
  std::vector<cas::QueryStats> stats;

  // query the in-memory index
  if (root_ != nullptr) {
    cas::Query query{root_, key, emitter};
    query.Execute();
    stats.push_back(query.Stats());
  }

  // check if the the pipeline folder exists
  if (!std::filesystem::is_directory(context_.pipeline_dir_) ||
      !std::filesystem::exists(context_.pipeline_dir_)) {
    return cas::QueryStats::Sum(stats);
  }

  // query every disk-based index
  for (const auto& entry : std::filesystem::directory_iterator(context_.pipeline_dir_)) {
    if (entry.is_regular_file()) {
      cas::QueryExecutor query{entry.path().string()};
      stats.push_back(query.Execute(key, emitter));
    }
  }

  return cas::QueryStats::Sum(stats);
}




template<class VType>
void cas::Index<VType>::DeleteNodesRecursively(cas::INode* node) {
  if (node == nullptr) {
    return;
  }
  node->ForEachChild([&](uint8_t /* byte  */, cas::INode* child) {
    DeleteNodesRecursively(child);
  });
  delete node;
}


template<class VType>
void cas::Index<VType>::ClearPipelineFiles() {
  // create the partition folder if it doesn't exist
  if (!std::filesystem::is_directory(context_.partition_folder_) ||
      !std::filesystem::exists(context_.partition_folder_)) {
    std::filesystem::create_directory(context_.partition_folder_);
  }
  // delete all existing partition files on disk
  for (const auto& entry : std::filesystem::directory_iterator(context_.partition_folder_)) {
    std::filesystem::remove_all(entry);
  }

  // create the pipeline folder if it doesn't exist
  if (!std::filesystem::is_directory(context_.pipeline_dir_) ||
      !std::filesystem::exists(context_.pipeline_dir_)) {
    std::filesystem::create_directory(context_.pipeline_dir_);
  }
  // delete all existing index files on disk
  for (const auto& entry : std::filesystem::directory_iterator(context_.pipeline_dir_)) {
    std::filesystem::remove_all(entry);
  }
}


template class cas::Index<cas::vint64_t>;
