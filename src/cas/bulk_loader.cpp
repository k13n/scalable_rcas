#include "cas/bulk_loader.hpp"
#include "cas/node_reader.hpp"
#include "cas/key_encoder.hpp"
#include "cas/util.hpp"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>


template<class VType>
cas::BulkLoader<VType>::BulkLoader(
      const cas::Context& context,
      cas::BulkLoaderStats& stats
    )
  : context_{context}
  , stats_{stats}
  , mpool_(MemoryPools::Construct(context.mem_size_bytes_, context.mem_capacity_bytes_))
  , pager_(context.index_file_)
  , shortened_key_buffer_(std::make_unique<std::array<std::byte, cas::PAGE_SZ>>())
  , serialization_buffer_(std::make_unique<std::array<uint8_t, 10'000'000>>())
{
  for (int b = 0; b <= 0xFF; ++b) {
    ref_keys_[b] = std::make_unique<std::array<std::byte, cas::PAGE_SZ>>();
  }
}


template<class VType>
void cas::BulkLoader<VType>::Load() {
  start_time_global = std::chrono::high_resolution_clock::now();

  // create the partition folder if it doesn't exist
  if (!std::filesystem::is_directory(context_.partition_folder_) ||
      !std::filesystem::exists(context_.partition_folder_)) {
    std::filesystem::create_directory(context_.partition_folder_);
  }

  // delete all existing partition files on disk
  for (const auto& entry : std::filesystem::directory_iterator(context_.partition_folder_)) {
    std::filesystem::remove_all(entry);
  }
  // delete the index file if it already exists
  pager_.Clear();

  // Initialize the root partition
  cas::Partition partition{context_.input_filename_, stats_, context_};
  InitializeRootPartition(partition);

  // Compute the root's discriminative byte
  auto start_time_dsc = std::chrono::high_resolution_clock::now();
  if (context_.use_root_dsc_bytes_) {
    partition.DscP(context_.root_dsc_P_);
    partition.DscV(context_.root_dsc_V_);
  } else {
    switch (context_.dsc_computation_) {
      case cas::DscComputation::Proactive:
      case cas::DscComputation::TwoPass:
        DscByte(partition);
        break;
      case cas::DscComputation::ByteByByte:
        DscByteByByte(partition);
        break;
      default:
        throw std::runtime_error{"unknown DscComputation"};
    }
  }
  cas::util::AddToTimer(stats_.runtime_dsc_computation_, start_time_dsc);

  // check that the input/output pages are fully available
  assert(mpool_.input_.Full());
  assert(mpool_.output_.Full());

  // time it takes to prepare the root partition
  cas::util::AddToTimer(stats_.runtime_root_partition_, start_time_global);

  // construct the index
  auto construct_start = std::chrono::high_resolution_clock::now();
  Construct(partition, cas::Dimension::VALUE, cas::Dimension::LEAF, 0, 0);
  cas::util::AddToTimer(stats_.runtime_construction_, construct_start);

  cas::util::AddToTimer(stats_.runtime_, start_time_global);
  stats_.index_bytes_written_ += std::filesystem::file_size(context_.index_file_);
  ++stats_.nr_bulkloads_;

  pager_.Close();
}



template<class VType>
void cas::BulkLoader<VType>::Load(cas::Partition& partition) {
  start_time_global = std::chrono::high_resolution_clock::now();

  // delete the index file if it already exists
  pager_.Clear();

  // Compute the root's discriminative byte
  auto start_time_dsc = std::chrono::high_resolution_clock::now();
  if (context_.use_root_dsc_bytes_) {
    partition.DscP(context_.root_dsc_P_);
    partition.DscV(context_.root_dsc_V_);
  } else {
    switch (context_.dsc_computation_) {
      case cas::DscComputation::Proactive:
      case cas::DscComputation::TwoPass:
        DscByte(partition);
        break;
      case cas::DscComputation::ByteByByte:
        DscByteByByte(partition);
        break;
      default:
        throw std::runtime_error{"unknown DscComputation"};
    }
  }
  cas::util::AddToTimer(stats_.runtime_dsc_computation_, start_time_dsc);

  // check that the input/output pages are fully available
  assert(mpool_.input_.Full());
  assert(mpool_.output_.Full());

  // time it takes to prepare the root partition
  cas::util::AddToTimer(stats_.runtime_root_partition_, start_time_global);

  // construct the index
  auto construct_start = std::chrono::high_resolution_clock::now();
  Construct(partition, cas::Dimension::VALUE, cas::Dimension::LEAF, 0, 0);
  cas::util::AddToTimer(stats_.runtime_construction_, construct_start);

  cas::util::AddToTimer(stats_.runtime_, start_time_global);
  stats_.index_bytes_written_ += std::filesystem::file_size(context_.index_file_);
  ++stats_.nr_bulkloads_;

  pager_.Close();
}


template<class VType>
size_t cas::BulkLoader<VType>::Construct(
          cas::Partition& partition,
          cas::Dimension dimension,
          cas::Dimension par_dimension,
          int depth,
          size_t offset) {

  if (depth > 0) {
    // ignore the root partition since we determined
    // its discriminative bytes already before
    auto start_time_dsc = std::chrono::high_resolution_clock::now();
    switch (context_.dsc_computation_) {
      case cas::DscComputation::Proactive:
        // do nothing, done implicityly
        break;
      case cas::DscComputation::TwoPass:
        DscByte(partition);
        break;
      case cas::DscComputation::ByteByByte:
        DscByteByByte(partition);
        break;
      default:
        throw std::runtime_error{"unknown DscComputation"};
    }
    cas::util::AddToTimer(stats_.runtime_dsc_computation_, start_time_dsc);
  }

  Node node;
  size_t dsc_p = partition.DscP();
  size_t dsc_v = partition.DscV();

  BinaryKey key(nullptr);
  size_t key_len_p = 0;
  size_t key_len_v = 0;
  size_t next_pos = offset;

  {
    MemoryPage io_page = mpool_.input_.Get();
    auto cursor = partition.Cursor(io_page);
    // make sure the page is read into io_page when NextPage is called
    cursor.FetchNextDiskPage();
    key = *(cursor.NextPage()).begin();
    node.path_.reserve(dsc_p);
    node.value_.reserve(dsc_v);
    key_len_p = key.LenPath();
    key_len_v = key.LenValue();

    // chop off the discriminative byte in the parent
    // partitioning dimension
    int off_p = 0;
    int off_v = 0;
    switch (par_dimension) {
      case cas::Dimension::PATH:  ++off_p; break;
      case cas::Dimension::VALUE: ++off_v; break;
      case cas::Dimension::LEAF:  break;
      default:
        throw std::runtime_error{"illegal dimension"};
    }

    std::copy(key.Path() + off_p,
        key.Path() + dsc_p,
        std::back_inserter(node.path_));
    std::copy(key.Value() + off_v,
        key.Value() + dsc_v,
        std::back_inserter(node.value_));

    // return io_page to the input pool
    mpool_.input_.Release(std::move(io_page));
  }

  if ((!partition.IsRootPartition() &&
        partition.NrKeys() <= context_.partitioning_threshold_) ||
      (dsc_p >= key_len_p && dsc_v >= key_len_v)) {
    // stop partitioning further if
    // - the partition contains at most \tau (partitioning threshold) keys
    // - the partition contains only one distinct key, but
    //   occupies more than one memory page. This means that
    //   this unique key must contain many duplicate references
    node.dimension_ = cas::Dimension::LEAF;
    ConstructLeafNode(node, partition);
    next_pos += node.ByteSize(0);
  } else {
    if (dimension == cas::Dimension::PATH && dsc_p >= key_len_p) {
      dimension = cas::Dimension::VALUE;
    } else if (dimension == cas::Dimension::VALUE && dsc_v >= key_len_v) {
      dimension = cas::Dimension::PATH;
    }

    node.dimension_ = dimension;
    PartitionTable table(partition_counter_, context_, stats_);
    PsiPartition(table, partition, dimension);

    if (context_.print_root_partition_table_allocation_ && depth == 0) {
      table.PrintMemoryAllocation();
      std::cout << std::flush;
    }
    if (context_.compute_fanout_) {
      stats_.node_fanout_.Record(table.NrPartitions());
    }

    cas::Dimension dim_next;
    if (dimension == cas::Dimension::PATH) {
      dim_next = cas::Dimension::VALUE;
      ++dsc_p;
    } else {
      dim_next = cas::Dimension::PATH;
      ++dsc_v;
    }

    next_pos += node.ByteSize(table.NrPartitions());
    for (int byte = 0x00; byte <= 0xFF; ++byte) {
      if (table.Exists(byte)) {
        node.children_pointers_.emplace_back(static_cast<std::byte>(byte), next_pos);
        next_pos = Construct(table[byte], dim_next, dimension, depth + 1, next_pos);
      }
    }
  }

  // Write to disk
  size_t node_size = SerializeNode(node);
  pager_.Write(&serialization_buffer_->at(0), node_size, offset);

  return next_pos;
}


// add all the partial keys and their references
template<class VType>
void cas::BulkLoader<VType>::ConstructLeafNode(
      Node& node,
      cas::Partition& partition) {
  auto start = std::chrono::high_resolution_clock::now();
  size_t dsc_p = partition.DscP();
  size_t dsc_v = partition.DscV();
  // iterate & delete every page in the partition
  MemoryPage io_page = mpool_.input_.Get();
  auto cursor = partition.Cursor(io_page);
  while (cursor.HasNext()) {
    auto page = cursor.RemoveNextPage();
    for (auto key : page) {
      MemoryKey lkey;
      uint16_t new_len_p = 0;
      uint16_t new_len_v = 0;
      if (dsc_p < key.LenPath()) {
        new_len_p = key.LenPath() - dsc_p;
      }
      if (dsc_v < key.LenValue()) {
        new_len_v = key.LenValue() - dsc_v;
      }
      lkey.path_.resize(new_len_p);
      lkey.value_.resize(new_len_v);
      std::memcpy(&lkey.path_[0], key.Path() + dsc_p, new_len_p);
      std::memcpy(&lkey.value_[0], key.Value() + dsc_v, new_len_v);
      lkey.ref_ = key.Ref();
      node.suffixes_.push_back(lkey);
    }
    if (page.Type() != cas::MemoryPageType::INPUT) {
      mpool_.work_.Release(std::move(page));
    }
  }
  partition.DeleteFile();
  // return io_page to the input pool
  mpool_.input_.Release(std::move(io_page));
  cas::util::AddToTimer(stats_.runtime_construct_leaf_node_, start);
}


template<class VType>
void cas::BulkLoader<VType>::PsiPartition(
        cas::PartitionTable& table,
        cas::Partition& partition,
        const cas::Dimension dimension) {
  auto start = std::chrono::high_resolution_clock::now();
  // we need to remember this now before we mutate partition
  bool is_memory_only = partition.IsMemoryOnly();
  bool is_disk_only = partition.IsDiskOnly();
  bool is_hybrid = partition.IsHybrid();
  // prepare the input & output pages
  std::vector<cas::MemoryPage> pages;
  pages.reserve(cas::BYTE_MAX);
  for (size_t i = 0; i < cas::BYTE_MAX; ++i) {
    pages.emplace_back(nullptr);
  }
  MemoryPage io_page = mpool_.input_.Get();
  // decide if we want to use memory pages at all during
  // this partitioning step
  bool use_memory_pages =
    context_.memory_placement_ != cas::MemoryPlacement::AllOrNothing
    || (partition.NrPages() <= mpool_.work_.Capacity());
  // iterate over each page in the input relation
  auto cursor = partition.Cursor(io_page);
  while (cursor.HasNext()) {
    auto page = cursor.RemoveNextPage();
    if (partition.IsRootPartition()) {
      stats_.nr_input_keys_ += page.NrKeys();
    }
    for (auto next_key : page) {
      // drop the common prefixes
      BinaryKey key{shortened_key_buffer_->data()};
      uint16_t new_len_p = 0;
      uint16_t new_len_v = 0;
      if (partition.DscP() < next_key.LenPath()) {
        new_len_p = next_key.LenPath() - partition.DscP();
      }
      if (partition.DscV() < next_key.LenValue()) {
        new_len_v = next_key.LenValue() - partition.DscV();
      }
      key.Ref(next_key.Ref());
      key.LenPath(new_len_p);
      key.LenValue(new_len_v);
      std::memcpy(key.Path(), next_key.Path() + partition.DscP(),
          new_len_p);
      std::memcpy(key.Value(), next_key.Value() + partition.DscV(),
          new_len_v);
      // determine the discriminative byte
      int b;
      switch (dimension) {
        case cas::Dimension::PATH:
          b = static_cast<int>(key.Path()[0]);
          break;
        case cas::Dimension::VALUE:
          b = static_cast<int>(key.Value()[0]);
          break;
        default:
          throw std::runtime_error{"unexpected dimension"};
      }
      if (table.Absent(b)) {
        // initialize the new partition
        table.InitializePartition(b, key.LenPath(), key.LenValue());
        pages[b] = mpool_.output_.Get();
        if (key.ByteSize() > ref_keys_[b]->size()) {
          throw std::runtime_error{"key does not fit into buffer"};
        }
        std::memcpy(ref_keys_[b]->data(), key.Begin(), key.ByteSize());
      } else {
        // update the discriminative bytes
        cas::BinaryKey ref_key(ref_keys_[b]->data());
        auto& partition = table[b];
        int g_P = 0;
        int g_V = 0;
        while (g_P < partition.DscP() && key.Path()[g_P] == ref_key.Path()[g_P]) {
          ++g_P;
        }
        while (g_V < partition.DscV() && key.Value()[g_V] == ref_key.Value()[g_V]) {
          ++g_V;
        }
        partition.DscP(g_P);
        partition.DscV(g_V);
      }
      // make sure there is space for key in pages[b]
      if (key.ByteSize() > pages[b].FreeSpace()) {
        if (use_memory_pages && mpool_.work_.HasFreePage()) {
          table[b].PushToMemory(std::move(pages[b]));
          pages[b] = mpool_.work_.Get();
        } else {
          switch (context_.memory_placement_) {
            case cas::MemoryPlacement::FrontLoading: {
              int bp = 0xFF;
              while (bp > b && (table.Absent(bp) || !table[bp].HasMemoryPage())) {
                --bp;
              }
              if (bp > b) {
                auto tmp_page = table[bp].PopFromMemory();
                table[bp].PushToDisk(tmp_page);
                table[b].PushToMemory(std::move(pages[b]));
                pages[b] = std::move(tmp_page);
              } else {
                table[b].PushToDisk(pages[b]);
              }
              break;
            }
            case cas::MemoryPlacement::BackLoading: {
              int bp = 0x00;
              while (bp < b && (table.Absent(bp) || !table[bp].HasMemoryPage())) {
                ++bp;
              }
              if (bp < b) {
                auto tmp_page = table[bp].PopFromMemory();
                table[bp].PushToDisk(tmp_page);
                table[b].PushToMemory(std::move(pages[b]));
                pages[b] = std::move(tmp_page);
              } else {
                table[b].PushToDisk(pages[b]);
              }
              break;
            }
            case cas::MemoryPlacement::Uniform:
            case cas::MemoryPlacement::AllOrNothing:
              table[b].PushToDisk(pages[b]);
              break;
          }
          pages[b].Reset();
        }
        ++table[b].NrPages();
      }
      pages[b].Push(key);
    }
    if (page.Type() != cas::MemoryPageType::INPUT) {
      mpool_.work_.Release(std::move(page));
    }
  }
  // move dirty output pages to their partition's mptr
  for (int b = 0x00; b <= 0xFF; ++b) {
    if (table.Exists(b)) {
      if (use_memory_pages) {
        table[b].PushToMemory(std::move(pages[b]));
      } else {
        table[b].PushToDisk(pages[b]);
        mpool_.output_.Release(std::move(pages[b]));
      }
      ++table[b].NrPages();
    }
  }
  // try to fill output pool as much as possible with work pages
  while (!mpool_.output_.Full() && mpool_.work_.HasFreePage()) {
    auto page = mpool_.work_.Get();
    mpool_.output_.Release(std::move(page));
  }
  // replenish output pool
  switch (context_.memory_placement_) {
    case cas::MemoryPlacement::FrontLoading: {
      int b = 0xFF;
      while (!mpool_.output_.Full() && b >= 0x00) {
        while (!mpool_.output_.Full() && table.Exists(b) && table[b].HasMemoryPage()) {
          auto tmp_page = table[b].PopFromMemory();
          table[b].PushToDisk(tmp_page);
          mpool_.output_.Release(std::move(tmp_page));
        }
        --b;
      }
      break;
    }
    case cas::MemoryPlacement::BackLoading: {
      int b = 0x00;
      while (!mpool_.output_.Full() && b <= 0xFF) {
        while (!mpool_.output_.Full() && table.Exists(b) && table[b].HasMemoryPage()) {
          auto tmp_page = table[b].PopFromMemory();
          table[b].PushToDisk(tmp_page);
          mpool_.output_.Release(std::move(tmp_page));
        }
        ++b;
      }
      break;
    }
    case cas::MemoryPlacement::Uniform:
    case cas::MemoryPlacement::AllOrNothing: {
      int b = 0x00;
      while (!mpool_.output_.Full() && b <= 0xFF) {
        if (table.Exists(b) && table[b].HasMemoryPage()) {
          auto tmp_page = table[b].PopFromMemory();
          table[b].PushToDisk(tmp_page);
          mpool_.output_.Release(std::move(tmp_page));
        }
        ++b;
      }
      break;
    }
  }
  // close all partitions
  for (int b = 0x00; b <= 0xFF; ++b) {
    if (table.Exists(b)) {
      UpdatePartitionStats(table[b]);
      table[b].Close();
    }
  }
  // return io_page to the input pool
  mpool_.input_.Release(std::move(io_page));
  // check that the input/output pools are full
  if (!mpool_.input_.Full()) {
    throw std::runtime_error{"input_ pool is not full"};
  }
  if (!mpool_.output_.Full()) {
    throw std::runtime_error{"output pool is not full"};
  }
  // record runtime
  auto end = std::chrono::high_resolution_clock::now();
  cas::util::AddToTimer(stats_.runtime_partitioning_, start, end);
  if (is_memory_only) {
    cas::util::AddToTimer(stats_.runtime_partitioning_mem_only_, start, end);
  } else if (is_disk_only) {
    cas::util::AddToTimer(stats_.runtime_partitioning_disk_only_, start, end);
  } else if (is_hybrid) {
    cas::util::AddToTimer(stats_.runtime_partitioning_hybrid_, start, end);
  }
  // delete the disk-based keys in the original partition
  if (context_.delete_root_partition_ || !partition.IsRootPartition()) {
    partition.DeleteFile();
  }
}


template<class VType>
size_t cas::BulkLoader<VType>::SerializeNode(Node& node) {
  auto& buffer = *serialization_buffer_.get();

  constexpr size_t path_limit    = (1ul << 12) - 1;
  constexpr size_t value_limit   = (1ul <<  4) - 1;
  constexpr size_t payload_limit = (1ul << 14) - 1;
  constexpr size_t pointer_limit = (1ul << 48) - 1;

  // check bounds
  if (node.path_.size() > path_limit) {
    throw std::runtime_error{"path size exceeds 2**12-1"};
  }
  if (node.value_.size() > value_limit) {
    throw std::runtime_error{"value size exceeds 2**4-1"};
  }
  if (node.children_pointers_.size() > 256) {
    throw std::runtime_error{"number of children exceeds 256"};
  }
  if (node.suffixes_.size() > payload_limit) {
    throw std::runtime_error{"number of suffixes exceeds 2**14-1"};
  }
  if (node.ByteSize(node.children_pointers_.size()) > buffer.size()) {
    throw std::runtime_error{"node exceeds buffer size"};
  }

  // determine nr children/suffixes
  size_t m = node.IsLeaf()
    ? node.suffixes_.size()
    : node.children_pointers_.size();

  uint32_t header = 0;
  header |= (static_cast<uint32_t>(node.dimension_)    << 30);
  header |= (static_cast<uint32_t>(node.path_.size())  << 18);
  header |= (static_cast<uint32_t>(node.value_.size()) << 14);
  header |= (static_cast<uint32_t>(m));

  // serialize header
  size_t offset = 0;
  CopyToSerializationBuffer(offset, &header, sizeof(uint32_t));
  CopyToSerializationBuffer(offset, &node.path_[0], node.path_.size());
  CopyToSerializationBuffer(offset, &node.value_[0], node.value_.size());

  if (node.IsLeaf()) {
    // serialize suffixes
    for (const auto& suffix : node.suffixes_) {
      if (suffix.path_.size() > path_limit) {
        throw std::runtime_error{"path-suffix size exceeds 2**12-1"};
      }
      if (suffix.value_.size() > value_limit) {
        throw std::runtime_error{"value-suffix size exceeds 2**4-1"};
      }
      uint16_t pv_len = cas::util::EncodeSizes(suffix.path_.size(), suffix.value_.size());
      buffer[offset++] = static_cast<uint8_t>((pv_len >> 8) & 0xFF);
      buffer[offset++] = static_cast<uint8_t>((pv_len >> 0) & 0xFF);
      CopyToSerializationBuffer(offset, &suffix.path_[0], suffix.path_.size());
      CopyToSerializationBuffer(offset, &suffix.value_[0], suffix.value_.size());
      CopyToSerializationBuffer(offset, &suffix.ref_, sizeof(cas::ref_t));
    }
  } else {
    // serialize child pointers
    for (const auto& [byte, ptr] : node.children_pointers_) {
      if (ptr >= pointer_limit) {
        throw std::runtime_error{"pointer size exceeds 2**48-1"};
      }
      buffer[offset++] = static_cast<uint8_t>(byte);
      buffer[offset++] = static_cast<uint8_t>((ptr >> 40) & 0xFF);
      buffer[offset++] = static_cast<uint8_t>((ptr >> 32) & 0xFF);
      buffer[offset++] = static_cast<uint8_t>((ptr >> 24) & 0xFF);
      buffer[offset++] = static_cast<uint8_t>((ptr >> 16) & 0xFF);
      buffer[offset++] = static_cast<uint8_t>((ptr >>  8) & 0xFF);
      buffer[offset++] = static_cast<uint8_t>((ptr >>  0) & 0xFF);
    }
  }

  // validation
  if (offset != node.ByteSize(node.children_pointers_.size())) {
    throw std::runtime_error{"serialization size does not match expected size"};
  }

  return offset;
}


template<class VType>
void cas::BulkLoader<VType>::CopyToSerializationBuffer(
    size_t& offset, const void* src, size_t count) {
  if (offset + count > serialization_buffer_->size()) {
    auto msg = "address violation: " + std::to_string(offset+count);
    throw std::out_of_range{msg};
  }
  std::memcpy(serialization_buffer_->begin()+offset, src, count);
  offset += count;
}


template<class VType>
size_t cas::BulkLoader<VType>::Node::ByteSize(int nr_children) const {
  size_t size = 0;
  // header (dimension: 2 bits, l_P: 12 bits, l_V: 4 bits, m: 14 bits)
  size += 4;
  // lenghts of substrings
  size += path_.size();
  size += value_.size();
  // payload
  if (IsLeaf()) {
    for (const MemoryKey& suffix : suffixes_) {
      // header (l_P:1B, l_V:1B)
      size += 2;
      // lenghts of substrings
      size += suffix.path_.size();
      size += suffix.value_.size();
      size += sizeof(cas::ref_t);
    }
  } else {
    // per child => b:1, ptr: 6
    size += (7 * nr_children);
  }
  return size;
}


template<class VType>
void cas::BulkLoader<VType>::Node::Dump() const {
  std::cout << "(Node)";
  std::cout << "\ndimension_: ";
  switch (dimension_) {
  case cas::Dimension::PATH:
    std::cout << "PATH";
    break;
  case cas::Dimension::VALUE:
    std::cout << "VALUE";
    break;
  case cas::Dimension::LEAF:
    std::cout << "LEAF";
    break;
  }
  std::cout << "\npath_: ";
  std::cout << "(len=" << path_.size() << ") ";
  cas::util::DumpHexValues(path_);
  std::cout << "\nvalue_: ";
  std::cout << "(len=" << value_.size() << ") ";
  cas::util::DumpHexValues(value_);
  std::cout << "\nbyte_size_: " << ByteSize(children_pointers_.size());
  std::cout << "\n";
}


template<class VType>
void cas::BulkLoader<VType>::UpdatePartitionStats(
        const cas::Partition& partition) {
  ++stats_.partitions_created_;
  if (partition.IsMemoryOnly()) {
    ++stats_.partitions_memory_only_;
  } else if (partition.IsDiskOnly()) {
    ++stats_.partitions_disk_only_;
  } else if (partition.IsHybrid()) {
    ++stats_.partitions_hybrid_;
  }
}


template<class VType>
void cas::BulkLoader<VType>::DscByte(
    cas::Partition& partition) {
  auto buffer = std::make_unique<std::array<std::byte, cas::PAGE_SZ>>();
  bool is_first_key = true;
  BinaryKey ref_key{buffer->data()};
  MemoryPage io_page = mpool_.input_.Get();
  auto cursor = partition.Cursor(io_page);
  while (cursor.HasNext()) {
    auto& page = cursor.NextPage();
    for (auto key : page) {
      if (is_first_key) {
        std::memcpy(buffer->data(), key.Begin(), key.ByteSize());
        partition.DscP(key.LenPath());
        partition.DscV(key.LenValue());
        is_first_key = false;
      } else {
        int g_P = 0;
        int g_V = 0;
        while (g_P < partition.DscP() && key.Path()[g_P] == ref_key.Path()[g_P]) {
          ++g_P;
        }
        while (g_V < partition.DscV() && key.Value()[g_V] == ref_key.Value()[g_V]) {
          ++g_V;
        }
        partition.DscP(g_P);
        partition.DscV(g_V);
      }
    }
  }

  // return io_page to the input pool
  mpool_.input_.Release(std::move(io_page));
}


template<class VType>
void cas::BulkLoader<VType>::DscByteByByte(
    cas::Partition& partition) {

  int dsc_P = 0;
  int dsc_V = 0;

  // storage for reference key
  auto buffer = std::make_unique<std::array<std::byte, cas::PAGE_SZ>>();
  BinaryKey ref_key{buffer->data()};
  bool is_first_key = true;

  bool dsc_P_found = false;
  bool dsc_V_found = false;
  MemoryPage io_page = mpool_.input_.Get();
  while (!dsc_P_found || !dsc_V_found) {
    auto cursor = partition.Cursor(io_page);
    while (cursor.HasNext() && (!dsc_P_found || !dsc_V_found)) {
      auto& page = cursor.NextPage();
      for (auto key : page) {
        if (is_first_key) {
          std::memcpy(buffer->data(), key.Begin(), key.ByteSize());
          is_first_key = false;
        } else {
          if (dsc_P >= ref_key.LenPath() || key.Path()[dsc_P] != ref_key.Path()[dsc_P]) {
            dsc_P_found = true;
          }
          if (dsc_V >= ref_key.LenValue() || key.Value()[dsc_V] != ref_key.Value()[dsc_V]) {
            dsc_V_found = true;
          }
        }
      }
    }
    if (!dsc_P_found) { ++dsc_P; }
    if (!dsc_V_found) { ++dsc_V; }
  }

  // return io_page to the input pool
  mpool_.input_.Release(std::move(io_page));

  partition.DscP(dsc_P);
  partition.DscV(dsc_V);
}


template<class VType>
void cas::BulkLoader<VType>::InitializeRootPartition(
      cas::Partition& partition) {
  // declare it the root partition
  partition.IsRootPartition(true);

  // this will denote the first page that is located on disk
  // after all memory-resident pages of this partition
  size_t first_disk_page_nr = 0;

  // decide how many input pages are processed
  size_t last_disk_page_nr = context_.dataset_size_ > 0
    ? context_.dataset_size_ / cas::PAGE_SZ
    : std::filesystem::file_size(partition.Filename()) / cas::PAGE_SZ;

  // decide if we want to use memory pages for the root partition
  bool use_memory_pages =
    context_.memory_placement_ != cas::MemoryPlacement::AllOrNothing
    || (last_disk_page_nr <= mpool_.work_.Capacity());

  if (use_memory_pages) {
    auto io_page = mpool_.input_.Get();
    auto cursor = partition.Cursor(io_page);
    while (cursor.HasNext() && mpool_.work_.HasFreePage() && first_disk_page_nr < last_disk_page_nr) {
      auto& page = cursor.NextPage();
      auto work_page = mpool_.work_.Get();
      std::memcpy(work_page.Data(), page.Data(), page.Size());
      partition.PushToMemory(std::move(work_page));
      ++first_disk_page_nr;
    }
    mpool_.input_.Release(std::move(io_page));
  }

  // remember the position of the first disk page that
  // is not cached in memory
  partition.FptrCursorFirstPageNr(first_disk_page_nr);
  partition.FptrCursorLastPageNr(last_disk_page_nr);
  partition.NrPages() = last_disk_page_nr;
}


/* // explicit instantiations to separate header from implementation */
/* template class cas::BulkLoader<cas::vint32_t, cas::PAGE_SZ>; */
/* template class cas::BulkLoader<cas::vint64_t, cas::PAGE_SZ>; */
/* template class cas::BulkLoader<cas::vstring_t, cas::PAGE_SZ>; */


template class cas::BulkLoader<cas::vint64_t>;
