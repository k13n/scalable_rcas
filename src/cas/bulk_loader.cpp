#include "cas/bulk_loader.hpp"
#include "cas/key_encoder.hpp"
#include "cas/record.hpp"
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


template<class VType, size_t PAGE_SZ>
cas::BulkLoader<VType, PAGE_SZ>::BulkLoader(
      cas::Pager<PAGE_SZ>& pager,
      const cas::Context& context)
  : mpool_(MemoryPools<PAGE_SZ>::Construct(context.mem_size_bytes_, context.mem_capacity_bytes_))
  , pager_(pager)
  , context_(context)
  , shortened_key_buffer_(std::make_unique<std::array<std::byte, PAGE_SZ>>())
  , serialization_buffer_(std::make_unique<std::array<uint8_t, 10'000'000>>())
{
  for (int b = 0; b <= 0xFF; ++b) {
    ref_keys_[b] = std::make_unique<std::array<std::byte, PAGE_SZ>>();
  }
}


template<class VType, size_t PAGE_SZ>
void cas::BulkLoader<VType, PAGE_SZ>::Load() {
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
  cas::Partition<PAGE_SZ> partition{context_.input_filename_, stats_, context_};
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

  stats_.root_partition_bytes_read_ = stats_.partition_bytes_read_;
  stats_.root_partition_bytes_written_ = stats_.partition_bytes_written_;
  stats_.partition_bytes_read_ = 0;
  stats_.partition_bytes_written_ = 0;

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
}


template<class VType, size_t PAGE_SZ>
cas::Key<VType> cas::BulkLoader<VType, PAGE_SZ>::ProcessLine(
      const std::string& line,
      char delimiter) {
  cas::Key<VType> key;

  std::string path;
  std::string value;
  std::string ref;
  std::stringstream line_stream(line);
  std::getline(line_stream, path,  delimiter);
  std::getline(line_stream, value, delimiter);
  std::getline(line_stream, ref,   delimiter);

  key.path_  = context_.reverse_paths_ ? ReversePath(path) : std::move(path);
  key.value_ = ParseValue<VType>(value);
  key.ref_   = ParseSwhPid(ref);

  return key;
}


template<class VType, size_t PAGE_SZ>
std::string cas::BulkLoader<VType, PAGE_SZ>::ReversePath(const std::string& path) {
  std::vector<std::string> labels;
  labels.reserve(20);
  char delimiter = '/';
  size_t prev_pos = 0;
  size_t pos = 0;
  while ((pos = path.find(delimiter, prev_pos)) != std::string::npos) {
    if (pos > prev_pos) {
      labels.push_back(path.substr(prev_pos, pos - prev_pos));
    }
    prev_pos = ++pos;
  }
  if (prev_pos < path.length()) {
    labels.push_back(path.substr(prev_pos, path.length() - prev_pos));
  }
  std::string result = "";
  for (auto i = labels.rbegin(); i != labels.rend(); ++i) {
    result += *i + "/";
  }
  return result;
}


template<>
cas::vint32_t cas::ParseValue<cas::vint32_t>(std::string& value) {
  return std::stoi(value);
}
template<>
cas::vint64_t cas::ParseValue<cas::vint64_t>(std::string& value) {
  return std::stoll(value);
}
template<>
std::string cas::ParseValue<std::string>(std::string& value) {
  size_t pos = 0;
  while (pos < value.size() && value[pos] == ' ') {
    ++pos;
  }
  return value.substr(pos);
}


// add all the partial keys and their references
template<class VType, size_t PAGE_SZ>
void cas::BulkLoader<VType, PAGE_SZ>::ConstructLeafNode(
      Node& node,
      cas::Partition<PAGE_SZ>& partition) {
  auto start = std::chrono::high_resolution_clock::now();
  size_t dsc_p = partition.DscP();
  size_t dsc_v = partition.DscV();
  // iterate & delete every page in the partition
  MemoryPage<PAGE_SZ> io_page = mpool_.input_.Get();
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
      lkey.path_.reserve(new_len_p);
      lkey.value_.reserve(new_len_v);
      std::copy(key.Path() + dsc_p, key.Path() + key.LenPath(),
          std::back_inserter(lkey.path_));
      std::copy(key.Value() + dsc_v, key.Value() + key.LenValue(),
          std::back_inserter(lkey.value_));
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


template<class VType, size_t PAGE_SZ>
size_t cas::BulkLoader<VType, PAGE_SZ>::Construct(
          cas::Partition<PAGE_SZ>& partition,
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
    MemoryPage<PAGE_SZ> io_page = mpool_.input_.Get();
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


  if ((partition.NrKeys() <= context_.partitioning_threshold_) ||
      (dsc_p >= key_len_p && dsc_v >= key_len_v)) {
    /* std::cout << "[DEBUG] reached base case" << std::endl; */
    // stop partitioning further if
    // - the partition contains at most \tau (partitioning threshold) keys
    // - the partition contains only one distinct key, but
    //   occupies more than one memory page. This means that
    //   this unique key must contain many duplicate references
    node.dimension_ = cas::Dimension::LEAF;
    ConstructLeafNode(node, partition);
    next_pos += node.ByteSize(0);
  } else {
    /* std::cout << "[DEBUG] recursive case" << std::endl; */
    if (dimension == cas::Dimension::PATH && dsc_p >= key_len_p) {
      dimension = cas::Dimension::VALUE;
    } else if (dimension == cas::Dimension::VALUE && dsc_v >= key_len_v) {
      dimension = cas::Dimension::PATH;
    }

    node.dimension_ = dimension;
    PartitionTable<PAGE_SZ> table(partition_counter_, context_, stats_);
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
    /* std::cout << "[DEBUG] hello world 7" <<  std::endl; */
    /* std::cout << "[DEBUG] node_size: " << node.ByteSize(table.NrPartitions()) << std::endl; */
    /* std::cout << "[DEBUG] next_pos: " << next_pos << std::endl; */

    for (int byte = 0x00; byte <= 0xFF; ++byte) {
      if (table.Exists(byte)) {
        node.children_pointers_.emplace_back(static_cast<std::byte>(byte), next_pos);
        /* std::cout << "[DEBUG] T byte exists: " << byte << std::endl; */
        next_pos = Construct(table[byte], dim_next, dimension, depth + 1, next_pos);
      }
    }
  }

  // Write to disk
  size_t node_size = SerializeNode(node);
  pager_.Write(&serialization_buffer_->at(0), node_size, offset);

  /* if (depth == 0) { */
  /*   std::cout << "Write node to disk from " << offset << " to " << */
  /*     (offset + node_size) << "\n"; */
  /*   cas::util::DumpHexValues(&serialization_buffer_->at(0), 0, node_size); */
  /*   std::cout << "\n"; */
  /* } */

  return next_pos;
}


template<class VType, size_t PAGE_SZ>
void cas::BulkLoader<VType, PAGE_SZ>::PsiPartition(
        cas::PartitionTable<PAGE_SZ>& table,
        cas::Partition<PAGE_SZ>& partition,
        const cas::Dimension dimension) {
  auto start = std::chrono::high_resolution_clock::now();
  // we need to remember this now before we mutate partition
  bool is_memory_only = partition.IsMemoryOnly();
  bool is_disk_only = partition.IsDiskOnly();
  bool is_hybrid = partition.IsHybrid();
  // prepare the input & output pages
  std::vector<cas::MemoryPage<PAGE_SZ>> pages;
  pages.reserve(cas::BYTE_MAX);
  for (size_t i = 0; i < cas::BYTE_MAX; ++i) {
    pages.emplace_back(nullptr);
  }
  MemoryPage<PAGE_SZ> io_page = mpool_.input_.Get();
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
  if (!partition.IsRootPartition()) {
    partition.DeleteFile();
  }
}


template<class VType, size_t PAGE_SZ>
std::string cas::BulkLoader<VType, PAGE_SZ>::getKey(int s2,int j){
    std::string key =std::to_string(s2) +"_"+ std::to_string(j);
    return key;
}


// template<class VType, size_t PAGE_SZ>
// void cas::BulkLoader<VType, PAGE_SZ>::PartitionTreeRS(std::unique_ptr<Node>& node) {
//   if (node->path_.size() > std::numeric_limits<uint16_t>::max()) {
//     throw std::runtime_error{"path size exceeds uint16_t"};
//   }
//   if (node->value_.size() > std::numeric_limits<uint16_t>::max()) {
//     throw std::runtime_error{"value size exceeds uint16_t"};
//   }
//   if (node->references_.size() > std::numeric_limits<uint32_t>::max()) {
//     std::string msg = "number references exceeds uint32_t: "
//       + std::to_string(node->references_.size());
//     throw std::runtime_error{msg};
//   }
//
//   std::deque<std::pair<std::byte, Node*>> partition;
//   // child-partition header (disc-byte=1b, offset=2b)
//   const size_t header_size = 3;
//
//   if (node->dimension_ == cas::Dimension::LEAF) {
//     // heaader size (dim=1b, plen=2b, vlen=2b, rlen=4b, overflow_page=1b)
//     node->byte_size_ = 10;
//   } else {
//     // heaader size (dimension=1b, plen=2b, vlen=2b, clen=2b)
//     node->byte_size_ = 7;
//   }
//   // path- and value-prefix sizes
//   node->byte_size_ += node->path_.size();
//   node->byte_size_ += node->value_.size();
//   // size of the references for a leaf node (>0 if node is a leaf)
//   size_t ref_bytes = 0;
//   for (const auto& leaf_key : node->references_) {
//     // header size (plen=2b, vlen=2b)
//     ref_bytes += 4;
//     ref_bytes += leaf_key.path_.size() + leaf_key.value_.size();
//     ref_bytes += sizeof(cas::ref_t);
//   }
//   node->byte_size_ += ref_bytes;
//   // so far only the node itself is in the same page
//   node->nr_descendants_in_same_page_ = 1;
//
//   // handle very big leaf nodes (try to create an overflow page)
//   if (header_size + node->byte_size_ > PAGE_SZ-1) {
//     assert(node->dimension_ == cas::Dimension::LEAF);
//     const size_t overflow_ptr_size = 8;
//     size_t overflow_node_size = header_size
//       + node->byte_size_ - ref_bytes + overflow_ptr_size;
//     if (overflow_node_size <= PAGE_SZ-1) {
//       // the node is a leaf that must be split into a number
//       // of overflow pages
//       node->overflow_page_nr_ = CreateOverflowPages(*node);
//       node->byte_size_ -= ref_bytes;
//       node->byte_size_ += overflow_ptr_size;
//       node->has_overflow_page_ = true;
//     } else {
//       throw std::runtime_error{"single node does not fit on disk page"};
//     }
//   }
//
//   size_t bytes_header = 0;
//   size_t bytes_content = 0;
//   for (int byte = 0xFF; byte >= 0x00; --byte) {
//     const auto& child = node->child_pointers_[byte];
//     if (child == nullptr) {
//       continue;
//     }
//     if (bytes_header + header_size + bytes_content + child->byte_size_ > PAGE_SZ-1) {
//       // write partition to a page
//       cas::page_nr_t page_nr = (max_page_nr_++);
//       WritePartition(partition, page_nr);
//       node->child_pages_.emplace_front(
//           std::get<0>(partition.front()),
//           std::get<0>(partition.back()),
//           page_nr);
//       node->nr_interpage_pointers_ += partition.size();
//       node->nr_interpage_pages_ += 1;
//       // free memory for nodes
//       for (int i = static_cast<int>(std::get<0>(partition.front()));
//            i <= static_cast<int>(std::get<0>(partition.back()));
//            ++i) {
//         node->child_pointers_[i] = nullptr;
//       }
//       partition.clear();
//       // the new partition is now empty, reset counters
//       bytes_header = 0;
//       bytes_content = 0;
//       // inter-page pointers from parent to page (type=1b, disc-byte-range=2b, page_nr=8b)
//       node->byte_size_ += 11;
//     }
//     bytes_header += header_size;
//     bytes_content += child->byte_size_;
//     partition.emplace_front(static_cast<std::byte>(byte), child.get());
//   }
//
//
//   // intra-page pointers (type=1b, disc-byte=1b, offset=2b)
//   size_t bytes_ptrs = 4*partition.size();
//   size_t node_header = 3;
//   if (node_header + node->byte_size_ + bytes_content + bytes_ptrs > PAGE_SZ-1) {
//     // parent is put in its own new page, remaining children are put in own page
//     cas::page_nr_t page_nr = (max_page_nr_++);
//     WritePartition(partition, page_nr);
//     node->child_pages_.emplace_front(
//         std::get<0>(partition.front()),
//         std::get<0>(partition.back()),
//         page_nr);
//     node->nr_interpage_pointers_ += partition.size();
//     node->nr_interpage_pages_ += 1;
//     // free memory for nodes
//     for (auto i = static_cast<int>(std::get<0>(partition.front()));
//          i <= static_cast<int>(std::get<0>(partition.back()));
//          ++i) {
//       node->child_pointers_[i] = nullptr;
//     }
//     partition.clear();
//     // the new partition is now empty, reset counters
//     bytes_header = 0;
//     bytes_content = 0;
//     // inter-page pointers from parent to page (type=1b, disc-byte-range=2b, pagenr=8b)
//     node->byte_size_ += 11;
//   } else {
//     // parent fits with all remaining children in one page
//     node->byte_size_ += bytes_content;
//     node->byte_size_ += bytes_ptrs;
//     // accumulate the statistics from the children in their parent
//     for (const auto& [_,child] : partition) {
//       node->nr_interpage_pointers_ += child->nr_interpage_pointers_;
//       node->nr_interpage_pages_ += child->nr_interpage_pages_;
//       node->nr_descendants_in_same_page_ += child->nr_descendants_in_same_page_;
//     }
//   }
// }

// template<class VType, size_t PAGE_SZ>
// size_t cas::BulkLoader<VType, PAGE_SZ>::WritePartition(
//     std::deque<std::pair<std::byte, Node*>> nodes,
//     cas::page_nr_t page_nr) {
//   auto page = pager_.NewIdxPage();
//   // the partition_size can be at most 256, which does not fit into
//   // uint8_t, therefore we wrap around and set it to 0 if the
//   // partition_size is 256
//   auto partition_size = static_cast<uint8_t>(nodes.size());
//   if (nodes.size() == 256) {
//     partition_size = 0;
//   }
//   const size_t node_header = 3;
//   size_t offset_header = 0;
//   size_t offset_payload = 1 + (node_header * nodes.size());
//   CopyToPage(*page, offset_header, &partition_size, sizeof(uint8_t));
//   for (auto& pair : nodes) {
//     auto disc_byte = std::get<0>(pair);
//     auto* node = std::get<1>(pair);
//     auto address = static_cast<uint16_t>(offset_payload);
//     CopyToPage(*page, offset_header, &disc_byte, sizeof(uint8_t));
//     CopyToPage(*page, offset_header, &address, sizeof(uint16_t));
//     offset_payload = SerializePartitionToPage(*node, *page, offset_payload);
//   }
//   WritePage(*page, page_nr);
//   return offset_payload;
// }


// template<class VType, size_t PAGE_SZ>
// void cas::BulkLoader<VType, PAGE_SZ>::WritePage(
//       const cas::IdxPage<PAGE_SZ>& page,
//       cas::page_nr_t page_nr) {
//   auto start = std::chrono::high_resolution_clock::now();
//   pager_.Write(page, page_nr);
//   stats_.index_bytes_written_ += PAGE_SZ;
//   cas::util::AddToTimer(stats_.runtime_clustering_disk_write_, start);
// }


template<class VType, size_t PAGE_SZ>
size_t cas::BulkLoader<VType, PAGE_SZ>::SerializeNode(Node& node) {
  auto& buffer = *serialization_buffer_.get();

  // check bounds
  if (node.path_.size() > std::numeric_limits<uint8_t>::max()) {
    throw std::runtime_error{"path size exceeds uint8_t"};
  }
  if (node.value_.size() > std::numeric_limits<uint8_t>::max()) {
    throw std::runtime_error{"value size exceeds uint8_t"};
  }
  if (node.suffixes_.size() > std::numeric_limits<uint16_t>::max()) {
    throw std::runtime_error{"number of suffixes exceeds uint16_t"};
  }
  if (node.ByteSize(node.children_pointers_.size()) > buffer.size()) {
    throw std::runtime_error{"node exceeds buffer size"};
  }

  // determine nr children/suffixes
  uint16_t m = node.IsLeaf()
    ? static_cast<uint16_t>(node.suffixes_.size())
    : static_cast<uint16_t>(node.children_pointers_.size());

  // serialize header
  size_t offset = 0;

  buffer[offset++] = static_cast<uint8_t>(node.dimension_);
  buffer[offset++] = static_cast<uint8_t>(node.path_.size());;
  buffer[offset++] = static_cast<uint8_t>(node.value_.size());
  CopyToSerializationBuffer(offset, &m, sizeof(uint16_t));
  CopyToSerializationBuffer(offset, &node.path_[0], node.path_.size());
  CopyToSerializationBuffer(offset, &node.value_[0], node.value_.size());

  if (node.IsLeaf()) {
    // serialize suffixes
    for (const auto& suffix : node.suffixes_) {
      if (suffix.path_.size() > std::numeric_limits<uint8_t>::max()) {
        throw std::runtime_error{"path size exceeds uint8_t"};
      }
      if (suffix.value_.size() > std::numeric_limits<uint8_t>::max()) {
        throw std::runtime_error{"value size exceeds uint8_t"};
      }
      buffer[offset++] = static_cast<uint8_t>(suffix.path_.size());
      buffer[offset++] = static_cast<uint8_t>(suffix.value_.size());
      CopyToSerializationBuffer(offset, &suffix.path_[0], suffix.path_.size());
      CopyToSerializationBuffer(offset, &suffix.value_[0], suffix.value_.size());
      CopyToSerializationBuffer(offset, &suffix.ref_[0], suffix.ref_.size());
    }
  } else {
    // serialize child pointers
    for (const auto& [byte, ptr] : node.children_pointers_) {
      constexpr size_t limit = 1ul<<48ul; // 6 byte range
      if (ptr >= limit) {
        throw std::runtime_error{"pointer size exceeds 2**48"};
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
    /* std::cout << "[DEBUG] serial_size: " << offset << "\n"; */
    /* std::cout << "[DEBUG] node_size: " << node.ByteSize(node.children_pointers_.size()) << "\n"; */
    throw std::runtime_error{"serialization size does not match expected size"};
  }

  return offset;
}


template<class VType, size_t PAGE_SZ>
void cas::BulkLoader<VType, PAGE_SZ>::CopyToSerializationBuffer(
    size_t& offset, const void* src, size_t count) {
  if (offset + count > serialization_buffer_->size()) {
    auto msg = "address violation: " + std::to_string(offset+count);
    throw std::out_of_range{msg};
  }
  std::memcpy(serialization_buffer_->begin()+offset, src, count);
  offset += count;
}



template<class VType, size_t PAGE_SZ>
size_t cas::BulkLoader<VType, PAGE_SZ>::SerializePartitionToPage(
    Node& node,
    cas::IdxPage<PAGE_SZ>& page,
    size_t offset) {

  // TODO

  // auto plen = static_cast<uint16_t>(node.path_.size());
  // auto vlen = static_cast<uint16_t>(node.value_.size());
  // uint32_t rlen = static_cast<uint32_t>(node.references_.size());

  // // count number of children
  // uint16_t clen = 0;
  // if (node.dimension_ != cas::Dimension::LEAF) {
  //   clen += node.child_pages_.size();
  //   for (size_t byte = 0x00; byte <= 0xFF; ++byte) {
  //     if (node.child_pointers_[byte] != nullptr) {
  //       ++clen;
  //     }
  //   }
  // }

  // // copy common header
  // page.at(offset) = static_cast<std::byte>(node.dimension_);
  // ++offset;
  // CopyToPage(page, offset, &plen, sizeof(plen));
  // CopyToPage(page, offset, &vlen, sizeof(vlen));

  // // copy node-type specific header
  // if (node.dimension_ == cas::Dimension::LEAF) {
  //   // leaf node has references
  //   CopyToPage(page, offset, &rlen, sizeof(rlen));
  //   page.at(offset) = node.has_overflow_page_ ? std::byte{1} : std::byte{0};
  //   ++offset;
  // } else {
  //   // inner node has children
  //   CopyToPage(page, offset, &clen, sizeof(clen));
  // }

  // // copy prefixes
  // CopyToPage(page, offset, &node.path_[0], plen);
  // CopyToPage(page, offset, &node.value_[0], vlen);

  // // copy references
  // if (node.dimension_ == cas::Dimension::LEAF) {
  //   if (node.has_overflow_page_) {
  //     CopyToPage(page, offset, &node.overflow_page_nr_, sizeof(cas::page_nr_t));
  //   } else {
  //     for (const auto& leaf_key : node.references_) {
  //       auto lk_plen = static_cast<uint16_t>(leaf_key.path_.size());
  //       auto lk_vlen = static_cast<uint16_t>(leaf_key.value_.size());
  //       CopyToPage(page, offset, &lk_plen, sizeof(lk_plen));
  //       CopyToPage(page, offset, &lk_vlen, sizeof(lk_vlen));
  //       CopyToPage(page, offset, &leaf_key.path_[0], lk_plen);
  //       CopyToPage(page, offset, &leaf_key.value_[0], lk_vlen);
  //       CopyToPage(page, offset, leaf_key.ref_.data(), sizeof(cas::ref_t));
  //     }
  //   }
  //   return offset;
  // }

  // // compute and skip space needed for child pointers
  // size_t offset_children = offset;
  // for (size_t byte = 0x00; byte <= 0xFF; ++byte) {
  //   if (node.child_pointers_[byte]) {
  //     offset += 2 + sizeof(uint16_t);
  //   }
  // }
  // // type=1b, disc-byte-range=2b, pagenr=8b
  // const size_t bytes_pageref = 1+2+8;
  // offset += node.child_pages_.size() * bytes_pageref;

  // // write child pointers & children
  // for (size_t byte = 0x00; byte <= 0xFF; ++byte) {
  //   if (node.child_pointers_[byte]) {
  //     uint8_t type = 1;
  //     auto val = static_cast<uint8_t>(byte);
  //     auto address = static_cast<uint16_t>(offset);
  //     offset = SerializePartitionToPage(*node.child_pointers_[byte], page, offset);
  //     CopyToPage(page, offset_children, &type, sizeof(type));
  //     CopyToPage(page, offset_children, &val, sizeof(val));
  //     CopyToPage(page, offset_children, &address, sizeof(uint16_t));
  //   }
  // }

  // // write child page pointers
  // for (auto& tuple : node.child_pages_) {
  //   uint8_t type  = 0;
  //   auto vlow  = static_cast<uint8_t>(std::get<0>(tuple));
  //   auto vhigh = static_cast<uint8_t>(std::get<1>(tuple));
  //   auto& page_nr = std::get<2>(tuple);
  //   CopyToPage(page, offset_children, &type, sizeof(type));
  //   CopyToPage(page, offset_children, &vlow, sizeof(vlow));
  //   CopyToPage(page, offset_children, &vhigh, sizeof(vhigh));
  //   CopyToPage(page, offset_children, &page_nr, sizeof(cas::page_nr_t));
  // }

  return offset;
}


template<class VType, size_t PAGE_SZ>
size_t cas::BulkLoader<VType, PAGE_SZ>::Node::ByteSize(int nr_children) const {
  size_t size = 0;
  // header (dimension:1B, l_P:1B, l_V:1B, m:2B)
  size += 5;
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
      size += suffix.ref_.size();
    }
  } else {
    // per child => b:1, ptr: 6
    size += (7 * nr_children);
  }
  return size;
}


template<class VType, size_t PAGE_SZ>
void cas::BulkLoader<VType, PAGE_SZ>::Node::Dump() const {
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


template<class VType, size_t PAGE_SZ>
void cas::BulkLoader<VType, PAGE_SZ>::UpdatePartitionStats(
        const cas::Partition<PAGE_SZ>& partition) {
  ++stats_.partitions_created_;
  if (partition.IsMemoryOnly()) {
    ++stats_.partitions_memory_only_;
  } else if (partition.IsDiskOnly()) {
    ++stats_.partitions_disk_only_;
  } else if (partition.IsHybrid()) {
    ++stats_.partitions_hybrid_;
  }
}


template<class VType, size_t PAGE_SZ>
void cas::BulkLoader<VType, PAGE_SZ>::DscByte(
    cas::Partition<PAGE_SZ>& partition) {
  auto buffer = std::make_unique<std::array<std::byte, PAGE_SZ>>();
  bool is_first_key = true;
  BinaryKey ref_key{buffer->data()};
  MemoryPage<PAGE_SZ> io_page = mpool_.input_.Get();
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


template<class VType, size_t PAGE_SZ>
void cas::BulkLoader<VType, PAGE_SZ>::DscByteByByte(
    cas::Partition<PAGE_SZ>& partition) {

  int dsc_P = 0;
  int dsc_V = 0;

  // storage for reference key
  auto buffer = std::make_unique<std::array<std::byte, PAGE_SZ>>();
  BinaryKey ref_key{buffer->data()};
  bool is_first_key = true;

  bool dsc_P_found = false;
  bool dsc_V_found = false;
  MemoryPage<PAGE_SZ> io_page = mpool_.input_.Get();
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


template<class VType, size_t PAGE_SZ>
void cas::BulkLoader<VType, PAGE_SZ>::InitializeRootPartition(
    cas::Partition<PAGE_SZ>& partition) {


  // declare it the root partition
  partition.IsRootPartition(true);

  // this will denote the first page that is located on disk
  // after all memory-resident pages of this partition
  size_t first_disk_page_nr = 0;

  // decide how many input pages are processed
  size_t last_disk_page_nr = context_.dataset_size_ > 0
    ? context_.dataset_size_ / PAGE_SZ
    : std::filesystem::file_size(partition.Filename()) / PAGE_SZ;

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


template class cas::BulkLoader<cas::vint64_t, cas::PAGE_SZ_64KB>;
template class cas::BulkLoader<cas::vint64_t, cas::PAGE_SZ_32KB>;
template class cas::BulkLoader<cas::vint64_t, cas::PAGE_SZ_16KB>;
template class cas::BulkLoader<cas::vint64_t, cas::PAGE_SZ_8KB>;
template class cas::BulkLoader<cas::vint64_t, cas::PAGE_SZ_4KB>;
