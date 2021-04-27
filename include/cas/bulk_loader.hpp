#pragma once

#include "cas/binary_key.hpp"
#include "cas/bulk_loader_stats.hpp"
#include "cas/context.hpp"
#include "cas/key.hpp"
#include "cas/memory_pool.hpp"
#include "cas/partition.hpp"
#include "cas/partition_table.hpp"
#include "cas/pager.hpp"
#include <deque>
#include <iostream>
#include <chrono>

namespace cas {

template<class VType, size_t PAGE_SZ>
class BulkLoader {
  MemoryPools<PAGE_SZ> mpool_;
  Pager<PAGE_SZ>& pager_;
  const Context& context_;
  page_nr_t max_page_nr_ = 1;
  long partition_counter_ = 0;
  std::array<std::unique_ptr<std::array<std::byte, PAGE_SZ>>, cas::BYTE_MAX> ref_keys_;
  std::unique_ptr<std::array<std::byte, PAGE_SZ>> shortened_key_buffer_;
  BulkLoaderStats stats_;

  std::chrono::time_point<std::chrono::high_resolution_clock> start_time_global;


  struct Node {
    cas::Dimension dimension_ = cas::Dimension::PATH;
    std::vector<std::byte> path_;
    std::vector<std::byte> value_;
    std::deque<std::tuple<std::byte,std::byte,cas::page_nr_t>> child_pages_{};
    std::array<std::unique_ptr<Node>, cas::BYTE_MAX> child_pointers_{};
    std::vector<MemoryKey> references_;
    size_t byte_size_ = 0;
    bool has_overflow_page_ = false;
    page_nr_t overflow_page_nr_ = 0;
    std::vector<int> child_byte_;
    size_t nr_interpage_pointers_ = 0;
    size_t nr_interpage_pages_ = 0;
    size_t nr_descendants_in_same_page_ = 0;

    void Dump() const;
  };

public:
  BulkLoader(Pager<PAGE_SZ>& pager, const Context& context);
  void Load();
  BulkLoaderStats& Stats() { return stats_; }

  std::string getKey(int s2,int j);
private:
  std::unique_ptr<Node> Construct(
      cas::Partition<PAGE_SZ>& partition,
      cas::Dimension dimension,
      cas::Dimension par_dimension,
      int depth = 0);

  void PsiPartition(
      PartitionTable<PAGE_SZ>& table,
      Partition<PAGE_SZ>& partition,
      const cas::Dimension dimension);

  void PartitionTreeFDW(std::unique_ptr<Node>& node);
  void PartitionTreeRS(std::unique_ptr<Node>& node);

  void ConstructLeafNode(
      std::unique_ptr<Node>& node,
      cas::Partition<PAGE_SZ>& partition);

  size_t WritePartition(
      std::deque<std::pair<std::byte, Node*>> nodes,
      cas::page_nr_t page_nr);

  size_t SerializePartitionToPage(
      Node& node,
      cas::IdxPage<PAGE_SZ>& page,
      size_t offset = 0);

  void WritePage(const cas::IdxPage<PAGE_SZ>& page,
      cas::page_nr_t page_nr);

  cas::page_nr_t CreateOverflowPages(Node& node);

  static void CopyToPage(cas::IdxPage<PAGE_SZ>& page,
    size_t& offset, const void* src, size_t count);

  size_t ComputeNodeByteSize(Node& node);


  Key<VType> ProcessLine(const std::string& line, char delimiter);
  std::string ReversePath(const std::string& path);

  void UpdatePartitionStats(const Partition<PAGE_SZ>& partition);

  void InitializeRootPartition(Partition<PAGE_SZ>& partition);

  void DscByte(Partition<PAGE_SZ>& partition);
  void DscByteByByte(Partition<PAGE_SZ>& partition);

  bool ContainsLeafNode(const std::deque<std::pair<std::byte, Node*>>& nodes);
};

template<class VType>
VType ParseValue(std::string& value);


struct dtable_t {
  std::string begin;
  int begin_idx;
  std::string end;
  int end_idx;
  int card=__INT_MAX__;
  int rw=-1;
  std::string nextKey = "0_0";

  void print(){
    std::cout << "\nbegin: "<< begin<< "\nbegin_idx: "<< begin_idx<< "\nend: "<< end<< "\nend_idx: "<< end_idx<< "\ncard: "<<std::to_string(card)<<"\nrw: "<<std::to_string(rw) << "\nnext: " << nextKey;
  }
};

}
