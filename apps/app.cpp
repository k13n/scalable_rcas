#include "benchmark/option_parser.hpp"
#include "cas/bulk_loader.hpp"
#include "cas/index.hpp"
#include "cas/key_decoder.hpp"
#include "cas/key_encoder.hpp"
#include "cas/mem/insertion.hpp"
#include "cas/mem/node.hpp"
#include "cas/node_reader.hpp"
#include "cas/partition.hpp"
#include "cas/query.hpp"
#include "cas/query_executor.hpp"
#include "cas/search_key.hpp"

template<class VType, size_t PAGE_SZ>
void CreateIndex(const cas::Context& context) {
  // create index
  cas::Pager<PAGE_SZ> pager{context.index_file_};
  cas::BulkLoader<VType, PAGE_SZ> bulk_loader{pager, context};
  bulk_loader.Load();

  // print output
  std::cout << "Output:\n";
  auto stats = bulk_loader.Stats();
  stats.Dump();

}


template<class VType>
void ReadIndex(const cas::Context& context) {
  // std::string path = "/arch/arm/boot/**";
  std::string path = "/drivers/hid/**";
  VType low  = 1455182876;
  VType high = 1555182879;

  cas::SearchKey<VType> skey{path, low, high};
  auto bkey = cas::KeyEncoder<VType>::Encode(skey, false);

  cas::BinaryKeyEmitter emitter = [&](
      const cas::QueryBuffer& path, size_t p_len,
      const cas::QueryBuffer& value, size_t v_len,
      cas::ref_t ref) -> void {
    auto key = cas::KeyDecoder<VType>::Decode(path, p_len, value, v_len, ref);
    key.Dump();
  };

  cas::QueryExecutor query{context.index_file_};
  auto stats = query.Execute(bkey, cas::kNullEmitter);
  stats.Dump();
}


void DiskBasedIndex() {
  using VType = cas::vint64_t;
  constexpr auto PAGE_SZ = cas::PAGE_SZ_16KB;

  // read input configuration
  cas::Context context = {
    /* .input_filename_ = "/home/user/wellenzohn/datasets/dataset.16384.partition", */
    .input_filename_ = "/local/scratch/wellenzohn/datasets/dataset.10GB.16384.partition",
    .partition_folder_ = "/local/scratch/wellenzohn/workspace/partitions/",
    .index_file_ = "/local/scratch/wellenzohn/workspace/indexes/index.bin",
    .mem_size_bytes_ = 1'000'000'000,
    .dataset_size_ = 16384*100000,
    /* .dataset_size_ = 16384*10000, */
    .partitioning_threshold_ = 200,
    /* .dataset_size_ = 16384, */
  };
  std::cout << "Configuration:\n";
  context.Dump();
  std::cout << std::endl;

  context.index_file_ = "/local/scratch/wellenzohn/workspace/indexes/index.100GB.bin.new";
  /* CreateIndex<VType, PAGE_SZ>(context); */
  ReadIndex<VType>(context);
}


void MemoryBasedIndex() {
  using VType = cas::vint64_t;
  constexpr auto PAGE_SZ = cas::PAGE_SZ_16KB;

  cas::mem::Node* root = nullptr;
  size_t partitioning_threshold = 2;

  const std::string filename = "/local/scratch/wellenzohn/datasets/dataset.10GB.16384.partition";
  cas::BulkLoaderStats stats;
  cas::Context context;
  cas::Partition<PAGE_SZ> partition{filename, stats, context};

  std::vector<std::byte> page_buffer;
  page_buffer.resize(PAGE_SZ);
  cas::MemoryPage<PAGE_SZ> io_page{&page_buffer[0]};
  auto cursor = partition.Cursor(io_page);

  /* cursor.FetchNextDiskPage(); */
  /* auto it = io_page.begin(); */
  /* for (int i = 0; i < 100; ++i) { */
  /*   auto key = *it; */
  /*   ++it; */
  /*   key.Dump(); */
  /*   cas::mem::Insertion insertion{&root, key, partitioning_threshold}; */
  /*   insertion.Execute(); */
  /*   root->DumpRecursive(); */
  /*   std::cout << "\n"; */
  /* } */

  /* cursor.FetchNextDiskPage(); */
  /* for (auto key : io_page) { */
  /*   key.Dump(); */
  /*   cas::mem::Insertion insertion{&root, key, partitioning_threshold}; */
  /*   insertion.Execute(); */
  /* } */

  int i = 0;
  while (cursor.HasNext()) {
    auto it = io_page.begin();
    for (auto key : io_page) {
      /* std::cout << (++i) << std::endl; */
      /* if (i == 2132) { */
      /*   std::cout << "\n"; */
      /*   root->DumpRecursive(); */
      /*   key.Dump(); */
      /*   std::cout << std::endl; */
      /* } */
      cas::mem::Insertion insertion{&root, key, partitioning_threshold};
      insertion.Execute();
    }
  }

  std::string path = "/**";
  VType low  = cas::VINT64_MIN;
  VType high = cas::VINT64_MAX;
  cas::SearchKey<VType> skey{path, low, high};
  auto bkey = cas::KeyEncoder<VType>::Encode(skey, false);
  auto emitter = [&](
          const cas::QueryBuffer& path, size_t p_len,
          const cas::QueryBuffer& value, size_t v_len,
          cas::ref_t ref) -> void {
    auto tuple = cas::KeyDecoder<VType>::Decode(path, p_len, value, v_len, ref);
    std::cout << tuple.path_ << ";" << tuple.value_ << ";" << cas::ToString(ref) << "\n";
  };
  cas::Query query{root, bkey, emitter};
  query.Execute();
}


void TestIndexingPipeline() {
  using VType = cas::vint64_t;
  constexpr auto PAGE_SZ = cas::PAGE_SZ_16KB;

  cas::Context context = {
    /* .input_filename_ = "/home/user/wellenzohn/datasets/dataset.16384.partition", */
    .input_filename_ = "/local/scratch/wellenzohn/datasets/dataset.10GB.16384.partition",
    .partition_folder_ = "/local/scratch/wellenzohn/workspace/partitions/",
    .index_file_ = "/local/scratch/wellenzohn/workspace/pipeline/index.bin",
    .max_memory_keys_ = 100'000,
    .mem_size_bytes_ = 1'000'000'000,
    /* .dataset_size_ = 16384*100000, */
    .dataset_size_ = 16384*1,
    .partitioning_threshold_ = 200,
  };

  cas::Index<VType, PAGE_SZ> index{context};

  cas::BulkLoaderStats stats;
  cas::Partition<PAGE_SZ> partition{context.input_filename_, stats, context};
  std::vector<std::byte> page_buffer;
  page_buffer.resize(PAGE_SZ);
  cas::MemoryPage<PAGE_SZ> io_page{&page_buffer[0]};
  auto cursor = partition.Cursor(io_page);

  size_t limit = 10'000'000;
  size_t nr_keys = 0;
  while (cursor.HasNext() && nr_keys < limit) {
    auto it = io_page.begin();
    for (auto key : io_page) {
      index.Insert(key);
      ++nr_keys;
      if (nr_keys == limit) {
        break;
      }
    }
  }

  index.FlushMemoryResidentKeys();

  /* while (cursor.HasNext()) { */
  /*   auto it = io_page.begin(); */
  /*   for (auto key : io_page) { */
			/* /1* key.Dump(); *1/ */
  /*     index.Insert(key); */
  /*   } */
  /* } */
}



int main_(int /* argc */, char** /* argv */) {
  /* DiskBasedIndex(); */
  /* MemoryBasedIndex(); */
  TestIndexingPipeline();
  return 0;
}


int main(int argc, char** argv) {
  try {
    return main_(argc, argv);
  } catch (std::exception& e) {
    std::cerr << "Standard exception. What: " << e.what() << std::endl;
    return 10;
  } catch (...) {
    std::cerr << "Unknown exception" << std::endl;
    return 11;
  }
}
