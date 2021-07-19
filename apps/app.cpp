#include "benchmark/option_parser.hpp"
#include "cas/bulk_loader.hpp"
#include "cas/key_encoder.hpp"
#include "cas/key_decoder.hpp"
#include "cas/node_reader.hpp"
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

  cas::QueryExecutor<VType> query{context.index_file_};
  auto stats = query.Execute(bkey, cas::kNullEmitter);
  stats.Dump();
}


int main_(int /* argc */, char** /* argv */) {
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

  context.index_file_ = "/local/scratch/wellenzohn/workspace/indexes/index.bin.new";
  /* CreateIndex<VType, PAGE_SZ>(context); */
  ReadIndex<VType>(context);

  return 0;
}


int main(int argc, char** argv) {
  try {
    return main_(argc, argv);
  } catch (std::exception& e) {
    std::cerr << "Standard exception. What: " << e.what() << "\n";
    return 10;
  } catch (...) {
    std::cerr << "Unknown exception.\n";
    return 11;
  }
}
