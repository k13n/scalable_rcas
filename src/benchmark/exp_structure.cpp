#include "benchmark/exp_structure.hpp"
#include "cas/bulk_loader.hpp"
#include "cas/index_stats.hpp"
#include "cas/util.hpp"

template<class VType>
benchmark::ExpStructure<VType>::ExpStructure(
      const cas::Context& context)
  : context_(context)
{
}


template<class VType>
void benchmark::ExpStructure<VType>::Execute() {
  cas::util::Log("Experiment ExpStructure\n\n");

  // configuration
  context_.compute_fanout_ = true;

  // print input
  cas::util::Log("Configuration\n");
  context_.Dump();
  std::cout << "\n\n" << std::flush;

  // run benchmark
  cas::BulkLoaderStats stats;
  cas::BulkLoader<VType> bulk_loader{context_, stats};
  bulk_loader.Load();

  // print output
  std::cout << "\n\n\n";
  cas::util::Log("Output:\n\n");
  stats.Dump();
  std::cout << "\n";

  // collect some stats
  cas::Pager pager{context_.index_file_};
  cas::IndexStats index_stats{pager};
  index_stats.Compute();

  PrintOutput(stats.node_fanout_, 32, 256, "Node Fanout");
  PrintOutput(stats.page_fanout_ptrs_, 32, "Page Fanout Pointers");
  PrintOutput(index_stats.internal_depth_histo_, 32, "Internal Depth");
  PrintOutput(index_stats.external_depth_histo_, 32, "External Depth");
  PrintOutput(index_stats.internal_leaf_depth_histo_, 32, "Internal Leaf Depth");
  PrintOutput(index_stats.external_leaf_depth_histo_, 32, "External Leaf Depth");

  std::cout << "\n\n\n"; cas::util::Log("done");
}

template<class VType>
void benchmark::ExpStructure<VType>::PrintOutput(
    const cas::Histogram& histogram,
    int nr_bins,
    const std::string& msg)
{
  PrintOutput(histogram, nr_bins, histogram.MaxValue(), msg);
}


template<class VType>
void benchmark::ExpStructure<VType>::PrintOutput(
    const cas::Histogram& histogram,
    int nr_bins,
    size_t upper_bound,
    const std::string& msg)
{
  std::cout << "\n\n" << msg << " (Stats):\n";
  histogram.PrintStats();
  std::cout << "\n\n" << msg << " (Raw Data):\n";
  histogram.Dump();
  std::cout << "\n\n" << msg << " (Histogram " << nr_bins << " bins):\n";
  histogram.Print(nr_bins, upper_bound);
}

template class benchmark::ExpStructure<cas::vint64_t>;
