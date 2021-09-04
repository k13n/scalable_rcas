#include "benchmark/exp_structure.hpp"
#include "cas/bulk_loader.hpp"
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
  context_.compute_depth_ = true;
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

  std::cout << "Node Depth Histogram:\n";
  stats.node_depth_.Dump();

  std::cout << "\nNode Fanout Histogram:\n";
  stats.node_fanout_.Dump();

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
