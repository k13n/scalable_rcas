require "benchmark"

def execute_cold_caches(index_file, query_file, nr_repetitions)
  bm = Benchmark.measure do
    nr_repetitions.times do |i|
      puts "clearing page cache"
      system "./scripts/clear_page_cache.sh"
      puts "\n\nexecuting iteration #{i}"
      system "./release/exp_querying --index_file=#{index_file} --query_file=#{query_file}"
    end
  end
  puts "\n\nSummary:"
  puts "index_file: #{index_file}"
  puts "query_file: #{query_file}"
  puts "runtime_s: #{bm.real / nr_repetitions.to_f}"
end


def execute_warm_caches(index_file, query_file, nr_repetitions)
  puts "clearing page cache"
  system "./scripts/clear_page_cache.sh"
  puts "warming up page cache"
  system "./release/exp_querying --index_file=#{index_file} --query_file=#{query_file}"
  bm = Benchmark.measure do
    nr_repetitions.times do |i|
      puts "\n\nexecuting iteration #{i}"
      system "./release/exp_querying --index_file=#{index_file} --query_file=#{query_file}"
    end
  end
  puts "\n\nSummary:"
  puts "index_file: #{index_file}"
  puts "query_file: #{query_file}"
  puts "runtime_s: #{bm.real / nr_repetitions.to_f}"
end

index_file = ARGV.shift
query_file = ARGV.shift

execute_warm_caches(index_file, query_file, 1)
