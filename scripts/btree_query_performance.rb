require "benchmark"

def execute_cold_caches(query_file, table, index, nr_repetitions)
  bm = Benchmark.measure do
    nr_repetitions.times do |i|
      puts "clearing page cache"
      system "./scripts/clear_page_cache.sh"
      puts "\n\nexecuting iteration #{i}"
      system "java -cp scripts/postgresql-42.2.19.jar scripts/BtreeQueryPerformance.java #{query_file} #{table} #{index}"
    end
  end
  puts "\n\nSummary:"
  puts "query_file: #{query_file}"
  puts "runtime_s: #{bm.real / nr_repetitions.to_f}"
end


def execute_warm_caches(query_file, table, index, nr_repetitions)
  puts "clearing page cache"
  system "./scripts/clear_page_cache.sh"
  puts "warming up page cache"
  system "java -cp scripts/postgresql-42.2.19.jar scripts/BtreeQueryPerformance.java #{query_file} #{table} #{index}"
  bm = Benchmark.measure do
    nr_repetitions.times do |i|
      puts "\n\nexecuting iteration #{i}"
      system "java -cp scripts/postgresql-42.2.19.jar scripts/BtreeQueryPerformance.java #{query_file} #{table} #{index}"
    end
  end
  puts "\n\nSummary:"
  puts "query_file: #{query_file}"
  puts "runtime_s: #{bm.real / nr_repetitions.to_f}"
end


query_file = ARGV.shift
table      = ARGV.shift
index      = ARGV.shift

unless ["vp","pv"].include? index
  puts "third argument must be one of {vp,pv}"
  exit
end

execute_warm_caches(query_file, table, index, 10)
