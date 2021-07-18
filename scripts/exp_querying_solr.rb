require "benchmark"

SOLR_JAR = "/local/scratch/wellenzohn/code/solrquery/target/solrquery-1.0-SNAPSHOT-jar-with-dependencies.jar"

def execute_cold_caches(query_file, nr_repetitions)
  bm = Benchmark.measure do
    nr_repetitions.times do |i|
      puts "clearing page cache"
      system "./scripts/clear_page_cache.sh"
      puts "\n\nexecuting iteration #{i}"
      system "java -jar #{SOLR_JAR} #{query_file}"
    end
  end
  puts "\n\nSummary:"
  puts "query_file: #{query_file}"
  puts "runtime_s: #{bm.real / nr_repetitions.to_f}"
end


def execute_warm_caches(query_file, nr_repetitions)
  puts "clearing page cache"
  system "./scripts/clear_page_cache.sh"
  puts "warming up page cache"
  system "java -jar #{SOLR_JAR} #{query_file}"
  bm = Benchmark.measure do
    nr_repetitions.times do |i|
      puts "\n\nexecuting iteration #{i}"
      system "java -jar #{SOLR_JAR} #{query_file}"
    end
  end
  puts "\n\nSummary:"
  puts "query_file: #{query_file}"
  puts "runtime_s: #{bm.real / nr_repetitions.to_f}"
end

query_file = ARGV.shift

execute_warm_caches(query_file, 2)
