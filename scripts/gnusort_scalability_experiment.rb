require "benchmark"

def execute_gnusort(nr_keys, mem_size, in_file, tmp_folder, out_file)
  cmd = "head -n #{nr_keys} #{in_file} | \
      sort \
        --parallel=1 \
        --temporary-directory=#{tmp_folder} \
        --buffer-size=#{mem_size} \
        --output=#{out_file}"

  puts cmd
  benchmark = Benchmark.measure { system(cmd) }
  puts benchmark.inspect

  return {
    nr_keys: nr_keys,
    file_size: File.size(out_file),
    runtime: benchmark,
  }
end


def execute_experiment(input_sizes, mem_size, in_file, tmp_folder, out_file)
  runtimes = input_sizes.collect do |nr_keys|
    execute_gnusort(nr_keys, mem_size, in_file, tmp_folder, out_file)
  end

  puts "nr_keys;file_size;runtime_s;runtime_h"
  runtimes.each do |data, i|
    puts [
      data[:nr_keys],
      data[:file_size],
      data[:runtime].real,
      data[:runtime].real / (60*60),
    ].join(";")
  end
end


input_sizes = [
     326398505, #  25GB
     657300553, #  50GB
     944912598, #  75GB
    1275111881, # 100GB
    1597548481, # 125GB
    1908875398, # 150GB
    2527310636, # 200GB
    3745580572, # 300GB
    4995194209, # 400GB
    6891972832, # 550GB (full dataset)
]

execute_experiment(input_sizes, *ARGV)
