# n is the number of keys in the input
# m is the number of keys that fit into memory
# b is the number of keys per page
# f is the fanout
def all_or_nothing(n, m, b, f)
  nb = (n/b.to_f).ceil
  mb = (m/b.to_f).ceil
  nb + (2*nb * Math.log((n/m.to_f).ceil, f).ceil)
end

# n is the number of keys in the input
# m is the number of keys that fit into memory
# b is the number of keys per page
# f is the fanout
def frontloading_bestcase(n, m, b, f)
  nb = (n/b.to_f).ceil
  mb = (m/b.to_f).ceil
  height = Math.log((n/m.to_f).ceil, f).ceil
  (nb - mb) + 2 * (1..height).reduce(0) do |sum, i|
    sum + (nb - f**(i-1) * mb)
  end
end


# n is the number of keys in the input
# m is the number of keys that fit into memory
# b is the number of keys per page
def frontloading_worstcase(n, m, b, f)
  n = n.to_f
  m = m.to_f
  b = b.to_f
  height = n - m + b
  (n/b).ceil - (m/b).ceil + 2 * (1..height).reduce(0) do |sum, i|
    cost = ((n-i)/b).ceil - (m/b).ceil + 1
    sum + cost
  end
end


def estimate(nr_keys, mem_size, page_size, avg_key_len, fanout, method)
  nr_keys_per_page = page_size / avg_key_len
  nr_mem_keys = mem_size / avg_key_len
  page_io_overhead = send(method,
                          nr_keys,
                          nr_mem_keys,
                          nr_keys_per_page,
                          fanout)
  page_io_overhead * page_size
end


def benchmark(dataset_sizes, memory_sizes, page_size, avg_key_len, fanout, approach)
  # configuration
  sep = "\t"
  puts "dataset_size_gb#{sep}" +
    "memory_size_gb#{sep}" +
    "fanout#{sep}" +
    "io_overhead#{sep}" +
    "io_overhead_tb"
  # evaluation
  dataset_sizes.each do |dataset_size|
    nr_keys = dataset_size / avg_key_len
    memory_sizes.each do |memory_size|
      cost = estimate(
        nr_keys,
        memory_size,
        page_size,
        avg_key_len,
        fanout,
        approach
      )
      dataset_size_gb = dataset_size / 1_000_000_000.0
      memory_size_gb = memory_size / 1_000_000_000.0
      cost_gb = cost / 1_000_000_000.0
      cost_tb = cost_gb / 1_000.0
      puts [
        dataset_size_gb,
        memory_size_gb,
        fanout,
        cost,
        cost_tb,
      ].join sep
    end
  end
end


page_size = 2**14
avg_key_len = 80
fanout = 10

dataset_sizes = [
  100_000_000_000,
  200_000_000_000,
  300_000_000_000,
  400_000_000_000,
  550_263_603_200,
]
default_dataset_sizes = [
  550_263_603_200,
]

memory_sizes = [
  16_000_000_000,
  32_000_000_000,
  64_000_000_000,
  128_000_000_000,
  256_000_000_000,
]
default_memory_sizes = [
  50_000_000_000,
]

puts "Varying input size"
benchmark(
  dataset_sizes,
  default_memory_sizes,
  page_size,
  avg_key_len,
  fanout,
  :frontloading_bestcase
)

puts "\n\nVarying memory size"
benchmark(
  default_dataset_sizes,
  memory_sizes,
  page_size,
  avg_key_len,
  fanout,
  :frontloading_bestcase
)
