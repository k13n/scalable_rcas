require "benchmark"

# parameters
PSQL_CMD  = ARGV.shift
DATASET   = ARGV.shift

# table config
TABLE_NAME = "data"
INDEX_NAME = "data_idx_pv"

# memory in base-2, not base-10!
MEM_SIZE = '300000000kB'

# disable parallel index builds
PARALLEL_WORKERS = 0

def exec_pg_cmd(cmd, print_output = false)
  dst = print_output ? "" : "> /dev/null"
  system "#{PSQL_CMD} -c \"#{cmd};\" #{dst}"
end


def exec_and_show_pg_cmd(cmd)
  exec_pg_cmd(cmd, true)
end


def create_db
  exec_pg_cmd <<SQL
DROP TABLE IF EXISTS #{TABLE_NAME};
CREATE TABLE #{TABLE_NAME} (
  kpath TEXT,
  kvalue BIGINT,
  revision TEXT
);
SQL
  # configure database
  exec_pg_cmd "ALTER DATABASE rcas SET work_mem = '#{MEM_SIZE}';";
  exec_pg_cmd "ALTER DATABASE rcas SET maintenance_work_mem = '#{MEM_SIZE}';";
  exec_pg_cmd "ALTER DATABASE rcas SET max_parallel_maintenance_workers = #{PARALLEL_WORKERS};"
end


def show_db_configs
  exec_and_show_pg_cmd "SHOW work_mem;";
  exec_and_show_pg_cmd "SHOW maintenance_work_mem;";
  exec_and_show_pg_cmd "SHOW max_parallel_maintenance_workers;";
end


def import_data(dataset, nr_keys)
  exec_pg_cmd "ALTER TABLE #{TABLE_NAME} SET UNLOGGED;"
    # cut -d ';' -f1,2 | \
  system "head -n #{nr_keys} #{dataset} | \
      #{PSQL_CMD} \
      -c \"COPY #{TABLE_NAME} FROM STDIN WITH (FORMAT 'csv', DELIMITER ';');\" \
      > /dev/null"
  # exec_pg_cmd "ALTER TABLE #{TABLE_NAME} SET LOGGED;"
end


def create_btree
  Benchmark.measure {
    exec_pg_cmd <<SQL
      CREATE INDEX #{INDEX_NAME}
      ON #{TABLE_NAME}
      USING btree (kpath, kvalue)
      WITH (FILLFACTOR = 100);
SQL
  }
end


def compute_btree_size
  cmd = "select pg_table_size('#{INDEX_NAME}')";
  output = `#{PSQL_CMD} -c \"#{cmd};\"`
  splits = output.split("\n")
  splits[2].to_i
end


def run_experiment(input_sizes)
  results = input_sizes.collect do |nr_keys|
    puts "[#{Time.now}] nr_keys: #{nr_keys}"
    create_db
    puts "[#{Time.now}] importing the data..."
    import_data(DATASET, nr_keys)
    puts "[#{Time.now}] creating b-tree..."
    idx_runtime = create_btree
    puts "[#{Time.now}] creating b-tree... done"
    idx_size = compute_btree_size
    puts "runtime: #{idx_runtime}"
    puts "size: #{idx_size}"
    puts "\n\n"
    STDOUT.flush
    { nr_keys: nr_keys, idx_runtime: idx_runtime, idx_size: idx_size }
  end

  puts "nr_keys;runtime_s;runtime_h;size_b;size_gb"
  results.each do |result|
    nr_keys = result[:nr_keys]
    runtime_s = result[:idx_runtime].real
    runtime_h = result[:idx_runtime].real / (60*60)
    size_b = result[:idx_size]
    size_gb = result[:idx_size] / 1_000_000_000.0
    puts "#{nr_keys};#{runtime_s};#{runtime_h};#{size_b};#{size_gb}"
  end
end

run_experiment [
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
