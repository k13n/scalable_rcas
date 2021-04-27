def exec_pg_cmd(cmd, print_output = false)
  dst = print_output ? "" : "> /dev/null"
  system "#{PSQL_CMD} -c \"#{cmd};\" #{dst}"
end

def exec_and_show_pg_cmd(cmd)
  exec_pg_cmd(cmd, true)
end


def create_table(table_name)
  exec_pg_cmd <<SQL
DROP TABLE IF EXISTS #{table_name};
CREATE TABLE #{table_name} (
  kpath TEXT,
  kvalue BIGINT,
  revision TEXT
);
SQL
  exec_pg_cmd "ALTER TABLE #{table_name} SET UNLOGGED;"
end


# jump to pos in file and look until the next newline
def bytes_until_next_newline(filename, pos)
  block_size = 1024
  File.open(filename) do |file|
    while pos < File.size(filename)
      file.pos = pos
      data = file.read(block_size)
      data.each_char do |char|
        return pos+1 if char == "\n"
        pos += 1
      end
    end
  end
  pos
end


def import_data(table_name, dataset, dataset_size)
  nr_bytes = bytes_until_next_newline(dataset, dataset_size)
  system "head -c #{nr_bytes} #{dataset} | \
      #{PSQL_CMD} \
      -c \"COPY #{table_name} FROM STDIN WITH (FORMAT 'csv', DELIMITER ';');\" \
      > /dev/null"
end


def create_btree(table_name, order)
  if order == "vp"
    attributes = "kvalue, kpath"
  elsif order == "pv"
    attributes = "kpath, kvalue"
  else
    raise "order must be one of {vp,pv}"
  end
  index_name = "#{table_name}_idx_#{order}"
  exec_pg_cmd <<SQL
    CREATE INDEX #{index_name}
    ON #{table_name}
    USING btree (#{attributes})
    WITH (FILLFACTOR = 100);
SQL
end



# parameters
PSQL_CMD     = ARGV.shift
dataset      = ARGV.shift
dataset_size = ARGV.shift.to_i
table        = ARGV.shift

if dataset_size == 0
  dataset_size = File.size(dataset)
end

$stdout.sync = true
puts "[#{Time.now}] creating table #{table}"
create_table(table)
puts "[#{Time.now}] importing data"
import_data(table, dataset, dataset_size)
puts "[#{Time.now}] creating B+ tree PV"
create_btree(table, "pv")
puts "[#{Time.now}] creating B+ tree VP"
create_btree(table, "vp")
puts "[#{Time.now}] done"
