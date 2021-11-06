Encoding.default_external = Encoding::UTF_8
Encoding.default_internal = Encoding::UTF_8


last_path = nil
nr_lines = 0
nr_unique_paths = 0
ARGF.each do |line|
  path = line.split(";")[0]
  if path != last_path
    nr_unique_paths += 1
    puts path
  end
  nr_lines += 1
  last_path = path
end


STDERR.puts "nr_lines: #{nr_lines}"
STDERR.puts "nr_unique_paths: #{nr_unique_paths}"
