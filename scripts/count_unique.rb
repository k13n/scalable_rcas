Encoding.default_external = Encoding::UTF_8
Encoding.default_internal = Encoding::UTF_8

raise "basename expected" if ARGV.empty?
basename = ARGV.shift

begin
  f_key    = File.open("#{basename}_keys.csv",   'w')
  f_path   = File.open("#{basename}_paths.csv",  'w')
  f_value  = File.open("#{basename}_values.csv", 'w')

  ARGF.each do |line|
    splits = line.split(";")
    f_key.puts splits[0..1].join(";")
    f_path.puts splits[0]
    f_value.puts splits[1]
  end
ensure
  f_key.close   unless f_key.nil?
  f_path.close  unless f_path.nil?
  f_value.close unless f_value.nil?
end

