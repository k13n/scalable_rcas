Encoding.default_external = Encoding::UTF_8
Encoding.default_internal = Encoding::UTF_8

max_len = 0
total_len = 0
count = 0

ARGF.each do |line|
  path = line.split(";")[0]
  unless path.nil?
    splits = path[1..-1].split("/")
    max_len = splits.size if splits.size > max_len
    total_len += splits.size
    count += 1
  end
end

puts "max_len: #{max_len}"
puts "avg_len: #{total_len / count.to_f}"
