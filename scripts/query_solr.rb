Query = Struct.new(:path, :vlow, :vhigh) do
  def path_solr
    p = path.gsub /\//, "\\/"
    p.gsub /\*\*$/, ".*"
  end
end

def exec_query(query)
  cmd = "curl \"http://localhost:8983/solr/cas/select?debugQuery=false&q.op=AND&q=path:/#{query.path_solr}/%20AND%20value:[#{query.vlow}%20TO%20#{query.vhigh}]&fl=revision&rows=1000000000\""
  puts cmd
  result = `#{cmd}`
  # puts result.inspect
  runtime = result.match(/"QTime":(\d+),/)[1]
  nr_tuples = result.match(/"numFound":(\d+),/)[1]
  { runtime: runtime, nr_tuples: nr_tuples }
end

measurements = File.readlines(ARGV.shift).collect do |line|
  splits = line.chomp.split(";")
  query = Query.new(splits[0], splits[1].to_i, splits[2].to_i)
  result = exec_query(query)
  { query: query }.merge(result)
end


measurements.each_with_index do |m,i|
  puts "Q#{i};#{m[:nr_tuples]};#{m[:runtime]}"
end
