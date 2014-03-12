class IngestJob
  @queue = :ingest

  def self.perform(institution, ftp_root = '/tmp/fischer')
    STDERR.puts "perform:  package ingest for #{institution} in #{ftp_root}..."
    num = 5 + rand(15)
    sleep num
    raise "perform: oops" if num > 18
    puts "perform: done"
  end

end
