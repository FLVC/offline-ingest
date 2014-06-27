require 'resque'
require 'offlin/config'
require 'offlin/packages'


class IngestJob
  @queue = :ingest

  def self.perform(config)
    sleep 2
    STDERR.puts "perform config: #{config.inspect}"     # handle config
  rescue  Resque::TermException
    STDERR.puts "Received TERM"
  rescue => e
    STDERR.puts "Received #{e.class}: #{e.message}"
  end

end
