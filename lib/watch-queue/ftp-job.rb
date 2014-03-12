$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '../../lib')

require 'rubygems'
require 'mono_logger'
require 'resque'
require 'watch-queue/ingest-job'
require 'watch-queue/watch-directory'


class FtpJob
  @queue = :ftp

  def self.perform()

    logger  = MonoLogger.new('/tmp/resque.log')   # TODO: syslog as well?  Needs write/close
    logger.level = MonoLogger::DEBUG

    logger.debug("ftp-job: watching '/tmp/fischer'")

    wd = WatchDirectory.new('/tmp/fischer')
    dirs = wd.ready_directories()

    if dirs.empty?
      logger.debug("ftp-job: no recent dirs")
      sleep 5
    else
      logger.debug("ftp-job: processing the following directories: #{dirs.join(', ')}")
    end

    dirs.each do |src|
      dest = File.join(wd.create_new_processing_directory(), File.basename(src))
      logger.debug("ftp-job: moving #{src} to #{dest}")
      FileUtils.mv(src, dest)

      Resque.enqueue(IngestJob, 'fsu')
      logger.debug "ftp-job: enqueued #{dest} for ingest-job"
    end
  end


end
