$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '../../lib'))

require 'rubygems'
require 'offin/config'
require 'mono_logger'
require 'resque'
require 'watch-queue/ingest-job'          # the actual handler corresponding to the 'ingest' queue specified below; the work happens in ingest-job



def setup_redis_connection config_filename
  begin
    config = Datyl::Config.new(config_filename, 'default')
  rescue => e
    raise SystemError, "Can't parse the config file #{config_filename}: #{e.message}"
  end

  raise SystemError, "Config doesn't include the required redis_database parameter" if not config.redis_database

  begin
    client = Redis.new(:url => config.redis_database)
    client.ping # fail fast
    Resque.redis = client
  rescue => e
    raise SystemError, "Can't connect to the redis database: #{e.message}"
  end
end


# Resque requires a specialized logger due to interrupt race
# conditions. Meh. Also: application code does a temporary dup/reopen
# of STDERR, which confuses MonoLogger; luckily we can get away with
# using STDOUT.

def setup_resque_logger
  logger = MonoLogger.new(STDOUT)   # Don't use STDERR here, it gets redirected & reopened under some conditions (ImageMagick error handling) and the logger never recovers the original STDERR stream.
  logger.formatter = proc { |severity, datetime, progname, msg| "#{progname} #{severity}: #{msg}\n" }
  logger.level = Logger::INFO
  Resque.logger = logger
end

def setup_adhoc_logger
  logger = MonoLogger.new(STDOUT)
  logger.formatter = proc { |severity, datetime, progname, msg| "#{progname} #{severity}: #{msg}\n" }
  logger.level = Logger::INFO
  return logger
end

def ftp_root_ok ftp_root
  return (File.exists? ftp_root and File.directory? ftp_root and File.readable? ftp_root and File.writable? ftp_root)
end

def short_package_container_name container, package
  return File.join(container.sub(/.*\//, ''), package.name)
end


# Start a single ingest worker - invokes IngestJob#perform  as needed. Assumes Resque.logger is setup.

def start_ingest_worker sleep_time
  worker = nil
  worker = Resque::Worker.new('ingest')
  worker.term_timeout      = 4.0
  worker.term_child        = 1
  worker.run_at_exit_hooks = 1  # ?!

  if Process.respond_to?('daemon')   # requires ruby >= 1.9"
     Process.daemon(true, true)
     worker.reconnect
  end

  worker.log  "Starting worker #{worker}"
  worker.log  "Worker #{worker.pid} will wakeup every #{sleep_time} seconds"
  worker.work  sleep_time

rescue => e
  Resque.logger.error "Fatal error: #{e.class}: #{e.message}, backtrace follows"
  e.backtrace.each { |line| Resque.logger.error line }
  Resque.logger.error "Please correct the error and restart."
  exit -1
end
