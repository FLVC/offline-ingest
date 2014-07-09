$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '../../lib'))

require 'offin/config'
require 'mono_logger'
require 'watch-queue/watch-directory'

# Resque requires a specialized logger due to interrupt race conditions. Meh.

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
