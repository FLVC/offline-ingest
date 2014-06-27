$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '../../lib'))




# Resque requires a specialized logger due to interrupt race conditions. Meh.

def setup_resque_logger
  logger = MonoLogger.new(STDOUT)
  logger.formatter = proc do |severity, datetime, progname, msg|
    "#{severity} #{datetime.strftime("%Y-%m-%d %H:%M:%S")}: #{msg}\n"
  end

  Resque.logger = logger
  Resque.logger.level = Logger::INFO

  return logger
end


# Search through various stanzas for ftp_root; do sanity check
