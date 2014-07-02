$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '../../lib'))

require 'offin/config'
require 'mono_logger'
require 'watch-queue/watch-directory'


# Resque requires a specialized logger due to interrupt race conditions. Meh.
# TODO:  it may be we can write directly to STDOUT; the god process monitor will log.

def setup_resque_logger
  logger = MonoLogger.new(STDOUT)
  logger.formatter = proc do |severity, datetime, progname, msg|
    "#{severity} #{datetime.strftime("%Y-%m-%d %H:%M:%S")}: #{msg}\n"
  end

  Resque.logger = logger
  Resque.logger.level = Logger::INFO

  return logger
end


def ftp_root_ok ftp_root
  return (File.exists? ftp_root and File.directory? ftp_root and File.readable? ftp_root and File.writable? ftp_root)
end


# Search through various stanzas for ftp_root; do sanity check and collect up all configs that refer to an ftp_site.

def watch_these_directories  config_file
  wds = []
  Datyl::Config.new(config_file, 'default').all_sections.each do |section|

    site_config = Datyl::Config.new(config_file, 'default', section)
    next unless site_config.ftp_root

    if not ftp_root_ok(site_config.ftp_root)
      STDERR.puts "Permissions error for directory #{site_config.ftp_root}, skipping"
      next
    else
      STDERR.puts "Adding #{site_config.ftp_root} for #{section}"
    end
    wd = get_watch_directory(config_file, section)
    wds.push wd if wd
  end

  return wds
end

# TODO: log error

def get_watch_directory config_file, section
  return WatchDirectory.new(config_file, section)
rescue
  return nil
end
