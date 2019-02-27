$LOAD_PATH.unshift  File.join(File.dirname(__FILE__), '../../lib')

require 'resque'
require 'fileutils'
require 'offin/exceptions'
require 'offin/ingest-support'
require 'watch-queue/ingest-job'
require 'mono_logger'
require 'socket'
require 'watch-queue/constants'

# We assume that the ingest database has been initialized at this
# point; see new_processing_directory().

# TODO: design bug,  we want to be able to sort a variety of subdirectories across several 'watch directories'.  Pluralize!


class BaseWatchDirectory

  include WatchConstants

  attr_reader :config, :config_section, :incoming_directory, :processing_directory, :warnings_directory, :errors_directory, :hostname

  def initialize(config, config_section, delay)
    @config  = config
    @config_section   = config_section
    @hostname         = config.site || Socket.gethostname
    @directory_delay  = delay

    # subclasses fill in @incoming_directory, @processing_directory, @warnings_directory, @errors_directory
  end

  def enqueue_incoming_packages
    ready_directories.each do |source|
      container_name = DataBase::FtpContainer.next_container_name(hostname)
      new_container_directory = new_processing_directory(container_name)
      package_name = File.basename(source)
      begin
        FileUtils.mv source, new_container_directory
        record_to_database_queued(@config.site, package_name, Time.now)
      rescue => e
        STDERR.puts "ERROR: Can't move the package #{package_name} from #{incoming_directory} to newly created #{new_container_directory} for processing. Skipping."
        STDERR.puts "ERROR: #{e.class}: #{e.message}"
        STDERR.puts "ERROR: removing unused #{new_container_directory}"
        STDERR.puts "ERROR: sleeping 5 minutes"
        cleanup_unused_container new_container_directory
        sleep 5 * 60
      else
        resque_enqueue container_name,  package_name
      end
    end
  end


  private

  def resque_enqueue container_name, package_name
    raise "INVALID PROGRAMMER ERROR:  resque_enqueue called from base class (don't instantiate the #{self} base class)"
  end

  def cleanup_unused_container dir
    FileUtils.rmdir dir
  rescue => e
    STDERR.puts "ERROR: Can't remove unused directory #{dir} - #{e.class}: #{e.message}"
  end

  def new_processing_directory(container_name)
    new_directory = File.join(processing_directory, container_name)
    FileUtils.mkdir new_directory
    return new_directory
  rescue => e
    raise SystemError, "Encountered a fatal error when creating a new processing directory for #{hostname}: #{e.class} - #{e.message}"
  end

  def not_recently_changed dir
    now = Time.now
    times = [ File.stat(dir).ctime ]  # ctime, mtime, doesn't matter which we use on directories

    # We check one level down, since that's all we really support (no subdirectories in packages)

    Dir.open(dir).each do |fl|
      next if ['.', '..'].include? fl
      times.push File.stat(File.join(dir, fl)).ctime  # ctime catches more changes than mtime on plain files, e.g. rename.
    end
    latest_change = times.sort.pop
    return (now - latest_change) > @directory_delay
  end

  # In the drop directory, we expect only subdirectories, each filled
  # with plain files (TODO: check and move to errors directory if
  # not).

  def ready_directories
    directories = []
    Dir.open(incoming_directory).each do |fl|
      next if [ '.', '..' ].include? fl
      path = File.join(incoming_directory, fl)
      directories.push(path) if File.directory?(path) and not_recently_changed(path)
    end
    return directories.sort {  |a,b|  File.stat(b).ctime <=> File.stat(a).ctime }
  end

end # class WatchDirectory


class FtpWatchDirectory < BaseWatchDirectory
  def initialize(config, config_section)

    super(config, config_section, FTP_DIRECTORY_DELAY)

    @errors_directory     = File.join(config.ftp_queue, ERRORS_SUBDIRECTORY)
    @incoming_directory   = File.join(config.ftp_queue, INCOMING_SUBDIRECTORY)
    @processing_directory = File.join(config.ftp_queue, PROCESSING_SUBDIRECTORY)
    @warnings_directory   = File.join(config.ftp_queue, WARNINGS_SUBDIRECTORY)
  end

  def resque_enqueue container_name, package_name

    # e.g.
    # 'config_file'    => '/usr/local/islandora/offline-ingest/config.yml'
    # 'config_section' => 'uf-test'
    # 'container'      => 'aabz'
    # 'package'        => 'UCF2350135C'
    # 'qroot'          => '/data/ftpdl/UF'

    Resque.enqueue(ProspectiveIngestJob,
                   { :config_section      => config_section,
                     :config_file         => config.path,
                     :container           => container_name,
                     :qroot               => config.ftp_queue,
                     :package             => package_name
                   })
  end
end

class DigitoolWatchDirectory < BaseWatchDirectory
  def initialize(config, config_section)

    super(config, config_section, FILESYSTEM_DIRECTORY_DELAY)

    @errors_directory     = File.join(config.digitool_queue, ERRORS_SUBDIRECTORY)
    @incoming_directory   = File.join(config.digitool_queue, INCOMING_SUBDIRECTORY)
    @processing_directory = File.join(config.digitool_queue, PROCESSING_SUBDIRECTORY)
    @warnings_directory   = File.join(config.digitool_queue, WARNINGS_SUBDIRECTORY)
  end

  def resque_enqueue(container_name, package_name)

    # e.g.
    # 'config_file'    => '/usr/local/islandora/offline-ingest/config.yml'
    # 'config_section' => 'uf-test'
    # 'container'      => 'aabz'
    # 'package'        => 'UCF2350135C'
    # 'qroot'          => '/data/digitool/UF'

    Resque.enqueue(DigitoolIngestJob,
                   { :config_section      => config_section,
                     :config_file         => config.path,
                     :container           => container_name,
                     :qroot               => config.digitool_queue,
                     :package             => package_name
                   })
  end

end
