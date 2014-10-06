$LOAD_PATH.unshift  File.join(File.dirname(__FILE__), '../../lib')

require 'resque'
require 'fileutils'
require 'offin/exceptions'
require 'watch-queue/ingest-job'
require 'mono_logger'
require 'socket'


# We assume that the ingest database has been initialized at this
# point; see new_processing_directory().

# TODO: design bug,  we want to be able to sort a variety of subdirectories across several 'watch directories'.  Pluralize!


class BaseWatchDirectory

  ERRORS_SUBDIRECTORY      = 'errors'
  INCOMING_SUBDIRECTORY    = 'incoming'
  PROCESSING_SUBDIRECTORY  = 'processing'
  SUCCESS_SUBDIRECTORY     = 'success'
  WARNINGS_SUBDIRECTORY    = 'warnings'

  DIRECTORY_UNCHANGED_TIME = 30
# DIRECTORY_UNCHANGED_TIME = 5 * 60  #### Use this variable on launch

  attr_reader :config_path, :config_section, :incoming_directory, :processing_directory, :warnings_directory, :errors_directory, :hostname

  def initialize config, config_section = nil
    @config_section = config_section   # nil when global/default :digitool queue
    @config_path    = config.path
    @hostname       = config.site || Socket.gethostname   # nil when global/default :digitool queue
  end

  def enqueue_incoming_packages
    ready_directories.each do |source|
      new_container_directory = new_processing_directory(hostname)
      package_directory = source.gsub(/.*\//, '')
      begin
        FileUtils.mv source, new_container_directory
      rescue => e
        STDERR.puts "ERROR: Can't move #{package_directory} from #{incoming_directory} to newly created #{new_container_directory} for processing. Skipping."
        STDERR.puts "ERROR: #{e.class}: #{e.message}"
        STDERR.puts "ERROR: removing unused #{new_container_directory}"
        cleanup_unused_container new_container_directory
      else
        resque_enqueue new_container_directory, package_directory
      end
    end
  end


  private

  def resque_enqueue container_directory, package_directory
    raise "INVALID PROGRAMMER ERROR:  resque_enqueue called from base class (don't instantiate the #{self} base class)"
  end

  def cleanup_unused_container dir
    FileUtils.rmdir dir
  rescue => e
    STDERR.puts "ERROR: Can't remove unused directory #{dir} - #{e.class}: #{e.message}"
  end

  def new_processing_directory(hostname)
    new_directory = File.join(processing_directory, DataBase::FtpContainer.next_container_name(hostname))
    FileUtils.mkdir new_directory
####   FileUtils.chown nil, SHARED_GROUP, new_directory          # we don't really need to do this; set-gid on directory should get this right
###    FileUtils.chmod 02775,  new_directory
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
    return (now - latest_change) > DIRECTORY_UNCHANGED_TIME
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
  def initialize config, config_section

    super(config, config_section)
    @errors_directory     = File.join(config.ftp_root, ERRORS_SUBDIRECTORY)
    @incoming_directory   = File.join(config.ftp_root, INCOMING_SUBDIRECTORY)
    @processing_directory = File.join(config.ftp_root, PROCESSING_SUBDIRECTORY)
    @warnings_directory   = File.join(config.ftp_root, WARNINGS_SUBDIRECTORY)
  end

  def resque_enqueue container_directory, package_directory
    Resque.enqueue(ProspectiveIngestJob,
                   { :config_section      => config_section,
                     :config_file         => config_path,
                     :container_directory => container_directory,
                     :errors_directory    => errors_directory,
                     :package_directory   => File.join(container_directory, package_directory),
                     :warnings_directory  => warnings_directory,
                   })
  end

end

class DigiToolWatchDirectory < BaseWatchDirectory
  def initialize config

    super(config, nil)
    @errors_directory     = File.join(config.digitool_root, ERRORS_SUBDIRECTORY)
    @incoming_directory   = File.join(config.digitool_root, INCOMING_SUBDIRECTORY)
    @processing_directory = File.join(config.digitool_root, PROCESSING_SUBDIRECTORY)
    @warnings_directory   = File.join(config.digitool_root, WARNINGS_SUBDIRECTORY)
  end

  def resque_enqueue container_directory, package_directory
    Resque.enqueue(DigiToolIngestJob,
                   { :config_file         => config_path,
                     :container_directory => container_directory,
                     :errors_directory    => errors_directory,
                     :package_directory   => File.join(container_directory, package_directory),
                     :warnings_directory  => warnings_directory,
                   })
  end

end
