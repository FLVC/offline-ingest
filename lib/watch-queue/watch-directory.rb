$LOAD_PATH.unshift  File.join(File.dirname(__FILE__), '../../lib')

require 'resque'
require 'fileutils'
require 'offin/exceptions'
require 'watch-queue/ingest-job'
require 'mono_logger'

# We assume that the ingest database has been initialized at this
# point; see new_processing_directory().

# TODO: setup for subclassing into WatchProspectiveDirectory,
# WatchDigitoolDirectory this will probably be hoisting Resque.enqueue
# into 'def enqueue ..' and let the subclass do IngestProspectiveJob
# (currently IngestJob) and IngestDigitoolJob (TBD), presumably
# subclasses themselves

class WatchDirectory

  ERRORS_SUBDIRECTORY      = 'errors'
  PROCESSING_SUBDIRECTORY  = 'processing'
  WARNINGS_SUBDIRECTORY    = 'warnings'
  INCOMING_SUBDIRECTORY    = 'incoming'
  SUCCESS_SUBDIRECTORY     = 'success'

  SHARED_GROUP             = 'ingestor'

  DIRECTORY_UNCHANGED_TIME = 10
# DIRECTORY_UNCHANGED_TIME = 5 * 60  #### TODO

  attr_reader :config_path, :config_section, :incoming_directory, :processing_directory, :warnings_directory, :errors_directory, :hostname

  def initialize config, config_section
    @config_section = config_section
    @config_path    = config.path
    @hostname       = config.site

    @incoming_directory   = File.join(config.ftp_root, INCOMING_SUBDIRECTORY)
    @processing_directory = File.join(config.ftp_root, PROCESSING_SUBDIRECTORY)
    @warnings_directory   = File.join(config.ftp_root, WARNINGS_SUBDIRECTORY)
    @errors_directory     = File.join(config.ftp_root, ERRORS_SUBDIRECTORY)
  end

  def enqueue_incoming_packages
    new_container_directory = new_processing_directory(hostname)

    ready_directories.each do |source|
      package_directory = source.gsub(/.*\//, '')
      begin
        FileUtils.mv source, new_container_directory
      rescue => e
        STDERR.puts "ERROR: Can't move #{package_directory} from #{incoming_directory} to #{new_container_directory} for processing. Skipping."
        STDERR.puts "ERROR: #{e.class}: #{e.message}"
      else
        Resque.enqueue(IngestJob,
                       { :config_section      => config_section,
                         :config_file         => config_path,
                         :container_directory => new_container_directory,
                         :package_directory   => File.join(new_container_directory, package_directory),
                         :warnings_directory  => warnings_directory,
                         :errors_directory    => errors_directory,
                       })
        new_container_directory = new_processing_directory(hostname)
      end
    end
  end

  private

  # Setup directories,  utility

  def self.setup_directories parent
    [ INCOMING_SUBDIRECTORY, WARNINGS_SUBDIRECTORY, ERRORS_SUBDIRECTORY ].each do |sub|
      dir = File.join parent, sub
      FileUtils.mkdir_p dir
      FileUtils.chmod 02775, dir
      FileUtils.chown 0, SHARED_GROUP, dir
    end
    dir = File.join parent, PROCESSING_DIRECTORY

    FileUtils.mkdir_p dir
    FileUtils.chmod 02755, dir
    FileUtils.chown 0, SHARED_GROUP, dir
  end


  def new_processing_directory(hostname)
    new_directory = File.join(processing_directory, DataBase::FtpContainer.next_container_name(hostname))
    FileUtils.mkdir new_directory
    FileUtils.chown nil, SHARED_GROUP, new_directory
    FileUtils.chmod 02775,  new_directory
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


end
