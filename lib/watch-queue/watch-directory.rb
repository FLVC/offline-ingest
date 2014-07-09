$LOAD_PATH.unshift  File.join(File.dirname(__FILE__), '../../lib')

require 'resque'
require 'fileutils'
require 'offin/exceptions'
require 'watch-queue/ingest-job'
require 'mono_logger'

# We assume that the ingest database has been initialized at this
# point; see new_processing_directory().

class WatchDirectory

  def log message          # TODO: better logging!
    STDERR.puts message
  end

  ERRORS_SUBDIRECTORY      = 'errors'
  PROCESSING_SUBDIRECTORY  = 'processing'
  WARNINGS_SUBDIRECTORY    = 'warnings'
  INCOMING_SUBDIRECTORY    = 'incoming'
  SUCCESS_SUBDIRECTORY     = 'success'

  SHARED_GROUP             = 'ftpil'
  DIRECTORY_UNCHANGED_TIME = 10
# DIRECTORY_UNCHANGED_TIME = 5 * 60

  attr_reader :config_file, :config_section, :incoming_directory, :processing_directory, :warnings_directory, :errors_directory, :hostname, :success_directory

  def initialize config_file, config_section

    @config_file    = config_file
    @config_section = config_section

    config = Datyl::Config.new(config_file, 'default', config_section)

    root = config.ftp_root
    sanity_check(root)

    @hostname = config.site

    @incoming_directory   = File.join(root, INCOMING_SUBDIRECTORY)
    @processing_directory = File.join(root, PROCESSING_SUBDIRECTORY)
    @warnings_directory   = File.join(root, WARNINGS_SUBDIRECTORY)
    @errors_directory     = File.join(root, ERRORS_SUBDIRECTORY)
    @success_directory    = File.join(root, SUCCESS_SUBDIRECTORY)
  end

  def enqueue_incoming_packages
    ready_directories.each do |source|
      package_directory = source.gsub(/.*\//, '')
      container_directory = new_processing_directory(hostname)
      FileUtils.mv source, container_directory
      Resque.enqueue(IngestJob,
                     { :config_section      => config_section,
                       :config_file         => config_file,
                       :package_directory   => File.join(container_directory, package_directory),
                       :container_directory => container_directory,
                       :warnings_directory  => warnings_directory,
                       :errors_directory    => errors_directory,
                       :success_directory   => success_directory,
                     })
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


  def sanity_check directory
    raise "Bad data: ftp directory is class #{directory.class} instead of a filename string" unless directory.class == String

    raise "FTP directory #{directory} doesn't exist."                  unless File.exist? directory
    raise "FTP directory #{directory} isn't actually a directory."     unless File.directory? directory
    raise "FTP Directory #{directory} isn't readable."                 unless File.readable? directory

    dirs = {
      File.join(directory, INCOMING_SUBDIRECTORY)   => "Incoming directory",
      File.join(directory, ERRORS_SUBDIRECTORY)     => "Post-processing error directory",
      File.join(directory, WARNINGS_SUBDIRECTORY)   => "Post-processing warnings directory",
      File.join(directory, PROCESSING_SUBDIRECTORY) => "Processing directory",
    }

    dirs.each do |dir, msg|
      raise "#{msg} #{dir} doesn't exist."                  unless File.exist? dir
      raise "#{msg} #{dir} isn't actually a directory."     unless File.directory? dir
      raise "#{msg} #{dir} isn't readable."                 unless File.readable? dir
      raise "#{msg} #{dir} isn't writable."                 unless File.writable? dir
    end
  end

end
