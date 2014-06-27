$LOAD_PATH.unshift  File.join(File.dirname(__FILE__), '../../lib')


### TODO: create subdirectories insofar as we can...

require 'resque'
require 'fileutils'
require 'watch-queue/exceptions'
require 'watch-queue/ingest-job'
require 'mono_logger'

# TODO:
# def initialize institution_directories
# institution_directories  is a hash, using valid institution codes (e.g. 'fsu', 'fau'...)


# Assumes that the ingest database has been initialilzed

class WatchDirectory

  def log message          # TODO: better logging!
    STDERR.puts message
  end

  # Ideally these are all on the same filesystem

  ERRORS_SUBDIRECTORY         = 'errors'
  PROCESSING_SUBDIRECTORY     = 'processing'
  WARNINGS_SUBDIRECTORY       = 'warnings'
  INCOMING_SUBDIRECTORY       = 'incoming'

  SHARED_GROUP                = 'ftpil'

  # DIRECTORY_UNCHANGED_TIME = 5 * 60                 # directory and contents should be unchanged for at least 5 minutes...

  DIRECTORY_UNCHANGED_TIME = 10 # seconds - this is the tricky one

  attr_reader :drop_directory, :incoming_directory, :processing_directory, :warnings_directory, :errors_directory

  def initialize hostname, dir
    sanity_check(dir)
    @hostname = hostname

    @incoming_directory   = File.join(dir, INCOMING_SUBDIRECTORY)
    @processing_directory = File.join(dir, PROCESSING_SUBDIRECTORY)
    @warnings_directory   = File.join(dir, WARNINGS_SUBDIRECTORY)
    @errors_directory     = File.join(dir, ERRORS_SUBDIRECTORY)
  end


  def enqueue_incoming_packages
    ready_directories.each do |source|
      destination = new_processing_directory
      FileUtils.mv source, destination
      Resque.enqueue(IngestJob, { :package_directory  => destination, :warnings_directory => @warnings_directory, :errors_directory   => @errors_directory })
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



  def new_processing_directory
    new_directory = File.join(@processing_directory, DataBase::FtpContainer.next_container_name(@hostname))
    STDERR.puts  "New directory: #{new_directory}"
    FileUtils.mkdir new_directory
    FileUtils.chown nil, SHARED_GROUP, new_directory
    FileUtils.chmod 02775,  new_directory
    return new_directory
  end

  def not_recently_changed dir
    now = Time.now
    times = [ File.stat(dir).ctime ]  # ctime, mtime, doesn't matter on directories

    # We check one level down, since that's all we really support (no subdirectories in packages)

    Dir.open(dir).each do |fl|
      next if ['.', '..'].include? fl
      times.push File.stat(File.join(dir, fl)).ctime  # ctime catches more changes than mtime on plain files, e.g. rename.
    end
    latest_change = times.sort.pop
    (now - latest_change) > DIRECTORY_UNCHANGED_TIME
  end

  # In the drop directory, we expect only subdirectories, each filled
  # with plain files (TODO: check and move to errors directory if
  # not).

  def ready_directories
    directories = []
    Dir.open(@incoming_directory).each do |fl|
      next if [ '.', '..' ].include? fl
      path = File.join(@incoming_directory, fl)
      directories.push(path) if File.directory?(path) and not_recently_changed(path)
    end
    return directories.sort {  |a,b|  File.stat(b).ctime <=> File.stat(a).ctime }
  end


  def sanity_check directory

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
