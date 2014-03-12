$LOAD_PATH.unshift  File.join(File.dirname(__FILE__), '../../lib')

require 'resque'
require 'fileutils'
require 'watch-queue/exceptions'
require 'watch-queue/ingest-job'
require 'mono_logger'

class WatchDirectory

  ERRORS_DIRECTORY         = 'submission-errors'
  PROCESSING_DIRECTORY     = 'submission-processing'
  SHARED_GROUP             = 'everyone'

# DIRECTORY_UNCHANGED_TIME = 5 * 60                 # directory unchanged for at least 5 minutes...

  DIRECTORY_UNCHANGED_TIME = 15 # seconds

  attr_reader :drop_directory

  # TODO: make this multiple directories to watch?


  #

  def initialize institution_directories
    # institution_directories  is a hash, using valid institution codes (e.g. 'fsu', 'fau'...)


    raise "Directory #{dir} doesn't exist."    unless File.exist? dir
    raise "#{dir} isn't a directory."          unless File.directory? dir
    raise "Directory #{dir} isn't readable."   unless File.readable? dir
    raise "Directory #{dir} isn't writable."   unless File.writable? dir

    @drop_directory = dir
  end


  def create_new_processing_directory
    return create_new_directory(File.join(drop_directory, PROCESSING_DIRECTORY))
  rescue => e
    raise SystemError,  "create_new_processing_directory: #{e.class} -- #{e.message}"
  end

  def create_new_errors_directory
    return create_new_directory(File.join(drop_directory, ERRORS_DIRECTORY))
  rescue => e
    raise SystemError,  "create_new_errors_directory: #{e.class} -- #{e.message}"
  end

  def create_new_directory top
    FileUtils.mkdir_p(top)

    list = Dir.open(top).select{ |name| name =~ /^[a-z]{6}$/ }

    next_dir = list.empty? ? 'aaaaaa' : list.sort.pop.succ
    new_dir  = File.join(top, next_dir)

    FileUtils.mkdir_p new_dir
    FileUtils.chown nil, SHARED_GROUP, new_dir
    FileUtils.chmod  02775,  new_dir

    return new_dir
  end

  def log message
    STDERR.puts message
  end


  def not_recently_changed dir
    (Time.now - File.stat(dir).ctime) > DIRECTORY_UNCHANGED_TIME
  end

  # In the drop directory, we expect only subdirectories, each filled
  # with plain files (TODO: check and move to errors directory if
  # not).


  def ready_directories
    directories = []
    Dir.open(drop_directory).each do |fl|
      next if [ '.', '..', PROCESSING_DIRECTORY,  ERRORS_DIRECTORY ].include? fl
      path = File.join(drop_directory, fl)
      directories.push(path) if File.directory?(path) and not_recently_changed(path)
    end
    return directories.sort {  |a,b|  File.stat(b).ctime <=> File.stat(a).ctime }
  end



  # TODO:  check that our directories are OK...

end
