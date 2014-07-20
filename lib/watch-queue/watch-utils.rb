$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '../../lib'))

require 'rubygems'
require 'offin/config'
require 'mono_logger'
require 'resque'
require 'watch-queue/ingest-job'          # the actual handler corresponding to the 'ingest' queue specified below; the work happens in ingest-job


class WatchUtils

  def WatchUtils.setup_config config_file=nil
    config_file ||= ENV['CONFIG_FILE']
    raise SystemError, "No configuration file specified (also checked ENV['CONFIG_FILE'])"   if not config_file
    return Datyl::Config.new(config_file, 'default')
  rescue SystemError
    raise
  rescue => e
    raise SystemError, "Can't process the configuration file '#{config_file}';  #{e.class}: #{e.message}"
  end

  def WatchUtils.setup_ingest_database config
    DataBase.setup(config)
  rescue => e
    raise SystemError, "Can't connect to the ingest database; #{e.class}: #{e.message}"
  end

  def WatchUtils.setup_redis_connection config
    raise SystemError, "Configuration doesn't include the required redis_database parameter" if not config.redis_database

    client = Redis.new(:url => config.redis_database)
    client.ping # fail fast
    Resque.redis = client
  rescue SystemError
    raise
  rescue => e
    raise SystemError, "Can't connect to the redis database: #{e.message}"
  end


  # Resque requires a specialized logger due to interrupt race
  # conditions. Meh. Also: our application code does a temporary
  # dup/reopen of STDERR when processing certain image content-types,
  # which confuses MonoLogger; luckily we can get away with using
  # STDOUT.

  def WatchUtils.setup_resque_logger
    logger = MonoLogger.new(STDOUT)   # Don't use STDERR here, it gets redirected & reopened under some conditions (ImageMagick error handling) and the logger never recovers the original STDERR stream.
    logger.formatter = proc { |severity, datetime, progname, msg| "#{progname} #{severity}: #{msg}\n" }
    logger.level = Logger::INFO
    Resque.logger = logger
  end

  def WatchUtils.ftp_directory_problems ftp_root
    errors = []
    dirs = [ ftp_root ] + [ WatchDirectory::ERRORS_SUBDIRECTORY, WatchDirectory::PROCESSING_SUBDIRECTORY, WatchDirectory::WARNINGS_SUBDIRECTORY, WatchDirectory::INCOMING_SUBDIRECTORY ].map { |sub| File.join(ftp_root, sub) }

    dirs.each do |dir|
      unless File.exists? dir
        errors.push "required directory

 '#{di'WatchUtils.setup_r doesn't exist"
        next
      end
      unless File.directory? dir
        errors.push "required directory '#{dir}' isn't actually a directory"
        next
      end
      unless File.readable? dir
        errors.push "required directory '#{dir}' isn't readable"
        next
      end
      unless File.writable? dir
        errors.push "required directory '#{dir}' isn't writable"
        next
      end
    end
    return errors
  end

  def WatchUtils.short_package_container_name container, package
    return File.join(container.sub(/.*\//, ''), package.name)
  end

  # Start a single ingest worker - invokes IngestJob#perform  as needed. Resque.logger must first have been setup

  def WatchUtils.start_ingest_worker sleep_time, *queues
    worker = nil
    worker = Resque::Worker.new(*queues)   # 'ftp', 'digitool' to be supported
    worker.term_timeout      = 4.0
    worker.term_child        = 1
    worker.run_at_exit_hooks = 1  # ?!

    if Process.respond_to?('daemon')   # for ruby >= 1.9"
      Process.daemon(true, true)      # note: likely we'd have to tweak god process monitor, esp. pidfile and restart behavior
      worker.reconnect
    end

    worker.log  "Starting worker #{worker}"
    worker.log  "Worker #{worker.pid} will wakeup every #{sleep_time} seconds"
    worker.work  sleep_time
  end

end # of class WatchUtils
