$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '../../lib'))

require 'rubygems'
require 'offin/config'
require 'mono_logger'
require 'resque'
require 'watch-queue/ingest-job'          # the actual handler corresponding to the 'ingest' queue specified below; the work happens in ingest-job
require 'date'

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
    DataBase.debug = true
    DataBase.setup(config)
  rescue => e
    raise SystemError, "Can't connect to the ingest database; #{e.class}: #{e.message}"
  end

  # used by the ftp-handler

  def WatchUtils.record_ftp_user site_hostname,  file_path

    user = WatchUtils.file_owner(file_path)
    site_record = DataBase::IslandoraSite.first_or_create(:hostname => site_hostname)
    user_record = DataBase::FtpUser.first_or_create(:name => user, :islandora_site => site_record)

    unless user_record.save
      errs = []
      site_record.errors.each { |e| errs.push e }
      user_record.errors.each { |e| errs.push e }
      raise SystemError, "Can't save new #{site_hostname} FTP user to database for file #{file_path}: #{errs.join(';')}"
    end

    return [ user_record, site_record ]

  rescue SystemError
    raise
  rescue => e
    raise SystemError, "Can't determine #{site_hostname} FTP user for file #{file_path}: #{e.class} #{e.message}"
  end



  # to do: make this do all-in-one, with ftp-user

  def WatchUtils.record_ftp_package site_hostname,  file_path

    user_record, site_record = WatchUtils.record_ftp_user(site_hostname, file_path)

    time_submitted = File.stat(file_path).ctime

    package_name = file_path.sub(/\/+$/, '').sub(/.*\//, '')

    package_record = DataBase::FtpPackage.first_or_create(:package_name => package_name,  :time_submitted => time_submitted,
                                                          :islandora_site => site_record, :ftp_user => user_record)

    unless package_record.save
      errs = []
      package_record.errors.each { |e| errs.push e }
      user_record.errors.each { |e| errs.push e }
      raise SystemError, "Can't save new #{site_hostname} FTP package to database for file #{file_path}: #{errs.join(';')}"
    end

    return package_record

  rescue SystemError
    raise
  rescue => e
    raise SystemError, "Can't determine #{site_hostname} FTP site for file #{file_path}: #{e.class} #{e.message}"
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
    logger.formatter = proc { |severity, datetime, progname, msg| "#{progname}[#{$$}] #{severity}: #{msg}\n" }
    logger.level = Logger::INFO
    Resque.logger = logger
  end

  def WatchUtils.directory_problems root_dir
    errors = []
    dirs = [ root_dir ] + [ BaseWatchDirectory::ERRORS_SUBDIRECTORY, BaseWatchDirectory::PROCESSING_SUBDIRECTORY, BaseWatchDirectory::WARNINGS_SUBDIRECTORY, BaseWatchDirectory::INCOMING_SUBDIRECTORY ].map { |sub| File.join(root_dir, sub) }

    dirs.each do |dir|
      unless File.exists? dir
        errors.push "required directory '#{dir}' doesn't exist"
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
    raise SystemError, "configuration error: ingest worker didn't receive any queues to watch" if queues.empty?

    worker = nil
    worker = Resque::Worker.new(*queues)   # 'ftp', 'digitool' currently supported
    worker.term_timeout      = 4.0
    worker.term_child        = 1
    worker.run_at_exit_hooks = 1  # ?!

    if Process.respond_to?('daemon')  # for ruby >= 1.9"
      Process.daemon(true, true)      # note: likely we'd have to tweak god process monitor
      worker.reconnect                # config, esp. pidfile and restart behavior
    end

    worker.log  "Starting worker #{worker}"
    worker.log  "Worker #{worker.pid} will wakeup every #{sleep_time} seconds"
    worker.work  sleep_time
  end

  def WatchUtils.setup_environment config
    (ENV['http_proxy'], ENV['HTTP_PROXY'] = config.proxy, config.proxy) if config.proxy
  end


  def WatchUtils.file_owner filepath
    uid = File.stat(filepath).uid
    return Etc.getpwuid(uid).name
  rescue => e
    return 'unknown'
  end

end # of class WatchUtils
