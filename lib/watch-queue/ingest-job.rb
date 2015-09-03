require 'resque'
require 'offin/config'
require 'offin/packages'
require 'offin/exceptions'
require 'offin/ingest-support'
require 'watch-queue/watch-utils'
require 'watch-queue/constants'
require 'mono_logger'

class BaseIngestJob

  include WatchConstants


  def self.around_perform(data)

    # e.g.
    #
    # data['config_file']     =>   '/usr/local/islandora/offline-ingest/config.yml'
    # data['config_section']  =>   'uf-test'
    # data['container']       =>   'aabz'
    # data['package']         =>   'UCF2350135C'
    # data['qroot']           =>   '/data/digitool/UF'

    container_directory = File.join(data['qroot'], PROCESSING_SUBDIRECTORY, data['container'])
    errors_directory    = File.join(data['qroot'], ERRORS_SUBDIRECTORY)
    package_directory   = File.join(container_directory, data['package'])

    yield

    # TODO: experimental control-c /sigterm handling...

  rescue SystemExit, Interrupt => e
    self.failsafe(container_directory, errors_directory)
    Resque.logger.error "Terminating #{package_directory}: #{e.message}"
    raise e

  rescue SystemError => e
    self.failsafe(container_directory, errors_directory)
    Resque.logger.error "System error when processing #{package_directory}, can't continue processing package: #{e.message}"

  rescue PackageError => e
    self.failsafe(container_directory, errors_directory)
    Resque.logger.error "Package error when processing #{package_directory}, can't continue processing package: #{e.message}"

  rescue => e
    self.failsafe(container_directory, errors_directory)
    Resque.logger.error "Caught unhandled error when processing #{package_directory}: #{e.class} - #{e.message}, backtrace follows:"
    e.backtrace.each { |line| Resque.logger.error line }
    Resque.logger.error "Please correct the error and restart the package.."

  ensure
    self.failsafe(container_directory, errors_directory)
  end

  def self.failsafe(container_directory, errors_directory)
    return unless File.exists? container_directory
    FileUtils.mv(container_directory, errors_directory)
  rescue => e
    Resque.logger.error "Failsafe error handler: can't move #{container_directory} to #{errors_directory}"
    Resque.logger.error "Error was #{e.class}: #{e.message}"
  else
    Resque.logger.error "Failsafe error handler: moved #{container_directory} to #{errors_directory}"
  end
end

class ProspectiveIngestJob < BaseIngestJob
  @queue = :ftp

  def self.perform(data)
    PackageIngestor.process(data, ProspectiveMetadataChecker)
  rescue => e
    Resque.logger.error "#{self} received #{e.class}: #{e.message}"
  end
end

class DigitoolIngestJob < BaseIngestJob
  @queue = :digitool

  def self.perform(data)
    PackageIngestor.process(data, DigitoolMetadataChecker)
  rescue => e
    Resque.logger.error "#{self} received #{e.class}: #{e.message}"
  end
end

class PackageIngestor

  include WatchConstants

  def self.process(data, updator_class)

    config = if data['config_section']
               Datyl::Config.new(data['config_file'], 'default', data['config_section'])
             else
               Datyl::Config.new(data['config_file'], 'default')
             end

    container_directory = File.join(data['qroot'], PROCESSING_SUBDIRECTORY, data['container'])
    errors_directory    = File.join(data['qroot'], ERRORS_SUBDIRECTORY)
    warnings_directory  = File.join(data['qroot'], WARNINGS_SUBDIRECTORY)
    package_directory   = File.join(container_directory, data['package'])


    completed, started, finished  = false, Time.now, Time.now

    (ENV['http_proxy'], ENV['HTTP_PROXY'] = config.proxy, config.proxy) if config.proxy

    WatchUtils.setup_ingest_database(config)

    package = PackageFactory.new(config, updator_class).new_package(package_directory)

    raise PackageError, "Invalid package in #{package_directory}." unless package && package.valid?

    package.ingest

    completed, finished = true, Time.now

  rescue PackageError => e
    Resque.logger.error e

  ensure  # we're run in a wrapper, which handles non-package errors

    if package
      self.log_summary(package, finished - started)
      package.delete_from_islandora unless completed && package.valid?
      self.disposition(package, container_directory, errors_directory, warnings_directory)
      record_to_database(config.site, package, completed && package.valid?, started, finished)
    end # if package

  end # self.ingest

  def self.log_summary(package, elapsed_time)
    Resque.logger.info  sprintf('%5.2f sec, %5.2f MB  %s::%s (%s) => %s, "%s"',
                          elapsed_time,
                          package.bytes_ingested/1048576.0,
                          package.class,
                          package.name,
                          package.pid || 'no pid',
                          package.collections.empty? ?  'no collections' : 'collection: ' + package.collections.join(', '),
                          package.label)
  rescue => e
    Resque.logger.error "Can't log summary data for package #{package.name}:  #{e.class}: #{e.message}"
  end

  # a package object was handled,  now check where it shoulid go

  def self.disposition(package, container_directory, errors_directory, warnings_directory)

    short_name = WatchUtils.short_package_container_name(container_directory, package)

    if package.errors?
      package.errors.each   { |line| Resque.logger.error line.strip } if package.errors
      Resque.logger.error "Moving from FTP directory #{short_name} to #{errors_directory}"
      FileUtils.mv(container_directory, errors_directory)
    elsif package.warnings?
      package.warnings.each { |line| Resque.logger.warn  line.strip } if package.warnings
      Resque.logger.warn "Moving from FTP directory #{short_name} to #{warnings_directory}"
      FileUtils.mv(container_directory, warnings_directory)
    else
      Resque.logger.info "Deleting successfully ingested package from FTP directory #{short_name}"
      FileUtils.rm_rf container_directory
    end

  rescue => e
    Resque.logger.error "Error during final package disposition for #{short_name}: #{e.class}: #{e.message.strip}"
  end

end
