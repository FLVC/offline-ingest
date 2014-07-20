require 'resque'
require 'offin/config'
require 'offin/packages'
require 'offin/exceptions'
require 'offin/ingest-support'
require 'watch-queue/utils'
require 'mono_logger'

### TODO: Deal with this: infinite loop on the hopefully-rare
### condition where a package both can't be moved out of the
### processing directory (configuration error) and it keeps
### hard-crashing the ingest process (e.g. memory)

class BaseIngestJob

  # @queue = :ingest

  # # class#perform is the one required class method for the resque
  # # system.  data is a hash with config filename, config section, and
  # # various directories:
  # # {container,errors,warnings,package}_directory.

  # def self.perform(data)
  #   PackageIngestor.process(data)
  # rescue => e
  #   Resque.logger.error "Received #{e.class}: #{e.message}"
  # end

  def self.around_perform (data)

    container_directory = data['container_directory']   # in processing_directory
    errors_directory    = data['errors_directory']
    package_directory   = data['package_directory']

    yield

  rescue SystemError => e
    self.failsafe(container_directory, errors_directory)
    Resque.logger.error "System error when processing #{package_directory}, can't continue processing package: #{e.message}"

  rescue => e
    self.failsafe(container_directory, errors_directory)
    Resque.logger.error "Caught unexpected error when processing #{package_directory}: #{e.class} - #{e.message}, backtrace follows:"
    e.backtrace.each { |line| Resque.logger.error line }
    Resque.logger.error "Please correct the error and restart the package.."

  ensure
    self.failsafe(container_directory, errors_directory)
  end

  def self.failsafe(container_directory, errors_directory)
    return unless File.exists? container_directory
    FileUtils.mv container_directory, errors_directory
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
    ProspectivePackageIngestor.process(data, ProspectiveMetadataChecker)
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

  def self.process data, updator_class

    config = Datyl::Config.new(data['config_file'], 'default', data['config_section'])

    package_directory   = data['package_directory']
    container_directory = data['container_directory']
    warnings_directory  = data['warnings_directory']
    errors_directory    = data['errors_directory']

    completed, started, finished  = false, Time.now, Time.now

    (ENV['http_proxy'], ENV['HTTP_PROXY'] = config.proxy, config.proxy) if config.proxy

    WatchUtils.setup_ingest_database(config)

    package = PackageFactory.new(config, updator_class).new_package(package_directory)

    raise PackageError, "Invalid package in #{package_directory}." unless package and package.valid?

    package.ingest

    completed, finished = true, Time.now

  rescue PackageError => e
    Resque.logger.error e

  ensure  # we're run in a wrapper, which handles non-package errors

    if package
      self.log_summary(package, finished - started)
      package.delete_from_islandora unless package.valid?
      self.disposition(package, container_directory, errors_directory, warnings_directory)
      record_to_database(config.site, package, completed && package.valid?, started, finished)
    end # if package

  end # self.ingest

  def self.log_summary  package, elapsed_time
    Resque.logger.info  sprintf('%5.2f sec, %5.2f MB  %s::%s (%s) => %s, "%s"',
                          elapsed_time,
                          package.bytes_ingested/1048576.0,
                          package.class,
                          package.name,
                          package.pid || 'no pid',
                          package.collections.empty? ?  'no collections' : 'collection: ' + package.collections.join(', '),
                          package.label)
  rescue => e
    Resque.logger.error "Can't do summary:  #{e.class}: #{e.message}"
  end

  # a package object was handled,  now check where it shoulid go

  def self.disposition package, container_directory, errors_directory, warnings_directory

    short_name = short_package_container_name(container_directory, package)

    if package.errors?
      package.errors.each   { |line| Resque.logger.error line.strip } if package.errors
      Resque.logger.error "Moving #{short_name} to #{errors_directory}"
      FileUtils.mv container_directory, errors_directory
    elsif package.warnings?
      package.warnings.each { |line| Resque.logger.warn  line.strip } if package.warnings
      Resque.logger.warn "Moving #{short_name} to #{warnings_directory}"
      FileUtils.mv container_directory, warnings_directory
    else
      Resque.logger.info "Deleting successfully ingested package #{short_name}"
      FileUtils.rm_rf container_directory
    end

  rescue => e
    Resque.logger.error "Error in package disposition for package #{short_name}: #{e.class}: #{e.message.strip}"
  end

end
