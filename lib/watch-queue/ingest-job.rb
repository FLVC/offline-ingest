require 'resque'
require 'offin/config'
require 'offin/packages'
require 'offin/ingest-support'
require 'watch-queue/utils'
require 'mono_logger'

class IngestJob

  @queue = :ingest

  # data is a hash with

  def self.perform(data)
    DequeuedPackageIngestor.process(data)
  rescue => e
    Resque.logger.error "Received #{e.class}: #{e.message}"
  end

  def self.before_perform_log_job(data)
    #  Resque.logger.info("WOO HOO!  about to perform #{self} with #{data.class} #{data.inspect}")
  end
end


class DequeuedPackageIngestor

  def self.process data

    config = Datyl::Config.new(data['config_file'], 'default', data['config_section'])

    package_directory   = data['package_directory']
    container_directory = data['container_directory']
    warnings_directory  = data['warnings_directory']
    errors_directory    = data['errors_directory']
    success_directory   = data['success_directory']

    if config.proxy
      ENV['http_proxy'] = config.proxy
      ENV['HTTP_PROXY'] = config.proxy
    end

    setup_ingest_database(config)

    factory = PackageFactory.new(config, ProspectiveMetadataChecker)

    completed, started, finished  = false, Time.now, Time.now
    package = factory.new_package(package_directory)

    raise PackageError, "Invalid package in #{package_directory}." unless package.valid?

    package.ingest
    completed, finished = true, Time.now


    #### TODO: work out disposition of directories on SystemError.....   this might get stuck in infinite loop... what about completed? is it well used?


  rescue PackageError => e
    @package_error = true     # this boolean is mostly to handle the case of errors from package initialization, like a missing manifest, that don't produce a package object at all.
    Resque.logger.error e
  rescue SystemError => e
    Resque.logger.error "System error, can't continue: #{e.message}"
    Resque.logger.error "Sleeping for 30 minutes"
    #### TODO:  sleep 30 * 60
  rescue => e
    Resque.logger.error "Caught unexpected error #{e.class} - #{e.message}"
    Resque.logger.error "Please correct the error and restart."
    # exit 1

  ensure

    if package
      DequeuedPackageIngestor.log_summary(package, finished - started)
      package.delete_from_islandora unless package.valid?
      DequeuedPackageIngestor.disposition(package, container_directory, errors_directory, warnings_directory, success_directory)
      record_to_database(config.site, package, completed && package.valid?, started, finished)
    else
      # TODO: no package!   need to do failsafe?
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


  def self.disposition package, container_directory, errors_directory, warnings_directory, success_directory
    if package.errors?
      package.errors.each   { |line| Resque.logger.error line.strip } if package.errors
      Resque.logger.error "Moving to #{errors_directory}"
      FileUtils.mv container_directory, errors_directory
    elsif package.warnings?
      package.warnings.each { |line| Resque.logger.warn  line.strip } if package.warnings
      Resque.logger.warn "Moving to #{warnings_directory}"
      FileUtils.mv container_directory, warnings_directory
    else
      Resque.logger.info "Moving to #{success_directory}"
      FileUtils.mv container_directory, success_directory
      #     FileUtils.rm_r container_directory
    end
  rescue => e
    Resque.logger.error "Error in package disposition: #{e.class}: #{e.message.strip}"
  end

end
