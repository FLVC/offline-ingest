require 'resque'
require 'offin/config'
require 'offin/packages'
require 'offin/ingest-support'

class IngestJob
  @queue = :ingest

  # data is a hash

  def self.perform(data)
    do_ingest(data)
  rescue  Resque::TermException   # TODO: attempt to cleanup here.
    STDERR.puts "Received TERM"
    exit 0
  rescue => e
    STDERR.puts "Received #{e.class}: #{e.message}", e.backtrace
  end

end



def do_ingest data

  config = Datyl::Config.new(data['config_file'], 'default', data['config_section'])

  package_directory   = data['package_directory']
  container_directory = data['container_directory']
  warnings_directory  = data['warnings_directory']
  errors_directory    = data['errors_directory']

  if config.proxy
    ENV['http_proxy'] = config.proxy
    ENV['HTTP_PROXY'] = config.proxy
  end

  setup_ingest_database(config)  # not clear if we should redo this each time...

  if config.digitool_rules
    factory = PackageFactory.new(config, DigitoolMetadataChecker)
  else
    factory = PackageFactory.new(config, ProspectiveMetadataChecker)
  end

  completed, started, finished  = false, Time.now, Time.now

  package = factory.new_package(package_directory)

  raise PackageError, "Invalid package in #{package_directory}." unless package.valid?

  package.ingest

  completed, finished = true, Time.now

rescue PackageError => e
  @package_error = true     # this boolean is mostly to handle the case of errors from package initialization, like a missing manifest, that don't produce a package object at all.
  STDERR.puts e

rescue SystemError => e
  STDERR.puts "Gracefully exiting from fatal system error: #{e.message}"
  exit 1

rescue => e
  STDERR.puts "Caught completely unexpected error #{e.class} - #{e.message}", e.backtrace, '', "Please correct the error and retry."
  exit 1

ensure

  if package
    STDERR.puts sprintf('%5.2f sec, %5.2f MB  %s::%s (%s) => %s, "%s"',
                        finished - started,
                        package.bytes_ingested/1048576.0,
                        package.class,
                        package.name,
                        package.pid || 'no pid',
                        package.collections.empty? ?  'no collections' : 'collection: ' + package.collections.join(', '),
                        package.label)

    package.delete_from_islandora if not package.valid?

    record_to_database(config.site, package, completed && package.valid?, started, finished)

    if package.errors
      FileUtils.mv container_directory, error_directory
    elsif package.warnings
      FileUtils.mv container_directory, warning_directory
    else
      FileUtils.rm_r container_directory
    end

  end
end
