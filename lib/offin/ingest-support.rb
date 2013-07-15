require 'socket'
require 'optparse'


# call this first

def setup_environment

  case Socket.gethostname

  when /alpo/i

    $LOAD_PATH.unshift "/home/fischer/WorkProjects/offline-ingest/lib/"
    config_filename  = "/home/fischer/WorkProjects/offline-ingest/config.yml"

  when /romeo-foxtrot/i, /flvc-rfischer.local/i

    $LOAD_PATH.unshift "/Users/fischer/WorkProjects/offline-ingest/lib/"
    config_filename  = "/Users/fischer/WorkProjects/offline-ingest/config.yml"

  when /islandorad/i, /islandorat/i, /islandorap/i

    $LOAD_PATH.unshift "/usr/local/islandora/offline-ingest/lib/"
    config_filename  = "/usr/local/islandora/offline-ingest/config.yml"

    # TODO: move following to config

    # ENV['HTTP_PROXY'] = 'http://localhost:3128/'  # libxml picks this up, but it's very picky about syntax!
    # ENV['http_proxy'] = 'http://localhost:3128/'

  else
    STDERR.puts "#{$0} Doesn't know how to configure for this environment (#{Socket.gethostname.downcase}), quitting."
    exit -1
  end

  Utils.silence_warnings do   # csv constant redefinition deep in datamapper
    require 'offin/db'
  end

  return config_filename
end


def record_to_database site, package, status, start_time, finish_time

  site = DataBase::IslandoraSite.first_or_create(:hostname => site)
  rec  = DataBase::IslandoraPackage.new(:title          => package.label,
                                        :package_name   => package.name,
                                        :islandora_pid  => package.pid,
                                        :time_started   => start_time.to_i,
                                        :time_finished  => finish_time.to_i,
                                        :bytes_ingested => package.bytes_ingested,
                                        :iid            => package.iid,
                                        :success        => status,
                                        :content_model  => package.content_model,
                                        :islandora_site => site)

  rec.add_warnings     package.warnings
  rec.add_errors       package.errors
  rec.add_purls        package.purls
  rec.add_collections  package.collections
  rec.add_components   package.component_objects

  if not rec.save
    STDERR.puts "Unable to save to database:", rec.errors.map { |err| err.to_s }
    exit 1
  end

end





Struct.new('Options', :server_code, :test_mode)

def parse_command config, args
  command_options = Struct::Options.new(nil, nil)

  # check config file for all sections that have sitename


  opts   = OptionParser.new do |opt|
    opt.banner = "Usage: package [ --test-mode ] --server-flag package-directory package-directories\n" + "where --flag is one of:\n"
    opt.on("--fsu7t",      "use fsu7t.flca.edu for ingest")                 { command_options.server_code = 'fsu7t' }
    opt.on("--fsu-prod",   "use fsu-prod.flca.edu for ingest")              { command_options.server_code = 'fsu-prod' }
    opt.on("--d7",         "use islandora7d.fcla.edu for ingest")           { command_options.server_code = 'i7d' }
    opt.on("--alpo",       "use alasnorid.alpo.fcla.edu for ingest")        { command_options.server_code = 'alpo' }
    opt.on("--test-mode",  "run basic checks on package without ingesting") { command_options.test_mode = true }
  end
  opts.parse!(args)
  raise "No server specified."        if command_options.server_code.nil? and not command_options.test_mode
  raise "No packages specified."      if args.empty?
rescue => e
  STDERR.puts e, opts
  exit -1
else
  return command_options
end
