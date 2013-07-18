# TODO: rename as script-support


require 'socket'
require 'optparse'
require 'offin/utils'
require 'offin/exceptions'
require 'offin/config'
require 'offin/packages'
Utils.silence_warnings { require 'offin/db' }   # csv constant redefinition deep in datamapper

def get_config_filename

  return case Socket.gethostname
  when /alpo/i;                                 "/home/fischer/WorkProjects/offline-ingest/config.yml"
  when /romeo-foxtrot|flvc-rfischer.local/i;    "/Users/fischer/WorkProjects/offline-ingest/config.yml"
  when /islandora[dtp]/i;                       "/usr/local/islandora/offline-ingest/config.yml"
  else
    STDERR.puts "#{$0} Doesn't know how to configure for this environment (#{Socket.gethostname.downcase}), quitting."
    exit -1
  end
end

# This is very config-specific and may need to be changed: given the
# hostname, return the section name of the config.yml file that deals
# with that host.

def appropriate_server
  case Socket.gethostname
  when /alpo/i;                            "alpo"
  when /islandorad/i;                      "islandora7d"
  when /islandorat/i;                      "fsu7t"
  when /islandorap/i;                      "fsu7prod"
  else
    STDERR.puts "#{$0} Doesn't know how to configure for this environment (#{Socket.gethostname.downcase}), quitting."
    exit -1
  end
end


def record_to_database site, package, status, start_time, finish_time

  site = DataBase::IslandoraSite.first_or_create(:hostname => site)
  rec  = DataBase::IslandoraPackage.new(:title          => package.label,
                                        :package_name   => package.name,
                                        :islandora_pid  => package.pid,
                                        :time_started   => start_time.to_i,
                                        :time_finished  => finish_time.to_i,
                                        :bytes_ingested => package.bytes_ingested,
                                        :digitool_id    => package.digitool_id,
                                        :success        => status,
                                        :content_model  => package.content_model,
                                        :islandora_site => site)

  rec.add_warnings     package.warnings
  rec.add_errors       package.errors
  rec.add_purls        package.purls
  rec.add_collections  package.collections
  rec.add_components   package.component_objects

  if not rec.save
    STDERR.puts rec.errors.inspect
    STDERR.puts "Unable to save to database:", rec.errors.map { |err| err.to_s }
    exit 1
  end
end

def setup_database config
  DataBase.setup(config) unless config.test_mode
rescue => e
  STDERR.puts e
  exit 1
end


def package_usage

  return <<-EOF.gsub(/^    /, '')
    Usage:  package [ --test-mode | --server ID ]  package-directories*
    the options can be abbreviated as -t and -s, respectively.
    EOF
end

def get_config_server_sections
  return  Datyl::Config.new(get_config_filename, 'default').all_sections - [ 'default' ]
rescue
  return []
end


def get_config *sections
  return  Datyl::Config.new(get_config_filename, 'default', *sections)
end


Struct.new('Options', :server_id, :test_mode)

def parse_command_line args
  command_options = Struct::Options.new(nil, nil)
  server_sections = get_config_server_sections


  opts   = OptionParser.new do |opt|
    opt.banner = package_usage
    opt.on("--server ID",   String,  "ingest to server id #{server_sections.join(', ')}.")  { |sid| command_options.server_id = sid }
    opt.on("--test-mode",  "run basic checks on package without ingesting") { command_options.test_mode = true }
  end

  opts.parse!(args)

  if not command_options.test_mode
    raise SystemError, "No server specified." if command_options.server_id.nil?
    raise SystemError, "Bad server ID: use one of #{server_sections.join(', ')}" unless server_sections.include? command_options.server_id
  end

  raise SystemError, "No packages specified." if args.empty?


  if command_options.test_mode
    config = Datyl::Config.new(get_config_filename, "default")
    config[:test_mode] = true
  else
    config = Datyl::Config.new(get_config_filename, "default", command_options.server_id)
  end

rescue => e
  STDERR.puts e, opts
  exit -1
else
  return config
end
