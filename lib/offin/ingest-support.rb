# TODO: rename as script-support

require 'fileutils'
require 'socket'
require 'optparse'
require 'offin/utils'
require 'offin/exceptions'
require 'offin/config'
require 'offin/packages'
Utils.silence_warnings { require 'offin/db' }   # csv constant redefinition deep in datamapper


# This file has a loose collection of functions used by the .../tools/package
# script, and perhaps other similar tools.


def get_config_filename
  return case Socket.gethostname
         when /islandora[dtp]\.fcla\.edu|tlhlxftp\d+-.*\.flvc\.org/i; "/usr/local/islandora/offline-ingest/config.yml"
         when /fischer.flvc.org/i;                                    "/Users/fischer/WorkProjects/offline-ingest/config.yml"
         else
           STDERR.puts "#{$0} Doesn't know how to configure the environment for host (#{Socket.gethostname.downcase}), quitting."
           exit -1
         end
end


def record_to_database site, package, status, start_time, finish_time

  site = DataBase::IslandoraSite.first_or_create(:hostname => site)

  # in case of errors, some of the following may be nil

  rec  = DataBase::IslandoraPackage.new(:title          => (package.label || '')[0, 255],
                                        :package_name   => package.name,
                                        :islandora_pid  => package.pid,
                                        :time_started   => start_time,
                                        :time_finished  => finish_time,
                                        :bytes_ingested => package.bytes_ingested,
                                        :digitool_id    => (package.digitool_id ? package.digitool_id.to_i : nil),
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

def setup_ingest_database config
  return if config.test_mode
  DataBase.setup(config)
rescue => e
  STDERR.puts e
  exit 1
end


def package_ingest_usage

  return <<-EOF.gsub(/^    /, '')
    Usage:  package [ --test-mode | --server ID ] [ --dump-directory dir ] [ --digitool ] package-directories*
    EOF
end

def get_config_server_sections
  return  Datyl::Config.new(get_config_filename, 'default').all_sections - [ 'default', 'zip_serialize_pdfs' ]
rescue
  return []
end


def get_config *sections
  return  Datyl::Config.new(get_config_filename, 'default', *sections)
end


Struct.new('Options', :server_id, :test_mode, :dump_directory, :digitool_rules)

def package_ingest_parse_command_line args
  command_options = Struct::Options.new(nil, false, nil, true)
  server_sections = get_config_server_sections
  command_options.digitool_rules = false

  opts   = OptionParser.new do |opt|
    opt.banner = package_ingest_usage
    opt.on("--server ID", String,          "ingest to server ID - one of: [ #{server_sections.join(', ')} ]")  { |sid| command_options.server_id = sid }
    opt.on("--test-mode",                  "run basic checks on package without ingesting")                    { command_options.test_mode = true }
    opt.on("--dump-directory DIR", String, "optionally, move failed packages to directory DIR")                { |dir| command_options.dump_directory = dir.sub(/\/+$/, '') }
    opt.on("--digitool",                   "apply DigiTool processing rules to package")                       { command_options.digitool_rules = true }
  end

  opts.parse!(args)

  if not command_options.test_mode
    raise SystemError, "No server specified." if command_options.server_id.nil?
    raise SystemError, "Bad server ID: use one of #{server_sections.join(', ')}" unless server_sections.include? command_options.server_id
  end

  case
  when (command_options.test_mode and command_options.server_id)
    config = Datyl::Config.new(get_config_filename, "default", command_options.server_id)
    config[:test_mode] = true
  when (command_options.test_mode)
    config = Datyl::Config.new(get_config_filename, "default")
    config[:test_mode] = true
  else
    config = Datyl::Config.new(get_config_filename, "default", command_options.server_id)
  end

  if command_options.dump_directory
    case
    when File.exists?(command_options.dump_directory)
      raise SystemError, "The directory #{command_options.dump_directory} isn't really a directory" unless File.directory? command_options.dump_directory
      raise SystemError, "Can't write to directory #{command_options.dump_directory}"  unless File.writable? command_options.dump_directory
    else
      FileUtils.mkdir_p command_options.dump_directory
    end
    config[:dump_directory] = command_options.dump_directory
  end

  config[:digitool_rules] = command_options.digitool_rules
  raise SystemError, "No packages specified." if args.empty?

rescue => e
  STDERR.puts e, opts
  exit -1
else
  return config
end


def get_move_target source, dest
  short_name = source.sub(/\/+/, '').sub(/^.*\//, '')
  i = 0
  target = File.join(dest, short_name)
  while File.exists? target
    target = File.join(dest, short_name) + " (#{i += 1})"
  end
  return target
end


def move_to_dump_directory_maybe package_directory, destination
  return unless destination
  FileUtils.mv package_directory, get_move_target(package_directory, destination)
rescue => e
  raise SystemError, "Could not move package #{package_directory} to #{destination}: #{e.class} #{e.message}"
end
