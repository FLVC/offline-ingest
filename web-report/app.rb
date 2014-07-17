require 'rubydora'
require 'offin/db'
require 'offin/config'
require 'offin/utils'
require 'offin/paginator'
require 'offin/csv-provider'


error do
  e = @env['sinatra.error']
  request.body.rewind if request.body.respond_to?('rewind')
  STDERR.puts "#{e.class} #{request.url} - #{e.message}"
  e.backtrace.each { |line| STDERR.puts line }
  [ 500, { 'Content-Type' => 'text/plain' }, "500 Internal Service Error - please contact a systems administrator and have them inspect the apache logs.\n" ]
end

not_found  do
  [ 404, { 'Content-Type' => 'text/plain' },  "404 Not Found - #{request.url} doesn't exist.\n" ]
end

configure do

  $KCODE = 'UTF8'

  set :environment,  :production
  set :logging,      :true        # use CommonLogger for now
  set :raise_errors, false        # Let our app handle the exceptions.
  set :dump_errors,  false        # Don't add backtraces automatically (we'll decide)


  if defined?(PhusionPassenger)
    PhusionPassenger.on_event(:starting_worker_process) do |forked|

      # When we fork a new ruby process under apache, re-connect to
      # the ingest database.  This web service doesn't require a
      # connection to the drupal databases

      if forked
        config = Utils.find_appropriate_admin_config(CONFIG_FILENAME, ENV['SERVER_NAME'])
        DataBase.debug = true
        DataBase.setup(config)
      end
    end
  end
end # of configure

helpers do

  def list_component_links package
    return package.get_components.map { |pid| "<a href=\"http://#{@hostname}/islandora/object/#{pid}\">#{pid}</a>" }
  end

  def list_purl_links package
    return package.get_purls.map { |purl| "<a href=\"#{purl}\">#{purl}</a>" }
  end

  def get_elapsed_time package
    return Utils.pretty_elapsed(package.time_finished.strftime('%s').to_i - package.time_started.strftime('%s').to_i)
  end

  # returns list [  [title, pid],  [title, pid], ... ] sorted on title

  def list_collections config
    used = Utils.available_collection_codes
    collections = Utils.get_collection_names(config)
    main_list = []
    palmm_list = []
    collections.each do |pid, title|
      next unless used[pid]
      title ||= "Title n/a - id #{pid}"
      if pid =~ /^palmm/i
        palmm_list.push [ pid, '(palmm) ' + title ]
      elsif  pid =~ /^#{config.site_namespace}:/
        main_list.push [ pid, title ]
      end
    end
    palmm_list.sort!{ |a,b| a[1].downcase <=> b[1].downcase }
    main_list.sort!{ |a,b| a[1].downcase <=> b[1].downcase }
    return main_list + palmm_list
  end

  def list_collection_links config, package, css = ''
    collection_titles = Utils.get_collection_names(config)
    list = []
    collections = package.get_collections.each do |pid|

      title = collection_titles[pid] ? collection_titles[pid] + " (#{pid})" : pid
      list.push "<a #{css} href=\"http://#{config.site}/islandora/object/#{pid}\">#{title}</a>"
    end
    return list
  end

  def list_datastream_links config, package, css = ''
    links = []
    Utils.get_datastream_names(config.fedora_url, package.islandora_pid).sort { |a, b| a[1] <=> b[1] }.each do |name, label|  # get name,label pairs: e.g. { 'TN' => 'Thumbnail', ... } - sort by label
      links.push "<a #{css} href=\"http://#{config.site}/islandora/object/#{package.islandora_pid}/datastream/#{name}/view\">#{label}</a>"
    end
    return links
  end

  def check_if_present config, package
    return Utils.ping_islandora_for_object(config.site, package.islandora_pid)
  end

  #  this is WAAAY too slow to use; rethink using an RI query

  def get_on_site_map config, packages
    pids = packages.map { |p| p.islandora_pid }
    return Utils.ping_islandora_for_objects(config.site, pids)
  end

end # of helpers



before do
  # By convention, we are running the web service as
  # 'admin.school.digital.flvc.org' where 'school.digital.flvc.org' is
  # the drupal server.  So we delete the leading 'admin.' to find
  # the appropriate server configuration (for db connection info).

  @hostname = ENV['SERVER_NAME'].sub(/^admin\./, '')
  @config   = Utils.find_appropriate_admin_config(CONFIG_FILENAME, ENV['SERVER_NAME'])

  halt 500, "Don't know how to configure for server '#{ENV['SERVER_NAME']}', using '#{CONFIG_FILENAME}'"  unless @config

  @site   = DataBase::IslandoraSite.first(:hostname => @hostname)

end # of before

# Intro page; not anything there now, let's just go to packages

get '/' do
  redirect '/packages'
  # haml :index # for later when we have more microservices....
end

get '/packages/' do
  redirect '/packages'
end

get '/packages' do
  @collections = list_collections(@config)
  @paginator   = PackageListPaginator.new(@site, params)
  @packages    = @paginator.packages

  # @islandora_presence = get_on_site_map(@config, @packages)  # too slow - rethink this
  haml :packages
end

get '/packages/:id' do
  @paginator    = PackagePaginator.new(@site, params)
  @package      = DataBase::IslandoraPackage.first(:id => params[:id], :islandora_site => @site)

  # TODO: move some of the following into PackagePaginator (used to just use a package object)

  @package_collections = list_collection_links(@config, @package)
  @elapsed             = get_elapsed_time(@package)
  @components          = list_component_links(@package)
  @purls               = list_purl_links(@package)
  @datastreams         = list_datastream_links(@config, @package)
  @on_islandora        = check_if_present(@config, @package)   # one of :present, :missing, :error, :forbidden

  haml :package
end

get '/status' do
  [ 200, { 'Content-Type'  => 'application/xml' }, "<status/>\n" ]
end

get '/csv' do
  csv_provider = CsvProvider.new(@site, params)
  content_type 'text/csv'
  attachment('packages.csv')
  csv_provider
end
