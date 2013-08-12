require 'rubydora'
require 'offin/db'
require 'offin/config'
require 'offin/utils'
require 'offin/paginator'



configure do
  $KCODE = 'UTF8'
  set :logging,      :true        # temporarily true - set to false and start it up explicitly later with our own logging

  set :environment,  :production  # Get some exceptional defaults.
  # set :raise_errors, false        # Let our app handle the exceptions.
  # set :dump_errors,  false        # Don't add backtraces automatically (we'll decide)

  set :raise_errors, true
  set :dump_errors,  true
  set :haml, :format => :html5, :escape_html => false
  # use Rack::CommonLogger


  # renaming in progress: TODO: clean this up

  section_name = case ENV['SERVER_NAME']
                 when "islandora-admin7d.fcla.edu"; 'islandora7d'
                 when "admin.islandora7d.fcla.edu"; 'islandora7d'
                 when "fsu-admin.sacred.net";       'fsu7prod'
                 when "admin.fsu.digital.flvc.org"; 'fsu7prod'
                 when "admin.fsu7t.fcla.edu";       'fsu7t'
                 else ;                             ENV['SERVER_NAME']  # to get it into error message, at lease
                 end

  DataBase.debug = true
  DataBase.setup(Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default',  section_name))
end


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

  def list_collection_links config, package, css = ''
    collection_titles = Utils.get_collection_names(config)
    list = []
    collections = package.get_collections.each do |pid|
      title = collection_titles[pid] ? collection_titles[pid] + " (#{pid})" : pid
      list.push "<a href=\"http://#{config.site}/islandora/object/#{pid}\">#{title}</a>"
    end
    return list
  end

  def list_datastream_links config, package
    links = []
    Utils.get_datastream_names(config.url, package.islandora_pid).sort { |a, b| a[1] <=> b[1] }.each do |name, label|  # get name,label pairs: e.g. { 'TN' => 'Thumbnail', ... } - sort by label
      links.push "<a href=\"http://#{config.site}/islandora/object/#{package.islandora_pid}/datastream/#{name}/view\">#{label}</a>"
    end
    return links
  end

  def check_if_present config, package
    return Utils.ping_islandora_for_object(config.site, package.islandora_pid)
  end

  def get_on_site_map config, packages
    pids = packages.map { |p| p.islandora_pid }
    return Utils.ping_islandora_for_objects(config.site, pids)
  end

end

before do

  # renaming in progress: TODO: clean this up

  case ENV['SERVER_NAME']

  when "islandora-admin7d.fcla.edu"
    @hostname = 'islandora7d.fcla.edu'
    @config = Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default', 'islandora7d')
  when "admin.islandora7d.fcla.edu"
    @hostname = 'islandora7d.fcla.edu'
    @config = Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default', 'islandora7d')

  when "fsu-admin.sacred.net"
    @hostname = 'fsu.digital.flvc.org'
    @config = Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default', 'fsu7prod')
  when "admin.fsu.digital.flvc.org"
    @hostname = 'fsu.digital.flvc.org'
    @config = Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default', 'fsu7prod')

  when "admin.fsu7t.fcla.edu"
    @hostname = 'fsu7t.fcla.edu'
    @config = Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default', 'fsu7t')

  else
    halt 500, "Don't know how to configure for server #{ENV['SERVER_NAME']}"
  end

  @site = DataBase::IslandoraSite.first(:hostname => @hostname)
end

# Intro page

get '/' do
  redirect '/packages'
  # haml :index  -- for later when we have more microservices....
end

get '/packages/' do
  redirect '/packages'
end

get '/packages' do
  @paginator          = PackageListPaginator.new(@site, params)
  @packages           = @paginator.packages
  # @islandora_presence = get_on_site_map(@config, @packages)  -- this is WAAAY to slow to use;
  haml :packages
end

get '/packages/:id' do
  @package      = DataBase::IslandoraPackage.first(:id => params[:id], :islandora_site => @site)
  @collections  = list_collection_links(@config, @package)
  @elapsed      = get_elapsed_time(@package)
  @components   = list_component_links(@package)
  @purls        = list_purl_links(@package)
  @datastreams  = list_datastream_links(@config, @package)
  @on_islandora = check_if_present(@config, @package)   # one of :present, :missing, :error
  haml :package
end

get '/status' do
  [ 200, {'Content-Type'  => 'application/xml'}, "<status/>\n" ]
end
