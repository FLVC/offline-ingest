require 'rubydora'
require 'offin/db'
require 'offin/config'
require 'offin/utils'
require 'offin/paginator'
require 'offin/csv-provider'
require 'offin/drupal-database'

error do
  e = @env['sinatra.error']
  request.body.rewind if request.body.respond_to?('rewind')
  STDERR.puts "#{e.class} #{request.url} - #{e.message}"
  e.backtrace.each { |line| STDERR.puts line }
  [ 500, { 'Content-Type' => 'text/plain' }, "Internal Service Error. Please contact the systems administrator.\n" ]
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

  # set :dump_errors,  true
  # set :haml, :format => :html5, :escape_html => false
  # use Rack::CommonLogger

  section_name = case ENV['SERVER_NAME']
                 when "admin.fsu.digital.flvc.org"; 'fsu7prod'
                 when "admin.fsu7t.fcla.edu";       'fsu7t'
                 when "admin.islandora7d.fcla.edu"; 'islandora7d'
                 else ;                              ENV['SERVER_NAME']  # at least we'll get an error message with some data...
                 end


  if defined?(PhusionPassenger)
    PhusionPassenger.on_event(:starting_worker_process) do |forked|
       if forked
         config = Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default',  section_name)
         DataBase.debug = true
         DataBase.setup(config)
         STDERR.puts "DB Setup Complete"

         # Don't need this quite yet:
         # DrupalDataBase.debug = true
         # DrupalDataBase.setup(Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default',  section_name))
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

  case ENV['SERVER_NAME']

  when "admin.islandora7d.fcla.edu"
    @hostname = 'islandora7d.fcla.edu'
    @config = Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default', 'islandora7d')

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

end # of before

# Intro page; not anything there now, let's just go to packages

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
  # @islandora_presence = get_on_site_map(@config, @packages)
  haml :packages
end

get '/packages/:id' do
  @paginator    = PackagePaginator.new(@site, params)
  @package      = DataBase::IslandoraPackage.first(:id => params[:id], :islandora_site => @site)

  # TODO: move some of the following into PackagePaginator (used to just use a package object)

  @collections  = list_collection_links(@config, @package)
  @elapsed      = get_elapsed_time(@package)
  @components   = list_component_links(@package)
  @purls        = list_purl_links(@package)
  @datastreams  = list_datastream_links(@config, @package)
  @on_islandora = check_if_present(@config, @package)   # one of :present, :missing, :error

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
