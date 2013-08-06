require 'rubydora'
require 'offin/db'
require 'offin/config'
require 'offin/utils'

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


  case ENV['SERVER_NAME']
  when "islandora-admin7d.fcla.edu"
    DataBase.debug = true
    DataBase.setup(Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default', 'islandora7d'))
  else
    # how do we error out sensibly here?
  end
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

  def list_collection_links config, package
    return package.islandora_collection_links(Utils.get_collection_names(config))
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
end

before do
  case ENV['SERVER_NAME']
  when 'islandora-admin7d.fcla.edu'
    @hostname = 'islandora7d.fcla.edu'
    @site = DataBase::IslandoraSite.first(:hostname => @hostname)
    @config = Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default', 'islandora7d')
  else
    halt 500, "Don't know how to configure for server #{ENV['SERVER_NAME']}"
  end
end

# Intro page

get '/' do
  haml :index
end

get '/packages' do
  redirect '/packages/'
end

# get '/:partition/data/' do |partition|
#   silo = get_silo(partition)

#   page   = params[:page].nil?   ?   1   : safe_number(params[:page])
#   search = (params[:search].nil? or params[:search] == '') ?  nil : params[:search]

#   count      = silo.package_count(search)
#   packages   = silo.package_names_by_page(page, PACKAGES_PER_PAGE, search)
#   packages_1 = packages[0 .. (PACKAGES_PER_PAGE + 1)/2 - 1]
#   packages_2 = packages[(PACKAGES_PER_PAGE + 1)/2 .. packages.count - 1]

#   if (packages_2.nil? or packages_2.empty?) and (packages_1.nil? or packages_1.empty?)
#     erb_page = :'packages-none-up'
#   elsif (packages_2.nil? or packages_2.empty?)
#     erb_page = :'packages-one-up'
#   else
#     erb_page = :'packages-two-up'
#   end

#   erb erb_page, :locals => {
#     :packages_1      => packages_1,
#     :packages_2      => packages_2,
#     :hostname        => hostname,
#     :silo            => silo,
#     :package_count   => count,
#     :page            => page,
#     :search          => search,
#     :number_of_pages => count/PACKAGES_PER_PAGE + (count % PACKAGES_PER_PAGE == 0 ? 0 : 1),
#     :revision        => REVISION }
# end

get '/packages/' do
  @packages = DataBase::IslandoraPackage.all(:order => [ :time_started.desc ], :islandora_site => @site)
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
