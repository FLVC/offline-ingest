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


  case ENV['SERVER_NAME']
  when "islandora-admin7d.fcla.edu"
    DataBase.debug = true
    DataBase.setup(Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default', 'islandora7d'))
  when "fsu-admin.sacred.net"
    DataBase.debug = true
    DataBase.setup(Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default', 'fsu7prod'))
  else
    # how do we error out sensibly here?
  end
end


helpers do
  PACKAGES_PER_PAGE = 15

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

  # At most one of before_id, after_id should be set.

  def get_page_worth_of_packages before_id, after_id

    if before_id
      pid = before_id.to_i
      ids = repository(:default).adapter.select("SELECT \"id\" FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]} AND \"id\" > #{pid} ORDER BY \"id\" ASC LIMIT #{PACKAGES_PER_PAGE}")
    elsif after_id
      pid = after_id.to_i
      ids = repository(:default).adapter.select("SELECT \"id\" FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]} AND \"id\" < #{pid} ORDER BY \"id\" DESC LIMIT #{PACKAGES_PER_PAGE}")
    else
      ids = repository(:default).adapter.select("SELECT \"id\" FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]} ORDER BY \"id\" DESC LIMIT #{PACKAGES_PER_PAGE}")
    end

    return [] if ids.empty?
    return DataBase::IslandoraPackage.all(:order => [ :id.desc ], :id => ids)
  end


  def get_page_limits packages
    return nil, nil if packages.empty?
    min = repository(:default).adapter.select("SELECT min(\"id\") FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]}")
    max = repository(:default).adapter.select("SELECT max(\"id\") FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]}")

    return [] if min.empty?
    min, max = min[0], max[0]

    before = packages.first[:id]
    after  = packages.last[:id]

    after = packages.last[:id] > min ? packages.last[:id] : nil
    before = packages.first[:id] < max ? packages.first[:id] : nil

    return before, after
  end
end

before do
  case ENV['SERVER_NAME']
  when 'islandora-admin7d.fcla.edu'
    @hostname = 'islandora7d.fcla.edu'
    @site = DataBase::IslandoraSite.first(:hostname => @hostname)
    @config = Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default', 'islandora7d')
  when "fsu-admin.sacred.net"
    @hostname = 'fsu.digital.flvc.org'
    @site = DataBase::IslandoraSite.first(:hostname => @hostname)
    @config = Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default', 'fsu7prod')
  else
    halt 500, "Don't know how to configure for server #{ENV['SERVER_NAME']}"
  end
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
  @paginator = PackagePaginator.new(@site, params[:before], params[:after])
  @packages  = @paginator.packages
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
