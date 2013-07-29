
require 'offin/db'
require 'offin/config'


configure do
  $KCODE = 'UTF8'
  set :logging,      :true        # temporarily true - set to false and start it up explicitly later with our own logging
  set :environment,  :production  # Get some exceptional defaults.
  set :raise_errors, false        # Let our app handle the exceptions.
  set :dump_errors,  false        # Don't add backtraces automatically (we'll decide)
  set :haml, :format => :html5, :escape_html => true
  # use Rack::CommonLogger

  @config   = Datyl::Config.new('/usr/local/islandora/offline-ingest/config.yml', 'default', 'islandora7d')

  DataBase.debug = true
  DataBase.setup(@config)


  @hostname = 'islandora7d.fcla.edu'
  @site = DataBase::IslandoraSite.first(:hostname => @hostname)

end


# Intro page

get '/' do
  haml :index
end


get '/packages' do
  @packages = DataBase::IslandoraPackage.all(:order => [ :time_started.desc ])
# @packages = DataBase::IslandoraPackage.all(:order => [ :time_started.desc ], :islandora_site => @hostname)
  haml :packages
end


get '/status' do
  [ 200, {'Content-Type'  => 'application/xml'}, "<status/>\n" ]
end
