
require 'offin/db'
require 'offin/config'


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
    # how do we error out here?
  end
end


before do
  case ENV['SERVER_NAME']
  when 'islandora-admin7d.fcla.edu'
    @hostname = 'islandora7d.fcla.edu'
    @site = DataBase::IslandoraSite.first(:hostname => @hostname)
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

get '/packages/' do
  @packages = DataBase::IslandoraPackage.all(:order => [ :time_started.desc ], :islandora_site => @site)
  haml :packages
end


get '/status' do
  [ 200, {'Content-Type'  => 'application/xml'}, "<status/>\n" ]
end
