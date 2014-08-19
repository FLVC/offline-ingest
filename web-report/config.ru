# -*- mode: ruby; -*-

$LOAD_PATH.unshift '/usr/local/islandora/offline-ingest/lib/'         # TODO: use relative path.

CONFIG_FILENAME  = '/usr/local/islandora/offline-ingest/config.yml'   # TODO: have apache set an environment variable for us.

require 'sinatra'
require 'app'
require 'haml'

run Sinatra::Application
