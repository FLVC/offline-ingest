# -*- mode: ruby; -*-

$LOAD_PATH.unshift "/usr/local/islandora/offline-ingest/lib/"

require 'sinatra'
require 'app'
require 'haml'

run Sinatra::Application
