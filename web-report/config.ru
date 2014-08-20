# -*- mode: ruby; -*-

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '../lib')

CONFIG_FILENAME  = ENV['CONFIG_FILENAME'] || '/usr/local/islandora/offline-ingest/config.yml'   # TODO: have apache set an environment variable for us.

require 'sinatra'
require 'app'
require 'haml'

run Sinatra::Application
