# -*- mode: ruby; -*-

$LOAD_PATH.unshift '/usr/local/islandora/offline-ingest/lib/'

CONFIG_FILENAME  = '/usr/local/islandora/offline-ingest/config.yml'

HOST_MAPPING = {
    'admin.famu.digital.flvc.org' => [ 'famu.digital.flvc.org', 'famu7prod' ],
    'admin.fau.digital.flvc.org'  => [ 'fau.digital.flvc.org',  'fau7prod' ],
    'admin.fau7t.fcla.edu'        => [ 'fau7t.fcla.edu',        'fau7t' ],
    'admin.fgcu.digital.flvc.org' => [ 'fgcu.digital.flvc.org', 'fgcu7prod' ],
    'admin.fsu.digital.flvc.org'  => [ 'fsu.digital.flvc.org',  'fsu7prod' ],
    'admin.fsu7t.fcla.edu'        => [ 'fsu7t.fcla.edu',        'fsu7t' ],
    'admin.islandora7d.fcla.edu'  => [ 'islandora7d.fcla.edu',  'islandora7d' ],
    'admin.ucf.digital.flvc.org'  => [ 'ucf.digital.flvc.org',  'ucf7prod' ],
  }

require 'sinatra'
require 'app'
require 'haml'

run Sinatra::Application
