# -*- mode: ruby; -*-

$LOAD_PATH.unshift '/usr/local/islandora/offline-ingest/lib/'

CONFIG_FILENAME  = '/usr/local/islandora/offline-ingest/config.yml'

# TODO: we don't need to maintain this mapping if we're smarter about
# examining the config.yaml file, and assuming 'admin.' tacked on the
# front.

HOST_MAPPING = {

  # development sites:

  'admin.islandora7d.fcla.edu'  => [ 'islandora7d.fcla.edu', 'islandora7d' ],

  # test sites:

  'admin.islandora-test.digital.flvc.org'  => [ 'islandora-test.digital.flvc.org', 'islandora-test' ],
  'admin.fsu-test.digital.flvc.org'        => [ 'fsu-test.digital.flvc.org',       'fsu-test' ],
  'admin.fau-test.digital.flvc.org'        => [ 'fau-test.digital.flvc.org',       'fau-test' ],
  'admin.ucf-test.digital.flvc.org'        => [ 'ucf-test.digital.flvc.org',       'ucf-test' ],
  'admin.gcsc-test.digital.flvc.org'       => [ 'gcsc-test.digital.flvc.org',      'gcsc-test' ],

  # production sites:

  'admin.famu.digital.flvc.org'  => [ 'famu.digital.flvc.org',  'famu7prod'  ],
  'admin.fau.digital.flvc.org'   => [ 'fau.digital.flvc.org',   'fau7prod'   ],
  'admin.fgcu.digital.flvc.org'  => [ 'fgcu.digital.flvc.org',  'fgcu7prod'  ],
  'admin.fsu.digital.flvc.org'   => [ 'fsu.digital.flvc.org',   'fsu7prod'   ],
  'admin.gcsc.digital.flvc.org'  => [ 'gcsc.digital.flvc.org',  'gcsc7prod'  ],
  'admin.nwfsc.digital.flvc.org' => [ 'nwfsc.digital.flvc.org', 'nwfsc7prod' ],
  'admin.palmm.digital.flvc.org' => [ 'palmm.digital.flvc.org', 'palmm7prod' ],
  'admin.ucf.digital.flvc.org'   => [ 'ucf.digital.flvc.org',   'ucf7prod'   ],
}

require 'sinatra'
require 'app'
require 'haml'

run Sinatra::Application
