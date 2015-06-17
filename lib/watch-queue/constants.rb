module WatchConstants

  FTP_DIRECTORY_DELAY        = 10 * 60  # directory has to be unchanged these many seconds
  FILESYSTEM_DIRECTORY_DELAY = 10       # before we'll process packages we discover in them

  ERRORS_SUBDIRECTORY      = 'errors'
  INCOMING_SUBDIRECTORY    = 'incoming'
  PROCESSING_SUBDIRECTORY  = 'processing'
  WARNINGS_SUBDIRECTORY    = 'warnings'

  DIRECTORY_SCAN_PAUSE  = 5      # pause between checks of directories for new packages (but see delay above) - we also check the config file for updates
  SYSTEM_ERROR_SLEEP    = 120    # sleep two minutes if we think it's an errant error.
  UNHANDLED_ERROR_SLEEP = 600    # five minutes
  WORKER_SLEEP          = 5      # try to increase to 10 seconds - does 'god terminate' still work reliably?

end
