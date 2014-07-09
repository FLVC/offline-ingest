# -*- mode: ruby -*-

NUMBER_OF_INGESTORS = 2
GID  = 'ftpil'
UID  = 'ftpil'

#  ftp-handler is very simple, it moves completed ftp uploads to a special
#  directory and adds a descriptor for them to the "ingest" queue.

# The associated ftp directories should be writable and readable by
#  UID and GID, see instructions.

God.watch do |w|
  w.uid      = UID
  w.gid      = GID
  w.name     = "ftp-handler"
  w.log_cmd  = "/bin/logger -t '#{w.name}'"
  w.start    = "/usr/local/islandora/offline-ingest/tools/ftp-handler"
  w.keepalive
end

# ingest-handler pulls off package descriptors from the "ingest" queue and
# attempts to process those packages.

NUMBER_OF_INGESTORS.times do |num|
  God.watch do |w|
    w.uid      = UID
    w.gid      = GID
    w.name     = "ingest-handler-#{num}"
    w.group    = "ingest-handlers"
    w.log_cmd  = "/bin/logger -t '#{w.name}'"
    w.start    = "/usr/local/islandora/offline-ingest/tools/ingest-handler"
    w.keepalive
  end
end