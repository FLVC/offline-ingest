# -*- mode: ruby -*-

NUMBER_OF_INGESTORS = 3

God.terminate_timeout = 20


#  ftp-worker is very simple, it moves completed ftp uploads to a special
#  directory and adds them to the "ingest" queue.

God.watch do |w|
  w.name     = "ftp-worker"
  w.start    = "/Users/fischer/WorkProjects/offline-ingest/tools/ftp-worker"
  w.keepalive
end

# resque-ingest-worker pulls off packages from the "ingest" queue and
# attempts to process them.


NUMBER_OF_INGESTORS.times do |num|

  God.watch do |w|
    w.name     = "ingest-worker-#{num}"
    w.group    = "ingest-workers"
    w.env      = { "QUEUE" => "ingest" }
    w.start    = "/Users/fischer/WorkProjects/offline-ingest/tools/resque-ingest-worker"
    w.keepalive
  end

end
