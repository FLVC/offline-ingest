require 'rubydora'
require 'offin/document-parsers'
require 'offin/mods'
require 'offin/collection'

class Ingestor

  # TODO: sanity check on config object, throw error that should stop all processing
  # TODO: this should get an already vetted repo and config, and be focused on one object
  # TODO: error handling for: repository (can't connect?, etc);  create (???); ....
  # TODO: stash error and warning messages

  attr_reader :repository, :pid, :namespace, :fedora_object, :errors, :warnings

  def initialize  config, namespace
    @config = config

    @repository = Rubydora.connect :url => @config.url, :user => @config.user, :password => @config.password
    @namespace = namespace

    @errors = []
    @warnings = []

    @pid = getpid
    @fedora_object = @repository.create(@pid)

    yield self

  rescue => e
    STDERR.puts "Yikes! A #{e.class}! Pssst... #{e.message}"
    attempt_delete(@pid)
    # figure out whether we should throw a SystemError or PackageError here.... or just load @errors
    raise SystemError, "Ingestor Error: #{e.class} - #{e.message}"
  end

  # TODO: run down if possible

  def attempt_delete pid
    return unless pid
    # return if nil, otherwise attempt to connect to repository and delete the PID
    #
  rescue => e
    @warnings.push "When handling an error and trying to delete partial object #{pid}, got an additinal error #{e.class}: #{e.message}"
  end

  def error string
    @errors.push string
  end

  def errors?
    not @errors.empty?
  end

  def warning string
    @warnings.push string
  end

  def warnings?
    not @warnings.empty?
  end

  # TODO: check pid, repository are properly returned here...

  def getpid
    sax_document = SaxDocumentGetNextPID.new
    Nokogiri::XML::SAX::Parser.new(sax_document).parse(@repository.next_pid(:namespace => @namespace))
    return sax_document.pids.shift
  end

  def collections= value
    value.each do |pid|
      collection = Collection.new(@config, @namespace, pid)  # TODO: move Collection back into here....
      @fedora_object.memberOfCollection << collection.pid
    end
  end

  def dc= value
    ds = @fedora_object.datastreams['DC']
    ds.dsLabel  = "Dublin Core Record"
    ds.content  = value
    ds.mimeType = 'text/xml'
  end

  def mods= value
    ds = @fedora_object.datastreams['MODS']
    ds.dsLabel  = "MODS Record"
    ds.content  = value
    ds.mimeType = 'text/xml'
  end

  def content_model= value
    @fedora_object.models << ( value =~ /^info:fedora/ ?  value : "info:fedora/#{value}" )
  end

  def label= value
    @fedora_object.label = value
  end

  def owner= value
    @fedora_object.ownerId = value
  end

  def datastream name
    yield @fedora_object.datastreams[name]
  end

  def ingest
    @fedora_object.save
  end

end # of class Ingestor
