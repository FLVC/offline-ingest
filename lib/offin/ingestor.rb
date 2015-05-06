require 'rubydora'
require 'offin/document-parsers'
require 'offin/mods'
require 'offin/errors'

# The ingestor class is a simple wrapper for datastream-level
# ingests. It uses a copy of the rubydora library to do the heavy
# lifting.


# TODO:  have to yank ingestor warnings and errors into Package object,




# Extend RI mixins to include itql queries:

module Rubydora
  module ResourceIndex
    def itql query
      if CSV.const_defined? :Reader
        FasterCSV.parse(self.risearch(query, :lang => 'itql'), :headers => true)
      else
        CSV.parse(self.risearch(query, :lang => 'itql'), :headers => true)
      end
    end
  end
end


class Ingestor

  include Errors

  # TODO: sanity check on config object, throw error that should stop all processing


  attr_reader :repository, :pid, :namespace, :fedora_object, :size

  # We use the yield self idiom here:
  #
  #  ingestor = Ingestor.new(...) do |ingestor|
  #     ingestor.do-your-thing..
  #  end
  #
  #  puts ingestor.warnings


  def initialize  config, namespace

    @owner         = nil
    @size          = 0
    @config        = config
    @namespace     = namespace
    @repository    = connect @config
    @pid           = getpid
    @fedora_object = @repository.create(@pid)

    yield self

    @fedora_object.save

    # TODO: not sure if we should just let these percolate up, or just non-package errors, or what, exactly

  rescue SystemError => e
    raise e
  rescue PackageError => e
    error e.message
    return self
  rescue Errno::ENOENT => e
    error "Caught exception while processing pid #{@pid} - #{e.class} #{e.message}"
    return self
  rescue => e
    error "Caught exception while processing pid #{@pid} - #{e.class} #{e.message}, backtrace follows:", e.backtrace
    return self
  end


  def connect config
    Utils.field_system_error("For repository '#{@config.fedora_url}'")   do
      repository = Rubydora.connect :url => config.fedora_url, :user => config.user, :password => config.password
      repository.ping
      return repository
    end
  end


  def getpid
    Utils.field_system_error("Unable to request a new PID from the fedora repository '#{@config.fedora_url}'")   do
      pid_doc = @repository.next_pid(:namespace => @namespace)
      sax_document = SaxDocumentGetNextPID.new
      Nokogiri::XML::SAX::Parser.new(sax_document).parse(pid_doc)
      return sax_document.pids.shift
    end
  end

  def collections= value
    value.each do |pid|
      fedora_pid = (pid =~ /^info:fedora/ ? pid : "info:fedora/#{pid}")
      create_new_collection_if_necessary fedora_pid
      @fedora_object.memberOfCollection << fedora_pid
    end
  end

  def dc= value
    datastream('DC') do |ds|
      ds.dsLabel  = "Dublin Core Record"
      ds.content  = value
      ds.mimeType = 'text/xml'
    end
  end

  def mods= value
    datastream('MODS') do |ds|
      ds.dsLabel  = "MODS Record"
      ds.content  = value
      ds.mimeType = 'text/xml'
    end
  end

  def fixup_pid pid
    return pid =~ /^info:fedora/ ?  pid : "info:fedora/#{pid}"
  end

  def content_model= value
    @fedora_object.models << fixup_pid(value)
  end

 # TODO: XMLescape values here for next two?  It may be escaped by rubydora library, check.

  def label= value
    @fedora_object.label = value
  end

  def owner= value
    @owner = value
    @fedora_object.ownerId = @owner
  end

  def datastream name
    trials ||= 0
    yield @fedora_object.datastreams[name]
    @size += @fedora_object.datastreams[name].content.size
    @fedora_object.datastreams[name].save
  rescue
    trials += 1
    if trials < 3
      sleep trials
      retry
    else
      raise
    end
  end

  # def add_relationship predicate, object
  #   @fedora_object.add_relationship predicate, object
  # end

  # dealing with collections for this object to be ingested into

  def existing_collections
    query = "select $object $title from <#ri> " +
            "where $object <fedora-model:label> $title " +
            "and " +
            "($object <fedora-model:hasModel> <info:fedora/islandora:collectionCModel> " +
            "or " +
            "$object <fedora-model:hasModel> <info:fedora/islandora:newspaperCModel>)"

    @repository.itql(query).map{ |row| row[0] }
  end


  # TODO: does label below needs to be xml escaped at some point?

  def collection_policy_text label
    str = ''
    @config.content_models.each do |pid, name|
      str += "          <content_model name=\"#{name}\" dsid=\"ISLANDORACM\" namespace=\"#{@namespace}\" pid=\"#{pid}\"/>\n"
    end

    return <<-XML.gsub(/^     /, '')
     <collection_policy xmlns="http://www.islandora.ca"
                        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                        name="#{label}"
                        xsi:schemaLocation="http://www.islandora.ca http://syn.lib.umanitoba.ca/collection_policy.xsd">
       <content_models>
          #{str.strip}
       </content_models>
       <relationship>
          isMemberOfCollection
       </relationship>
       <search_terms/>
       <staging_area/>
     </collection_policy>
    XML
  end


  def create_new_collection_if_necessary collection_pid

    label = 'collection ' + collection_pid.sub(/^info:fedora\//, '').sub(/^.*:/, '')
    return if existing_collections.include? collection_pid

    warning "Creating new collection named '#{label}' for object #{@pid}."

    collection_object = @repository.create(collection_pid)

    collection_object.memberOfCollection << @config.root_collection
    collection_object.models << 'info:fedora/islandora:collectionCModel'
    collection_object.label   = label
    collection_object.ownerId = @owner

    ds = collection_object.datastreams['TN']
    ds.dsLabel  = "Thumbnail"
    ds.content  = File.read(@config.collection_thumbnail_filename)
    ds.mimeType = 'image/png'

    ds = collection_object.datastreams['COLLECTION_POLICY']
    ds.dsLabel      = "Collection Policy"
    ds.content      = collection_policy_text label
    ds.mimeType     = 'text/xml'
    ds.controlGroup = 'X'

    collection_object.save

    # we'll wait up to 15 seconds for the collection to be created

    30.times do
      sleep 0.5
      return if existing_collections.include? collection_pid
    end

    raise PackageError, "Could not create collection #{collection_pid} for new object #{@pid}."
  end


end # of class Ingestor
