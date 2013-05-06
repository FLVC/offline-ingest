require 'rubydora'
require 'offin/document-parsers'
require 'offin/mods'
require 'offin/errors'


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
  # TODO: error handling for: repository (can't connect?, etc);  create (???); ....
  # TODO: try to run down pid and delete if error after a datastream or object save occurs...


  attr_reader :repository, :pid, :namespace, :fedora_object

  # We use the yield self idiom here:
  #
  #  Ingestor.new(...) do |ingestor|
  #     ingestor.do your thing..
  #  end


  def initialize  config, namespace
    @config = config

    @repository = Rubydora.connect :url => @config.url, :user => @config.user, :password => @config.password
    @namespace = namespace

    @pid = getpid
    @fedora_object = @repository.create(@pid)
    @owner = nil


    yield self

    @fedora_object.save
  end


  # TODO: check pid, repository are properly returned here...

  def getpid
    sax_document = SaxDocumentGetNextPID.new
    pid_doc = @repository.next_pid(:namespace => @namespace)
    Nokogiri::XML::SAX::Parser.new(sax_document).parse(pid_doc)
    return sax_document.pids.shift
  end


  def collections= value
    value.each do |pid|
      fedora_pid = (pid =~ /^info:fedora/ ? pid : "info:fedora/#{pid}")
      create_new_collection_if_necessary fedora_pid
      @fedora_object.memberOfCollection << fedora_pid
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

  # TODO: XMLescape values here for next two?

  def label= value
    @fedora_object.label = value
  end

  def owner= value
    @owner = value
    @fedora_object.ownerId = @owner
  end

  def datastream name
    yield @fedora_object.datastreams[name]
    @fedora_object.datastreams[name].save
  end


  # dealing with collections for this object to be ingested into

  def existing_collections
    query = "select $object $title from <#ri> " +
             "where $object <fedora-model:label> $title " +
               "and $object <fedora-model:hasModel> <info:fedora/islandora:collectionCModel>"

    @repository.itql(query).map{ |row| row[0] }
  end

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
    label = 'digitool collection: ' + collection_pid.sub(/^info:fedora\//, '').sub(/^.*:/, '')
    return if existing_collections.include? collection_pid

    warning "Creating new digitool collection #{collection_pid} for object #{@pid}."

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
