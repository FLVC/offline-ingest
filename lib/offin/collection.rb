require 'rubydora'
require 'offin/document-parsers'
require 'offin/mods'

# Extend RI mixins to include itql queries

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

class Collection

  attr_reader :pid, :label

  # TODO: check that namespace, pid, and label do not have to be
  # XML-escaped... label may need to be, but (I Guess) it should be
  # escaped when inserted into some XML content.  Have to know the
  # rules for this...

  def initialize config, collection_pid, collection_label = nil

    @config     = config
    @repository = Rubydora.connect :url => @config.url, :user => @config.user, :password => @config.password
    @namespace  = config.namespace


    @pid   = collection_pid =~ /^info:fedora/ ? collection_pid : 'info:fedora/' + collection_pid
    @label = collection_label || "Collection #{@pid.sub(/^info:fedora\//, '')}."

    create_new_collection unless collections.include? @pid
  end


  # Go grab the list of collections as they exist right now:

  def collections
    query = "select $object $title from <#ri> where ($object <fedora-model:label> $title and $object <fedora-model:hasModel> <info:fedora/islandora:collectionCModel>)"
    @repository.itql(query).map{ |row| row[0] }
  end


  def collection_policy_text
    str = ''
    @config.content_models.each do |pid, name|
      str += "         <content_model name=\"#{name}\" dsid=\"ISLANDORACM\" namespace=\"#{@namespace}\" pid=\"#{pid}\"/>\n"
    end

    return <<-XML.gsub(/^     /, '')
     <collection_policy xmlns="http://www.islandora.ca" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" name="#{@label}" xsi:schemaLocation="http://www.islandora.ca http://syn.lib.umanitoba.ca/collection_policy.xsd">
       <content_models>
         #{str.strip}
       </content_models>
       <search_terms/>
       <staging_area/>
       <relationship>isMemberOfCollection</relationship>
     </collection_policy>
    XML
  end

  private

  # create_new_collection: we require @pid in full form, e.g. 'info:fedora/islandora:collection'

  # TODO: check for well-formedness of collection pid
  # TODO: should check not only if the named collection exists, but if the PID is used for any kind of object.

  def create_new_collection

    object = @repository.create(@pid)

    object.memberOfCollection << @config.root_collection
    object.models << 'info:fedora/islandora:collectionCModel'
    object.label   = @label
    object.ownerId = @config.object_owner    # TODO: eventually, we'll want a sanity check here that it exists in drupal. Not necessary for digitool migration

    ds = object.datastreams['TN']
    ds.dsLabel  = "Thumbnail"
    ds.content  = File.read(@config.collection_thumbnail_filename)
    ds.mimeType = 'image/png'

    # TODO: check that we're getting versionable correct

    ds = object.datastreams['COLLECTION_POLICY']
    ds.dsLabel      = "Collection Policy"
    ds.content      = collection_policy_text
    ds.mimeType     = 'text/xml'
    ds.controlGroup = 'X'

    object.save

    20.times do
      sleep 0.5
      return if collections.include? @pid
    end

    raise "Collection #{@pid} wasn't created."

  rescue => e
    raise CollectionError, "#{e.message}"
  end
end
