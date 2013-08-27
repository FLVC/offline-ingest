# -*- coding: utf-8 -*-
require 'nokogiri'
require 'ostruct'
require 'offin/errors'
require 'offin/utils'
require 'mime/types'


class SaxDocument < Nokogiri::XML::SAX::Document

  @@debug = false  # hmmm... wonder what this is for?

  def self.debug= value
    @@debug = value
  end

  include Errors     # We use this mixin for almost all our classes in
                     # offline ingest; here, it also nicely overrides
                     # the built-in sax parser methods error and
                     # warning (even though the method signatures are
                     # a bit different!).

  def initialize
    @current_string = ''      # the actual character data content
                              # between parsed elements; subclasses
                              # will play with this (usually truncating
                              # it at the 'end_element' event)
    super()
  end

  def characters string
    @current_string += string.strip  # This may be a bit harsh to leading whitespace...
  end

end # of class SaxDocument

class SaxDocumentAddDatastream < SaxDocument

  # Handler for parsing the returned XML document from a successful addDatastream request. For example:
  #
  #     <?xml version="1.0" encoding="UTF-8"?>
  #     <datastreamProfile  xmlns="http://www.fedora.info/definitions/1/0/management/"
  #                         xmlns:xsd="http://www.w3.org/2001/XMLSchema"
  #                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  #                         xsi:schemaLocation="http://www.fedora.info/definitions/1/0/management/ http://www.fedora.info/definitions/1/0/datastreamProfile.xsd"
  #                         pid="alasnorid:114"
  #                         dsID="OBJ" >
  #           <dsChecksum>none</dsChecksum>
  #           <dsChecksumType>DISABLED</dsChecksumType>
  #           <dsControlGroup>M</dsControlGroup>
  #           <dsCreateDate>2012-11-02T21:05:31.834Z</dsCreateDate>
  #           <dsFormatURI></dsFormatURI>
  #           <dsInfoType></dsInfoType>
  #           <dsLabel></dsLabel>
  #           <dsLocation>alasnorid:114+OBJ+OBJ.0</dsLocation>
  #           <dsLocationType>INTERNAL_ID</dsLocationType>
  #           <dsMIME>text/plain</dsMIME>
  #           <dsSize>2305</dsSize>
  #           <dsState>A</dsState>
  #           <dsVersionID>OBJ.0</dsVersionID>
  #           <dsVersionable>true</dsVersionable>
  #     </datastreamProfile>"
  #
  #  When done parsing, SaxDocumentAddDatastream#results returns an OpenStruct with the following values:
  #
  #     "dsChecksum" => "none",
  #     "dsChecksumType" => "DISABLED",
  #     "dsControlGroup" => "M",
  #     "dsCreateDate" => "2012-11-02T21:05:31.834Z",
  #     "dsFormatURI" => "",
  #     "dsID" => "OBJ",
  #     "dsInfoType" => "",
  #     "dsLabel" => "",
  #     "dsLocation" => "alasnorid:114+OBJ+OBJ.0",
  #     "dsLocationType" => "INTERNAL_ID",
  #     "dsMIME" => "text/plain",
  #     "dsSize" => "2305",
  #     "dsState" => "A",
  #     "dsVersionID" => "OBJ.0",
  #     "dsVersionable" => "true",
  #     "pid" => "alasnorid:114"
  #
  #
  # Example usage:
  #
  #    response = ... # make request to fedora, get text of response...
  #
  #    sax_document = SaxDocumentAddDatastream.new
  #    Nokogiri::XML::SAX::Parser.new(sax_document).parse(response)
  #
  #    puts sax_document.results

  attr_reader :results

  FIELDS = [ "dsChecksum", "dsChecksumType", "dsControlGroup", "dsCreateDate", "dsFormatURI", "dsID", "dsInfoType", "dsLabel",
             "dsLocation", "dsLocationType", "dsMIME", "dsSize", "dsState", "dsVersionID", "dsVersionable", "pid" ]

  def initialize
    @record   = {}   # temp hash for collecting data
    @results  = nil
    super()
  end

  def start_element name, attributes = []
    if name == 'datastreamProfile'
      attributes.each do |attribute|
        @record['pid']  = attribute[1] if attribute[0] == 'pid'    # see above example XML - only two cases where info is on attributes.
        @record['dsID'] = attribute[1] if attribute[0] == 'dsID'
      end
    end
  end

  def end_element name
    @record[name]  = @current_string unless name == 'datastreamProfile'
    @current_string = ''
  end

  def end_document
    FIELDS.each { |elt|  @record[elt] = "" unless @record[elt] }  # just defensive coding - "Can't Happen" that fedora would exclude a field, but just in case...
    @results = OpenStruct.new(@record)
  end

end # of class SaxDocumentAddDatastream


class SaxDocumentGetNextPID < SaxDocument

  # Handler for parsing the returned XML document from a successful
  # getNextPID request to fedora; an XML document returned for a
  # request of three PIDs looks something like:
  #
  #     <?xml version="1.0" encoding="UTF-8"?>
  #     <pidList  xmlns:xsd="http://www.w3.org/2001/XMLSchema"
  #               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  #               xsi:schemaLocation="http://www.fedora.info/definitions/1/0/management/ http://alpo.fcla.edu:8080/getNextPIDInfo.xsd">
  #         <pid>alasnorid:43</pid>
  #         <pid>alasnorid:44</pid>
  #         <pid>alasnorid:45</pid>
  #     </pidList>
  #
  # when done parsing, DocumentParseGetNextPID#pids returns the array [ "alasnorid:43", "alasnorid:44", "alasnorid:45" ]

  attr_reader :pids

  def initialize
    @pids     = []            # array of PIDs for content of <pid>...</pid> elements
    super()
  end

  def start_element name, attributes = []
  end

  def end_element name
    @pids.push @current_string if name == 'pid'
    @current_string = ''
  end
end  # of class SaxDocumentGetNextPID

class SaxDocumentExamineMods < SaxDocument

  # Handler for checking if a document is a simple MODS file, and for
  # extracting some information for later handling, namely the schema
  # location.

  # <?xml version="1.0" encoding="UTF-8"?>
  # <mods xmlns="http://www.loc.gov/mods/v3"
  #       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  #       xmlns:xlink="http://www.w3.org/1999/xlink"
  #       version="3.2"
  #       xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-2.xsd">
  # u.s.w
  # </mods>
  #
  # We want only a few things out of this: to know it is an actual
  # MODS document, and to know the location of the mods_schema it
  # uses (errors, warnings, errors? and warnings? are inherited)
  #
  # Example use:
  #
  #    text = File.read(...)
  #    sax_document = SaxDocumentExamineMods.new
  #    Nokogiri::XML::SAX::Parser.new(sax_document).parse(text)
  #
  #    puts "Errors: ", *sax_document.errors \
  #         if sax_document.errors?
  #
  #    puts "Warnings: ", *sax_document.warnings \
  #         if sax_document.warnings?
  #
  #    puts "It's a MODS document" \
  #         if sax_document.is_simple_mods
  #
  #    puts "You can find its schema at " + sax_document.mods_schema_location \
  #         if sax_document.mods_schema_location

  MODS_NAMESPACE = %r{^http://www.loc.gov/mods}i

  attr_reader :mods_schema_location, :prefix

  def initialize
    @is_simple_mods = false
    @depth = 0
    @declared_version = nil
    @schema_locations = {}
    @mods_schema_location = nil
    @prefix = nil
    super()
  end

  def is_simple_mods?
    @is_simple_mods
  end


  def find_mods_namespace namespaces
    candidates = []
    namespaces.each do |prefix, namespace|
      candidates.push prefix  if namespace =~ MODS_NAMESPACE
    end
  end



  def end_element_namespace name, prefix = nil, uri = nil
    @depth -= 1
  end

  def start_element_namespace name, attributes = [], prefix = nil, uri = nil, ns = []
    @depth += 1

    # We're handling the case for when <mods ...> is the first element
    # of the document.  We're also trying to locate the schema document
    # used for this MODS document (we'll default to versoin 3.4) and
    # get the prefix used....



    if name == 'mods' and @depth == 1 and uri =~ MODS_NAMESPACE
      @is_simple_mods = true

      find_mods_namespace ns

      @prefix = prefix  # if nil, we get to use the default namespace.

      attributes.each do |a|
        case
        when a.localname == 'schemaLocation'
          a.value.split(/\s+/).each_slice(2) do |namespace, location|
            @schema_locations[namespace] = location
          end
        when a.localname == 'version'
          @declared_version = a.value
        end
      end

      if not @schema_locations[uri]
        warning "The MODS document does not specify a schemaLocation for '#{uri}', assuming 'http://www.loc.gov/standards/mods/v3/mods-3-4.xsd'"
        @mods_schema_location = "http://www.loc.gov/standards/mods/v3/mods-3-4.xsd"
      else
        @mods_schema_location = @schema_locations[uri]
      end
    end

  end
end   # of class SaxDocumentExamineMods


class SaxDocumentExtractSparql < SaxDocument

  # Handler for parsing the returned XML document from a successful resource query request.
  #
  # (See the SPARQL Query Results XML Format documented at http://www.w3.org/TR/rdf-sparql-XMLres/ for details.)
  #
  # An example for us -- given the mulgara (resource index) ITQL query:
  #
  #     select  $object $title $content from <#ri>
  #      where ($object <fedora-model:label> $title
  #        and  $object <fedora-model:hasModel> $content
  #        and ($object <fedora-rels-ext:isMemberOfCollection> <info:fedora/islandora:sp_basic_image_collection>
  #         or  $object <fedora-rels-ext:isMemberOf> <info:fedora/islandora:sp_basic_image_collection>)
  #        and  $object <fedora-model:state> <info:fedora/fedora-system:def/model#Active>)
  #      minus  $content <mulgara:is> <info:fedora/fedora-system:FedoraObject-3.0>
  #   order by  $title
  #
  # mulgara returns an XML document providing the object, title
  # and content model for all objects in the collection named
  # 'islandora:sp_basic_image_collection' as so:
  #
  # <?xml version="1.0" encoding="UTF-8"?>
  # <sparql xmlns="http://www.w3.org/2001/sw/DataAccess/rf1/result">
  #   <head>
  #     <variable name="object"/>
  #     <variable name="title"/>
  #     <variable name="content"/>
  #   </head>
  #   <results>
  #     <result>
  #       <object uri="info:fedora/islandora:475"/>
  #       <title>A Page from Woody Guthrie's Diary: New Years Resolutions</title>
  #       <content uri="info:fedora/islandora:sp_basic_image"/>
  #     </result>
  #       ....
  #       ....
  #       ....
  #     <result>
  #       <object uri="info:fedora/islandora:438"/>
  #       <title>Rumble with the Gainesville Roller Rebels!</title>
  #       <content uri="info:fedora/islandora:sp_basic_image"/>
  #     </result>
  #   </results>
  # </sparql>
  #
  # We're going to parse it and provide a list of structs with the
  # slots 'object', 'title', 'content', one for each <result>, e.g.:
  #
  #  #<OpenStruct object="info:fedora/islandora:475", content="info:fedora/islandora:sp_basic_image", title="A Page from Woody Guthrie's Diaries: New Years Resolutions">
  #  #<OpenStruct object="info:fedora/islandora:438", content="info:fedora/islandora:sp_basic_image", title="Rumble with the Gainesville Roller Rebels!">
  #
  # This is NOT optimized for big lists: we'd need to stream output
  # from the mulgara triple store and parse it in chunks, yielding the
  # structs as we go. This is very possible but overkill for now.

  attr_reader :results

  def initialize
    @results         = []      # holds all the records (OpenStructs) we parse
    @variables       = {}      # keys are the variables named in a <head> section, values always true
    @parsing_result  = false   # keep state
    @current_result  = nil     # a temporary hash for holding the data as we parse
    super()
  end

  def start_element name, attributes = []

    case name

    # if name == 'variable', we are in the heading, collecting the names of variables that
    # will appear in later <result> sections:
    when 'variable'

      @variables[ attributes.assoc('name')[1] ] = true  if attributes.assoc('name')

    # here we enter a result record (we have a list of all variables parsed
    # from the heading section at this point):

    when 'result'
      @parsing_result = true
      @current_result = {}
      @variables.keys.each do |var|
        @current_result[var] = nil   # paranoia: make sure we'll have complete set of slots for missing data (can't happen, but...)
      end

    # we have entered a field for one particular result record. it may
    # have it's data as an attribute (only one, I hope) or as
    # character data. In this latter case, we'll have to get it when we're
    # done with the element (see below):

    else
      if @parsing_result and @variables[name]
        @current_result[name] = (attributes.empty?  ? nil : attributes[0][1])
      end

    end
  end


  def end_element name

    # If we're done with one of the named elements under a <result>,
    # let's check if it got a value assigned from an attribute (see
    # above). If not, the value gets assigned the character data:

    if @parsing_result and @variables[name]
      @current_result[name] = @current_string if @current_result[name].nil?
    end

    # If we're done with a <result>, bundle up its hash into an open
    # struct and stash it.

    if name == 'result'
      @parsing_result = false
      @results.push OpenStruct.new(@current_result)
    end

    @stack.pop
    @current_string = ''
  end
end   # of class SaxDocumentExtractSparql


# ManifestSaxDocument parses out these kinds of XML files (no schema yet)
#
# <manifest xmlns="info:flvc/manifest">
#
#     <contentModel>
#         islandora:sp_basic_image
#     </contentModel>
#
#     <collection>
#         NCF:slides
#     </collection>
#
#     <submittingInstitution>
#         NCF
#     </submittingInstitution>
#
#     <owningInstitution>
#         NCF
#     </owningInstitution>
#
#     <objectHistory source="digitool">
#         admin_unit="NCF01", ingest_id="ing13302", creator="creator:SNORRIS", creation_date="2012-07-20 14:40:25", modified_by="creator:SNORRIS", modification_date="2012-07-20 14:40:48"
#     </objectHistory>
#
#     <embargo rangeName="fsu campus" endDate="2016-12-29"/>
# </manifest>
#

class ManifestSaxDocument < SaxDocument

  @@institutions = nil
  @@content_models = nil


  attr_reader :collections, :content_model, :embargo, :identifiers, :object_history, :other_logos, :label, :content_model,
              :owning_institution, :submitting_institution, :owning_user, :valid

  def self.institutions= value
    @@institutions = value
  end

  def self.content_models= value
    @@content_models = value
  end

  def initialize

    raise "You must set ManifestSaxDocument.institutions to an array of institutional codes before using this class." unless @@institutions
    raise "You must set ManifestSaxDocument.content_models to an array of islandora content models before using this class." unless @@content_models

    @stack  = []     # only used for debugging
    @bogons = {}     # collect unrecognized elements

    @elements = {}   # dictionary with keys by element names (collection, contentModel), values are lists, generally of strings from XML character data

    [ 'collection', 'contentModel', 'embargo', 'identifier', 'label', 'objectHistory', 'otherLogo', 'owningInstitution', 'owningUser', 'submittingInstitution' ].each do |name|
      @elements[name] = []
    end

    @valid = true

    @collections = []
    @identifiers = []
    @other_logos = []
    @object_history = []
    @embargos = []

    @label = nil
    @content_model = nil
    @owning_institution = nil
    @submitting_institution = nil
    @owning_user = nil
    @embargo = nil

    super()
  end

  private

  # What manifests are all about:
  #
  #    Manifest Element      | Required | Repeatable    | Allowed Character Data                                  | Notes
  #    ----------------------+----------+---------------+---------------------------------------------------------+------------------------------------------------------
  #    collection            | yes      | yes           | existing Islandora collection object id                 | will create collection on the fly for digitool
  #    contentModel          | yes      | no            | islandora:{sp_pdf,sp_basic_image,sp_large_image_cmodel} |
  #    identifier            | no       | yes           | no embedded spaces?                                     | additional identifiers to be saved
  #    label                 | no       | no            | any UTF-8 string?                                       | used when displaying the object or object thumbnail
  #    otherLogo             | no       | yes           | existing drupal code                                    | determines logo for multibranding
  #    owningInstitution     | yes      | no            | FLVC, UF, FIU, FSU, FAMU, UNF, UWF, FIU, FAU, NCF, UCF  |
  #    owningUser            | yes      | no            | valid drupal user                                       | should have submitter role across owningInstitution
  #    submittingInstitution | no       | no            | FLVC, UF, FIU, FSU, FAMU, UNF, UWF, FIU, FAU, NCF, UCF  | defaults to owningInstitution
  #    embargo               | no       | not currently | n/a                                                     | required attribute rangeName, optional expirationDate

  # Textualize the attribute data off a stack element (ignore the :name key)

  def prettify hash
    text = []
    hash.keys.each do |k|
      next if k == :name  # element name
      text.push k + ' => ' + hash[k].inspect
    end
    return '{ ' + text.sort.join(', ') + ' }'
  end

  def stack_dump
    @stack.map { |h| h[:name] }.join(' => ') + '  ' + prettify(@stack[-1])
  end

  # We'll maintain a stack of elements and their attributes: each
  # element of the stack is a hash, with the name of the element keyed
  # by symbol :name; all the other key/value pairs, all strings, are
  # the attributes.

  def start_element_namespace name, attributes = [], prefix = nil, uri = nil, ns = []
    datum = { :name => name }
    datum['prefix'] = prefix if prefix
    datum['uri']    = uri if uri
    datum['ns']     = ns unless ns.empty?

    hash = {}
    attributes.each do |at|
      hash[at.localname] = at.value;
      datum[at.localname] = at.value
    end

    @elements[name].push hash if [ 'objectHistory', 'embargo' ].include? name

    @stack.push datum
    puts stack_dump if @@debug
  end

  def end_element_namespace name, prefix = nil, uri = nil
    case name

    when 'collection', 'contentModel', 'embargo', 'identifier', 'label', 'otherLogo', 'owningInstitution', 'owningUser', 'submittingInstitution'
      @elements[name].push @current_string unless @current_string.empty?

    when 'objectHistory'

      hash = @elements['objectHistory'].pop || { }
      hash['data'] = @current_string
      @elements['objectHistory'].push(hash) unless @current_string.empty?

    else
      @bogons[name] = :foo  unless @stack.length == 1 # we don't care what they call the root
    end

    @stack.pop
    @current_string = ''
  end

  # The following checks are done after the document is completely read.

  # Check that there is at least one collection listed.

  def collections_ok?
    if @elements['collection'].empty?
      error "The manifest document does not contain a collection ID. At least one is required."
      return
    end

    @collections = @elements['collection']
  end


  def check_date str
    return Time.parse(str).strftime('%F') == str
  rescue => e
    return
  end

  # Check embargo

  def embargo_ok?
    return true if @elements['embargo'].empty?

    if @elements['embargo'].length > 1
      error "The manifest document contains more than one embargo,  which is currently not supported."
      return
    end

    hash = @elements['embargo'].shift

    unexpected_attributes = hash.keys - ['endDate', 'rangeName']

    unless unexpected_attributes.empty?
      error "The manifest embargo has unexpected attributes: #{unexpected_attributes.join(', ')}"
      return
    end

    if hash['data']
      warning "The manifest embargo should not have character data present '#{hash['data']}'"
    end

    if hash['rangeName'].nil? or hash['rangeName'].empty?
      error "The manifest embargo is missing the 'rangeName' attribute, which is required: #{hash.inspect}."
      return
    end

    # 'endDate' is optional and means 'forever', but if present must be a valid date in YYYY-MM-DD format

    if hash['endDate'] and not check_date(hash['endDate'])
      error "The manifest embargo attribute endDate is not a valid date: #{hash['endDate']}; it must be a valid date of the form 'YYYY-MM-DD'."
    end

    @embargo = hash
    return true
  end


  # Check that exactly one of the allowed content models is present.

  def content_model_ok?

    if @elements['contentModel'].empty?
      error "The manifest document does not contain a content model. Exactly one of #{@@content_models.join(', ')} is required."
      return
    end

    if @elements['contentModel'].length > 1
      errors "The manifest document contains multiple content models.  Exactly one of #{@@content_models.join(', ')} is required."
      return
    end

    content_model = @elements['contentModel'].shift

    unless @@content_models.include? content_model
      error "The manifest document contains an unsupported content model, #{content_model}. Exactly one of #{@@content_models.join(', ')} is required."
      return
    end

    @content_model = content_model
  end


  # Check for optional label, if present there may be only one.

  def label_ok?

    return true if @elements['label'].empty?

    if @elements['label'].length > 1
      error "The manifest document lists more than one label - at most one can be specfied."
      return
    end

    @label = @elements['label'].shift
  end

  # Check for required owningUser, there must be exactly one (relaxed requirement for now)

  def owning_user_ok?

    return true if @elements['owningUser'].empty?

    if @elements['owningUser'].length > 1
      error "The manifest document lists multiple owning users - at most, one must be present."
      return
    end

    @owning_user = @elements['owningUser'].shift   # might be nil

    return true
  end


  # check for the required owningInstitution, it must be one of the
  # allowed INSTITUTIONS

  def owning_institution_ok?

    if @elements['owningInstitution'].empty?
      error "The manifest document does not list an owning institution - it must have exacly one of #{@@institutions.join(', ')}."
      return
    end

    if @elements['owningInstitution'].length > 1
      error "The manifest document lists multitple owning institutions - it must have exacly one of #{@@institutions.join(', ')}."
      return
    end

    owning_institution = @elements['owningInstitution'].shift.upcase

    unless @@institutions.include? owning_institution
      error "The manifest document includes an invalid owning institution '#{owning_institution}' - it must have exacly one of #{@@institutions.join(', ')}."
      return
    end

    @owning_institution = owning_institution
  end

  # Check for an optional submittingInstitution - if present it must be one of the @@institutions

  def submitting_institution_ok?
    if @elements['submittingInstitution'].length > 1
      error "The manifest document lists multitple submitting institutions - it must have at most one of #{@@institutions.join(', ')}."
      return
    end

    if @elements['submittingInstitution'].length == 1
      submitting_institution = @elements['submittingInstitution'].shift.upcase

      unless @@institutions.include? submitting_institution
        error "The manifest document includes an invalid submitting institution '#{submitting_institution}' - if present, it must be one of #{@@institutions.join(', ')}."
        return
      end

      @submitting_institution = submitting_institution
    end

    return true
  end


  # optional, otherwise a list of hashes which must have 'source' and 'data', both non-empty strings

  def object_history_ok?
    return true if @elements['objectHistory'].empty?

    list = []
    @elements['objectHistory'].each do |hash|
      if hash['source'].nil? or hash['source'].empty?
        error "The manifest has an object history element that is missing the 'source' attribute: #{hash.inspect}."
        return
      end
      if hash['data'].nil? or hash['data'].empty?
        error "The manifest has an object history element that is missing data: #{hash.inspect}."
        return
      end
      list.push hash
    end
    @object_history = list
    return true
  end


  def end_document
    # optional, multivalued

    @identifiers = @elements['identifier']
    @other_logos = @elements['otherLogo']

    # do each of these so we can collect up all errors

    @valid =  collections_ok?              && @valid
    @valid =  content_model_ok?            && @valid
    @valid =  owning_user_ok?              && @valid
    @valid =  submitting_institution_ok?   && @valid
    @valid =  owning_institution_ok?       && @valid
    @valid =  label_ok?                    && @valid
    @valid =  object_history_ok?           && @valid
    @valid =  embargo_ok?                  && @valid

    @valid &&=  true   # if not false, force to 'true' value, instead of potentially confusing non-boolean that ...ok? methods might return

    warning "There were unexpected elements in the manifest:  #{@bogons.keys.sort.join(', ')}."  unless @bogons.empty?
  end
end   # of ManifestSaxDocument


Struct.new('MetsFileDictionaryEntry', :sequence, :href, :mimetype, :use, :fid)

# Helper class for SaxDocumentExamineMets; save fileSec information.

class MetsFileDictionary

  include Errors

  # A simple class to keep information from a METS subtree such as
  #
  # <METS:fileGrp USE="index">
  #   <METS:file GROUPID="GID1" ID="FID1" SEQ="1" MIMETYPE="image/jpeg">
  #     <METS:FLocat LOCTYPE="OTHER" OTHERLOCTYPE="SYSTEM" xlink:href="FI05030701_cover1.jpg" />
  #   </METS:file>
  #   ... more METS:file entries here...
  # </METS:fileGrp>
  #
  # We keep an ordered list of MetsFileDictionaryEntry structs, which can
  # returned via hash-like lookup ['FID1'] or sequentially via each.
  #
  # The MetsFileDictionaryEntry struct has entries (using the example above) with
  #
  #   dictionary.sequence => '1'                       -- we don't really use this, instead we take the sequence supplied by the structMap. TODO: maybe we'll warn if these differ...
  #   dictionary.mimetype => 'image/jpeg'              -- might be nil - we'll fill in by MIME href extension if missing.
  #   dictionary.href     => 'FI05030701_cover1.jpg'   -- has to be present; we need this so we can check if it correctly resolves to a file in the package (not done in this class).
  #   dictionary.use      => 'index'                   -- we really want this to be 'index' (full text) or 'reference' (the designated format to ingest), but we may get 'archive' in some cases.
  #   dictionary.fid      => 'FID1'                    -- this string will be present.
  #
  # .mimetype and .use are always lower-cased when strings -  :fid and :href will be present.

  def initialize
    @sequence = []
    @dict = {}
  end

  def safe_type str
    type = MIME::Types.type_for(str).shift
    return type.content_type if type
    warning "Can't determine MIME type for #{str.inspect}, using application/octet-stream."
    return 'application/octet-stream'
  rescue => e
    warning "Exception #{e.class}, '#{e.message}' trying to find MIME type for #{str.inspect}, using application/octet-stream."
    return 'application/octet-stream'
  end

  def []=(fid, value)
    value.fid = fid
    value.mimetype = safe_type(value.href) unless value.mimetype
    @dict[fid] = value
    @sequence.push fid
  end

  def [](fid)
    @dict[fid]
  end

  def each
    @sequence.each { |fid|  yield @dict[fid] }
  end

  def print
    puts self.to_s
    self.each { |elt| puts "#{elt.fid}: sequence=#{elt.sequence.inspect} mimetype=#{elt.mimetype.inspect} href=#{elt.href.inspect} use=#{elt.use.inspect}" }
  end

end


# Data formats we'll manipulate in SaxDocumentExamineMods, though when we're done only MetsDivData will be present:

Struct.new('MetsDivData',  :level, :title, :is_page, :fids, :files)
Struct.new('MetsFileData', :file_id)

# Helper class for SaxDocumentExamineMets: save structmap information.  When we're done with post_processing here, we will only
# have

class MetsStructMap

  include Errors

  attr_reader :number_files, :label

  def initialize
    @list = []
    @number_files = 0
    @label = nil
  end

  def each
    @list.each { |elt| yield elt }
  end

  # include page and sections chapters here

  def add_div hash, level

    if hash['DMDID'] =~ /DMD1/i and hash['LABEL']   # e.g. <METS:div DMDID="DMD1" LABEL="Lydia's roses" ORDER="0" TYPE="main">
      @label = hash['LABEL']
    end

    record = Struct::MetsDivData.new
    record.level   = level
    record.title   = hash['LABEL'] || hash['TYPE'] || ''
    record.is_page = hash['TYPE'] && hash['TYPE'].downcase == 'page'
    record.fids    = []
    record.files   = []
    @list.push record
  end

  def add_file hash, level
    @number_files += 1
    record = Struct::MetsFileData.new
    record.file_id = hash['FILEID']
    @list.push record
  end

  # post_process should be called after an entire METS document has been
  # read (so we know we have the complete file dictionary, too).

  def post_process file_dictionary

    # Collapse out the MetsFileData (fptr) datum nodes
    # from the list, assigning its fileid attributes to the previous
    # MetsDivData node's list of file-ids (fids).

    new = []
    @list.each do | rec |
      case rec
      when Struct::MetsDivData
        new.push rec
      when Struct::MetsFileData
        if new.empty?
          warning "METS file data #{rec.inspect} doesn't have a parent <div> node, skipping."
          next
        end
        parent = new[-1]            # grab immedidate parent, which will be a MetsDivData element
        parent.is_page = true
        parent.fids.push(rec.file_id)
      end
    end
    @list = new

    # Now we have nothing but Struct::MetsDivData; we'll first remove any record that claims to be a page
    # but has no associated fids:

    new = []
    @list.each do |div_data|
      if div_data.fids.empty? and div_data.is_page
        warning "<div> element of type page, labeled '#{div_data.title}', has no associated file data, skipped."
      else
        new.push div_data
      end
    end
    @list = new

    # We now fill in the files data from the dictionary:

    @list.each do |div_data|
      div_data.fids.each do |fid|
        file_entry = file_dictionary[fid]
        if not file_entry
          warning "METS structMap FILEID #{fid} was not found in the METS fileSec."
        else
          div_data.files.push file_entry
        end
      end
    end


    # We may have aribitrarily deep divs, so we'll adjust the level:

    return if @list.empty?

    adjustment = 1 - @list[0].level

    @list.each do |entry|
      entry.level += adjustment
    end


  end


  def print
    puts self.to_s
    @list.each  do |elt|
      puts '. ' * elt.level + elt.inspect.gsub('#<struct Struct::MetsDivData ',  '<')
    end
  end

end # of class MetsStructMap


# Class for parsing out the table of contents information in a METS file:

class SaxDocumentExamineMets < SaxDocument

  METS_NAMESPACE = %r{^http://www.loc.gov/METS/}
  VALID_FLOCAT   = %r{(fileSec:)(fileGrp:)+(file:)(FLocat)$}  ## TODO: use this with new onstack?

  include Errors

  attr_reader :xml_document, :sax_document, :file_dictionary, :label, :structmaps

  def initialize
    @stack = []                                    # keeps the nested XML elements and attributes
    @label = nil                                   # gets the label from top level mets, e.g. <METS:mets LABEL="The Title of This Book" ...> or, perhaps, the structMap label
    @file_dictionary = MetsFileDictionary.new      # collects METS data from subtree /fileGrp/file/FLocat/
    @structmaps = []                               # collects data from multiple METS structMaps
    @current_structmap = nil                       # the current METS structMap we're parsing (and acts as a flag to let us know we're in a structMap)
    super()
  end

  # Given a list of element names, return true if all of them are on
  # the stack (first of the list argument is topmost on the stack).

  def onstack? *list
    i = -1
    list.each do |el|
      return false unless @stack[i]
      return false unless @stack[i][:name] == el
      i -= 1
    end
    ####  puts onstack_again? ''
    return true
  end


  def onstack_again? regexp
    str = @stack.reverse.map { |elt| elt[:name] }.join(':')
    return str
  end


  # When we've identified a 'FLocat' subtree, place it and some of its parent data into the dictionary:

  def handle_file_dictionary

    # Won't work for some cases:
    # return unless  onstack? 'FLocat', 'file', 'fileGrp', 'fileSec'   # leftmost is towards top of stack, très confusing!
    #
    # we came up with case of multiply nested fileGrp, e.g.
    #
    #  mets => fileSec  {  }
    #  mets => fileSec => fileGrp  { USE => "VIEW", VERSDATE => "2007-06-21T14:30:26.374Z" }
    #  mets => fileSec => fileGrp => fileGrp  {  }
    #  mets => fileSec => fileGrp => fileGrp => file  { CREATED => "2007-06-21T14:30:26.421Z", GROUPID => "pg001view", ID => "pg001m1", MIMETYPE => "image/jpeg", USE => "VIEW" }
    #  mets => fileSec => fileGrp => fileGrp => file => FLocat  { LOCTYPE => "URL", href => "METSID-1" }

    # TODO: need wild card for onstack? to make sure we're under fileSec!

    return unless  onstack? 'FLocat', 'file', 'fileGrp'

    # stack  text
    # -----  ----------
    # [-3]   <METS:fileGrp USE="index">
    # [-2]     <METS:file GROUPID="GID1" ID="FID1" SEQ="1" MIMETYPE="image/jpeg">
    # [-1]       <METS:FLocat LOCTYPE="OTHER" OTHERLOCTYPE="SYSTEM" xlink:href="FI05030701_cover1.jpg" />
    #          </METS:file>
    #   ...
    # </METS:fileGrp>

    flocat_element, file_element, file_group = @stack[-1], @stack[-2], @stack[-3]

    if not file_element['ID']
      warning "METS file element #{file_element.inspect} doesn't have an ID, skipping."
      return
    end

    if not flocat_element['href']
      warning "METS FLocat element #{flocat_element.inspect} doesn't have an href, skipping."
      return
    end

    fid = file_element['ID']

    data = Struct::MetsFileDictionaryEntry.new

    data.sequence = file_element['SEQ']                        #
    data.href     = Utils.xml_unescape(flocat_element['href']) #
    data.mimetype = safe_downcase(file_element['MIMETYPE'])    # expected 'image/jp2' etc.
    data.use      = safe_downcase(file_group['USE'])           # expected limited set: 'archive', 'thumbnail', 'reference', 'index'.  In general we'll only be using the last two (image, ocr)

    @file_dictionary[fid] = data
  end

  # Grab the value of the LABEL attribute from the topmost mets element; a label from a DMD sec may be assigned to the structmap as well.

  def handle_label
    if text = @stack[-1]['LABEL']  # cleanup whitespace
      @label = text.split(/\s+/).join(' ').strip
    end
  end

  def safe_downcase text
    return text unless text.class == String
    return text.downcase.strip
  end

  # Textualize the atttribute data off a stack element for printing
  # (ignore the :name key, the XML element name)

  def prettify hash
    text = []
    hash.keys.each do |k|
      next if k == :name  # element name
      text.push k + ' => ' + hash[k].inspect
    end
    return '{ ' + text.sort.join(', ') + ' }'
  end

  def stack_dump
    @stack.map { |h| h[:name] }.join(' => ') + '  ' + prettify(@stack[-1])
  end

  def handle_structmap_begin
    @current_structmap = MetsStructMap.new
  end

  def handle_structmap_end
    @structmaps.push @current_structmap
    @current_structmap = nil             # N.B. also used as flag to show we're not currently in the structMap parsing state
  end


  def div_level
    return @stack.map { |elt| elt[:name] == 'div' }.length
  end

  def handle_structmap_update
    return unless @current_structmap    # e.g, we got a <div> or <fptr> but not inside a structmap
    level, elt = div_level, @stack[-1]

    # example div records from stack:
    #
    #  { "ID"=>"D1", "ORDER"=>"1", "TYPE"=>"Main", :name=>"div" }
    #  { "LABEL"=>"Page vii", "ID"=>"P7", :name=>"div", "TYPE"=>"Page" }
    #
    # example fptr elements from stack:
    #
    #  { "FILEID"=>"E7", :name=>"fptr" }

    case elt[:name]
    when 'div';    @current_structmap.add_div(elt, level)
    when 'fptr';   @current_structmap.add_file(elt, level)
    end
  end

  # We'll maintain a stack of elements and their attributes: each
  # entry in the stack is a hash, with the name of the XML element
  # keyed by the symbol :name; the remaining key/value pairs, all
  # strings, are the XML attributes of the elements. Character data
  # are not relevent here.

  def start_element_namespace name, attributes = [], prefix = nil, uri = nil, ns = []

    return unless uri =~ METS_NAMESPACE

    hash = { :name => name }
    attributes.each { |at|  hash[at.localname] = at.value }
    @stack.push hash

    puts stack_dump if @@debug

    case name
    when 'structMap';     handle_structmap_begin
    when 'fptr', 'div';   handle_structmap_update
    when 'FLocat';        handle_file_dictionary
    when 'mets';          handle_label
    end
  end

  # Pop the stack when we're done, and if we've done a structMap, do
  # some special processing.

  def end_element_namespace name, prefix = nil, uri = nil

    return unless uri =~ METS_NAMESPACE

    handle_structmap_end if name == 'structMap'
    @stack.pop
    @current_string = ''
  end

  # All data has now been collected into one MetsFileDictionary object
  # (a list of Struct::MetsFileData thangs) and one or more
  # MetsStructMap objects (a list of Struct::MetsDivData objects).
  #
  # TODO: resolve some appropriate issues here: warn if sequences don't match (??)

  def end_document

    @structmaps.each do |structmap|
      structmap.post_process(@file_dictionary)
      if structmap.warnings?
        warning "Warnings when post-processing structMap:"
        warning structmap.warnings
      end
      if structmap.errors?
        error "Errors when post-processing structMap:"
        error structmap.errors
      end
    end

    if @file_dictionary.warnings?
      warning "Warnings when post-processing file section:"
      warning @file_dictionary.warnings
    end
    if @file_dictionary.errors?
      error "Errors when post-processing file section:"
      error @file_dictionary.errors
    end

    if @@debug
      puts "structMap count: #{@structmaps.length}"
      @structmaps.each { |sm| sm.print }
    end
  end
end  # of class SaxDocumentExamineMets


# Next section is for fixing up the infamous METSID-NNN issue.


Struct.new('StreamRef', :file_name, :file_id, :file_size_bytes)

class DigitoolStreamRef < SaxDocument

# for parsing out the following data, from which want file_name, file_id, and file_size_bytes

# ...
# <stream_ref>
#   <file_name>163960_41965_pg016_utview_m1_toc14_lblAll About A Frog.jpg</file_name>
#   <file_extension>jpg</file_extension>
#   <mime_type>image/jpeg</mime_type>
#   <directory_path>/exlibris4/dtl/storage/2009/02/16/file_3/163960</directory_path>
#   <file_id>METSID-16</file_id>
#   <storage_id>1063</storage_id>
#   <external_type>-1</external_type>
#   <file_size_bytes>959430</file_size_bytes>
# </stream_ref>
# ....
#
# Yikes?! Can also have:
#
#   <file_name>
#     <![CDATA[2657397_pg305_utview_m1_toc29_lblRobert Clark & Co.'s Publications.jp2]]>
#   </file_name>


  attr_reader :stream_ref

  def initialize
    @cdata = ''
    @in_stream_ref = false
    @stream_ref = Struct::StreamRef.new
    super
  end


  def start_element_namespace name, attributes = [], prefix = nil, uri = nil, ns = []
    @in_stream_ref = true if name == 'stream_ref'
  end

  def cdata_block cdata
    @cdata = cdata
  end

  def best_data
    return @current_string if not @current_string.empty?
    return @cdata
  end

  def end_element_namespace name, prefix = nil, uri = nil
    if @in_stream_ref
      case
      when name == 'file_name';         @stream_ref.file_name       = best_data()
      when name == 'file_id';           @stream_ref.file_id         = @current_string
      when name == 'file_size_bytes';   @stream_ref.file_size_bytes = @current_string
      end
    end
    @in_stream_ref = false if name == 'stream_ref'

    @current_string = ''
    @cdata = ''
  end

end #
