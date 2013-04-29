require 'nokogiri'
require 'ostruct'

# Boiler plate for the way I parse SAX documents:

class FedoraSaxDocument < Nokogiri::XML::SAX::Document

  attr_reader :warnings, :errors

  def initialize
    @errors   = []            # array of strings of errors encountered during processing
    @warnings = []            # ditto, for warnings
    @current_string = ''      # the actual character data content between parsed elements; subclasses will play with this (usually resetting it at the 'end_element' event)

    super()
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

  def characters string
    @current_string += string.strip
  end
end

class SaxDocumentAddDatastream < FedoraSaxDocument

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

end


class SaxDocumentGetNextPID < FedoraSaxDocument

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
    @state    = nil           # two states: parsing <pid>...</pid>, or not (nil)

    super()
  end

  def start_element name, attributes = []
    @state = 'pid'  if name == 'pid'
  end

  def end_element name
    if name == 'pid'
      @state = nil
      @pids.push @current_string
    end
    @current_string = ''
  end
end


class SaxDocumentExamineMods < FedoraSaxDocument

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


  attr_reader :mods_schema_location

  def initialize
    @is_simple_mods = false
    @depth = 0
    @declared_version = nil
    @schema_locations = {}
    @mods_schema_location = nil
    super()
  end

  def is_simple_mods?
    @is_simple_mods
  end

  def end_element name
    @depth -= 1
  end

  def start_element_namespace name, attributes = [], prefix = nil, uri = nil, ns = []
    @depth += 1

    # We're handling the case for when <mods ...> is the first element
    # of the document.  We're also trying to locate the schema document
    # used for this MODS document.

    if name == 'mods' and @depth == 1 and uri =~ %r{^http://www.loc.gov/mods}i
      @is_simple_mods = true

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
      @mods_schema_location = @schema_locations[uri]
    end

  end
end


class SaxDocumentExtractSparql < FedoraSaxDocument

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
  # mulgara returns an XML document provinding the object, title
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
  #       <title>A Page from Woody Guthrie's Diary:  New Years Resolutions</title>
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
      #####
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
end


# ManifestSaxDocument parses out these kinds of XML files (no schema yet)
#
# <manifest xmlns="info:/flvc/manifest/v1">
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
# </manifest>
#

### TODO: handle object history

class ManifestSaxDocument < FedoraSaxDocument

  @@debug = false

  def self.debug= value
    @@debug = value
  end

  # These need to get set by a configuration file.

  ### @@institutions = [ 'FAMU', 'FAU', 'FIU', 'FIU', 'FLVC', 'FSU', 'NCF', 'UCF', 'UF', 'UNF', 'UWF' ]
  ### @@content_models = [ 'islandora:sp_basic_image', 'islandora:sp_large_image_cmodel', 'islandora:sp_pdf' ]

  attr_reader :collections, :content_model, :identifiers, :object_history, :other_logos, :label, :content_model,
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

    @elements = {}   # lists

    [ 'collection', 'contentModel', 'identifier', 'label', 'objectHistory', 'otherLogo', 'owningInstitution', 'owningUser', 'submittingInstitution' ].each do |name|
      @elements[name] = []
    end

    @valid = true

    @collections = []
    @identifiers = []
    @other_logos = []

    @label = nil
    @content_model = nil
    @owning_institution = nil
    @submitting_institution = nil
    @owning_user = nil

    super()
  end

  private

  # | Manifest Element      | Required | Repeatable | Allowed Character Data                                  | Notes                                               |
  # |-----------------------+----------+------------+---------------------------------------------------------+-----------------------------------------------------|
  # | collection            | yes      | yes        | existing Islandora collection object id                 | will create collection on the fly for digitool      |
  # | contentModel          | yes      | no         | islandora:{sp_pdf,sp_basic_image,sp_large_image_cmodel} |                                                     |
  # | identifier            | no       | yes        | no embedded spaces?                                     | additional identifiers to be saved                  |
  # | label                 | no       | no         | any UTF-8 string?                                       | used when displaying the object or object thumbnail |
  # | otherLogo             | no       | yes        | existing drupal code                                    | determines logo for multibranding                   |
  # | owningInstitution     | yes      | no         | FLVC, UF, FIU, FSU, FAMU, UNF, UWF, FIU, FAU, NCF, UCF  |                                                     |
  # | owningUser            | yes      | no         | valid drupal user                                       | should have submitter role across owningInstitution |
  # | submittingInstitution | no       | no         | FLVC, UF, FIU, FSU, FAMU, UNF, UWF, FIU, FAU, NCF, UCF  | defaults to owningInstitution                       |

  # Textualize the attribute data off a stack element (ignore the :name key)

  def pretty_print hash
    text = []
    hash.keys.each do |k|
      next if k == :name  # element name
      text.push k + ' => ' + hash[k].inspect
    end
    return '{ ' + text.sort.join(', ') + ' }'
  end

  def stack_dump
    @stack.map { |h| h[:name] }.join(' => ') + '  ' + pretty_print(@stack[-1])
  end

  # We'll maintain a stack of elements and their attributes: each
  # element of the stack is a hash, with the name of the element keyed
  # by symbol :name; all the other key/value pairs, all strings, are
  # the attributes.

  def start_element_namespace name, attributes = [], prefix = nil, uri = nil, ns = []
    debug_data = { :name => name }
    debug_data['prefix'] = prefix if prefix
    debug_data['uri']    = uri if uri
    debug_data['ns']     = ns unless ns.empty?

    hash = {}
    attributes.each do |at|
      hash[at.localname] = at.value;
      debug_data[at.localname] = at.value
    end

    @elements['objectHistory'].push hash if name == 'objectHistory'

    @stack.push debug_data
    puts stack_dump if @@debug

  end

  def end_element_namespace name, prefix = nil, uri = nil
    case name

    when 'collection', 'contentModel', 'identifier', 'label', 'otherLogo', 'owningInstitution', 'owningUser', 'submittingInstitution'
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
      @errors.push "The manifest document does not contain a collection ID. At least one is required."
      return
    end

    @collections = @elements['collection']
  end

  # Check that exactly one of the allowed content models is present.

  def content_model_ok?

    if @elements['contentModel'].empty?
      @errors.push "The manifest document does not contain a content model. Exactly one of #{@@content_models.join(', ')} is required."
      return
    end

    if @elements['contentModel'].length > 1
      @errors.push "The manifest document contains multiple content models.  Exactly one of #{@@content_models.join(', ')} is required."
      return
    end

    content_model = @elements['contentModel'].shift

    unless @@content_models.include? content_model
      @errors.push "The manifest document contains an unsupported content model, #{content_model}. Exactly one of #{@@content_models.join(', ')} is required."
      return
    end

    @content_model = content_model
  end


  # Check for optional label, if present there may be only one.

  def label_ok?

    return true if @elements['label'].empty?

    if @elements['label'].length > 1
      @errors.push "The manifest document lists more than one label - at most one can be specfied."
      return
    end

    @label = @elements['label'].shift
  end

  # Check for required owningUser, there must be exactly one (relaxed requirement for now)

  def owning_user_ok?

    return true if @elements['owningUser'].empty?

    if @elements['owningUser'].length > 1
      @errors.push "The manifest document lists multiple owning users - at most, one must be present."
      return
    end

    @owning_user = @elements['owningUser'].shift   # might be nil

    return true
  end


  # check for the required owningInstitution, it must be one of the
  # allowed INSTITUTIONS

  def owning_institution_ok?

    if @elements['owningInstitution'].empty?
      @errors.push "The manifest document does not list an owning institution - it must have exacly one of #{@@institutions.join(', ')}."
      return
    end

    if @elements['owningInstitution'].length > 1
      @errors.push "The manifest document lists multitple owning institutions - it must have exacly one of #{@@institutions.join(', ')}."
      return
    end

    owning_institution = @elements['owningInstitution'].shift.upcase

    unless @@institutions.include? owning_institution
      @errors.push "The manifest document includes an invalid owning institution '#{owning_institution}' - it must have exacly one of #{@@institutions.join(', ')}."
      return
    end

    @owning_institution = owning_institution
  end

  # Check for an optional submittingInstitution - if present it must be one of the @@institutions

  def submitting_institution_ok?
    if @elements['submittingInstitution'].length > 1
      @errors.push "The manifest document lists multitple submitting institutions - it must have at most one of #{@@institutions.join(', ')}."
      return
    end

    if @elements['submittingInstitution'].length == 1
      submitting_institution = @elements['submittingInstitution'].shift.upcase

      unless @@institutions.include? submitting_institution
        @errors.push "The manifest document includes an invalid submitting institution '#{submitting_institution}' - if present, it must be one of #{@@institutions.join(', ')}."
        return
      end

      @submitting_institution = submitting_institution
    end

    return true
  end


  # optional, otherwise a list of hashes wchih must have 'source' and 'data', both non-empty strings

  def object_history_ok?
    return true if @elements['objectHistory'].empty?

    list = []
    @elements['objectHistory'].each do |hash|
      if hash['source'].nil? or hash['source'].empty?
        @errors.push "The manifest has an object history element that is missing the 'source' attribute."
        return
      end
      if hash['data'].nil? or hash['data'].empty?
        @errors.push "The manifest has an object history element that is missing data."
        return
      end
      list.push hash
    end
    @object_history = list
    return true
  end



  # TODO: make sure that objectHistory is a list of hashes that has both "source" and non-empty "data"

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

    @valid &&=  true   # if not false, force to 'true' value


    @warnings.push "There were unexpected elements in the manifest:  #{@bogons.keys.sort.join(', ')}."  unless @bogons.empty?
  end
end
