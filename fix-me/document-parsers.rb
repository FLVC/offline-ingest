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

    @current_string = ''
  end
end
