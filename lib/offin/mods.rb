require 'nokogiri'
require 'offin/document-parsers'
class Mods

# TODO: forget about our own caching; use fetch from net, check for squid proxy, yada yada


INCLUDE_DIR = File.expand_path(File.join(File.dirname(__FILE__), '../include'))


  # This class encapsulates what we at FLVC want to do a MODS
  # document:
  #
  # *) Make sure it's a valid MODS document, first and foremost.
  #
  # *) A MODS file has metadata about a digital object to be
  # ingested, along the lines of DC.  Our affiliates often have MODS
  # files associated with their digital objects as a matter of
  # course. Our DigiTool product exports MODS files. We need to
  # produce a Dublin Core transform of this file.
  #
  # In the limit we'll potentially have MODS extracted from METS
  # files, but we're not attempting that currently.
  #
  # Currently, we only support getting a MODS file from a filename:

  attr_reader :warnings, :errors

  def initialize config, path   # probablty should not be a path here,  or at least include the package identifier for error messages, or we should limit reporting the file name to the package/filename

    # check if pathname valid, readable...
    # check that MODS_TO_DC_XSL is available - this must produce a full stop for any multi-packate processing loop
    # check that http proxy exists

    @warnings  = []
    @errors    = []       # errors imply that this document is unusable for all practical purposes.
    @filename  = path
    @validated = nil


    # TODO: check config file for http_proxy, and set it here (checking that we can connect...). Possible to set env at this point?

    #  ENV['http_proxy'] = 'http://localhost:3128/'

    text = File.read(@filename)

    @mods_to_dc   = File.read(config.mods_to_dc_transform_filename)
    @xml_document = Nokogiri::XML(text)
    @sax_document = SaxDocumentExamineMods.new
    Nokogiri::XML::SAX::Parser.new(@sax_document).parse(text)

    warning *@sax_document.errors               # any sax processing errors are mere warnings for us.
    warning *@sax_document.warnings
  end

  def warning *stuff
    @warnings.push *stuff
  end

  def error *stuff
    @errors.push *stuff
  end

  def errors?
    not @errors.empty?
  end

  def warnings?
    not @warnings.empty?
  end


  # This next assumes a well-formed top-level mods file

  def mods_document?
    return @sax_document.is_simple_mods?
  rescue => e
    error "When examining '#{@filename}' as a MODS document, this exception occurred: '#{e.message}'"
    return false
  end

  # Validate this MODS document

  def mods_validates?

    return @validated unless @validated.nil?

    xsd = Nokogiri::XML::Schema(get_schema(@sax_document.mods_schema_location))   #### TODO: work out what to do if mods_schema is nil

    validation_errors = []
    @validated = true
    xsd.validate(@xml_document).each do |err|
      validation_errors.push err
      @validated = false
    end

    if not validation_errors.empty?
      warning "Validation errors occurred for the MODS file '#{@filename}' when using the schema file '#{@sax_document.mods_schema_location}':"
      warning *validation_errors
    end

    return @validated

  rescue => e
    error "When validating '#{@filename}' as a MODS document, this exception occurred: '#{e.message}'"
    return false
  end

  # Return DC derivation from this document

  def to_dc
    xslt = Nokogiri::XSLT(@mods_to_dc)
    return xslt.transform(@xml_document)
  rescue => e
    error "When attempting to transform the MODS document '#{@filename}' to DC with stylesheet '#{MODS_TO_DC_XSL}', this exception occurred: '#{e.message}'"
    return nil
  end

  private

  # We've got some MODS schemas which we identify by location; we keep 'em handy
  # TODO: fetch and configure....

  def get_schema location

    case location
    when 'http://www.loc.gov/standards/mods/v3/mods-3-0.xsd'
      return File.open(File.join(INCLUDE_DIR, 'mods-3-0.xsd'))

    when 'http://www.loc.gov/standards/mods/v3/mods-3-1.xsd'
      return File.open(File.join(INCLUDE_DIR, 'mods-3-1.xsd'))

    when 'http://www.loc.gov/standards/mods/v3/mods-3-2.xsd'
      return File.open(File.join(INCLUDE_DIR, 'mods-3-2.xsd'))

    when 'http://www.loc.gov/standards/mods/v3/mods-3-3.xsd'
      return File.open(File.join(INCLUDE_DIR, 'mods-3-3.xsd'))

    when 'http://www.loc.gov/standards/mods/v3/mods-3-4.xsd'
      return File.open(File.join(INCLUDE_DIR, 'mods-3-4.xsd'))
    end

    if location.nil?
      raise "No location could be determined for the MODS schema in '#{@filename}'"
    else
      raise "There was an unexpected location '#{location}' specified for the MODS schema file in '#{@filename}'"
    end

    # TODO: go out and attempt to fetch it otherwise...via squid proxy
  end

end
