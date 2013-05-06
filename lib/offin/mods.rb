require 'nokogiri'
require 'offin/document-parsers'
require 'offin/errors'

# If @config is reasonable and the server properly provisioned
# (e.g. enough disk space) no error will be raised by this class;
# check Mods#valid? or Mods#error? (and see Mods#errors) to determine
# if there are failures.


# TODO: do a sanity check on @config. Failure should throw an error that will stop all processing at the top level:
#
#    @config.mods_to_dc_transform_filename must exist and be readable
#    @mods_schema_directory must exist and have the expected complement of schema versions


class Mods

  # This class encapsulates what we at FLVC want to do a MODS
  # document:
  #
  # *) Make sure it's a valid MODS document, first and foremost.
  # *) transform to DC
  # *) get title (label)
  # ---- NEEDS ----
  # *) insert extension elements (see manifest.rb for what could go in there)
  # ---- TODO for non-digitool ----
  # *) insert new title
  # *) update extension elements
  # *) get extension elements


  include Errors

  attr_reader :xml_document


  def initialize config, path

    @filename  = path
    @config    = config
    @valid     = false

    # TODO: check config file for http_proxy, have nokogiri use it;  e.g.  ENV['http_proxy'] = 'http://localhost:3128/' ??

    @text = File.read(@filename)

    if @text.empty?
      error "MODS file '#{@filename}' is empty."
      return
    end

    @xml_document = Nokogiri::XML(@text)

    if not @xml_document.errors.empty?
      error "Error parsing MODS file '#{@filename}':"
      error @xml_document.errors
      return
    end

    return unless validates_against_schema?

    @valid = true
  end

  def title
    xslt = Nokogiri::XSLT(File.read(@config.mods_to_title_transform_filename))
    text = xslt.transform(@xml_document).to_s
    titles = text.split(/\n/).select { |t| not t =~ /<?xml/ }
    str = titles[0]
    return nil if str.nil? or str.empty?
    return str.strip.gsub(/\s\s+/, ' ')
  end

  # Return DC derivation for this document as text (or, if errors, nil)

  def to_dc
    x = to_dc_xml
    return x.to_s unless x.nil?
    return
  end

  # Return DC derivation for this document as an XML document (or, if errors, nil)

  def to_dc_xml

    return unless @valid

    mods_to_dc = File.read(@config.mods_to_dc_transform_filename)
    xslt = Nokogiri::XSLT(mods_to_dc)
    output =  xslt.transform(@xml_document)

    if not output.errors.empty?
      error "When transforming the MODS document '#{@filename}' to DC with stylesheet '#{@config.mods_to_dc_transform_filename}', the following errors occured:"
      error *output.errors
      return
    end

    return output  # a Nokogiri::XML::Document

  rescue => e
    error "Exception '#{e}' occured transforming the MODS document '#{@filename}' to DC with stylesheet '#{@config.mods_to_dc_transform_filename}', backtrace follows"
    error e.backtrace
    return nil
  end

  def to_s
    @text
  end

  def valid?   # we'll have warnings and errors if not
    @valid
  end

  private

  def validates_against_schema?

    sax_document = SaxDocumentExamineMods.new
    Nokogiri::XML::SAX::Parser.new(sax_document).parse(@text)

    if sax_document.warnings? or sax_document.errors?
      warning "SAX parsing warnings for '#{@filename}'"
      warning  sax_document.errors
      warning  sax_document.warnings
    end

    if sax_document.mods_schema_location.nil?
      error "Can't find the MODS schema location in the MODS file '#{@filename}'"
      return false
    end

    xsd = Nokogiri::XML::Schema(get_schema(sax_document.mods_schema_location))

    issues = []
    xsd.validate(@xml_document).each { |err| issues.push err }

    if not issues.empty?
      error "MODS file '#{@filename}' had validation errors as follows:"
      error *issues
      return false
    end

    return true

  rescue => e
    error "Exception '#{e}' occurred when validating '#{@filename}' against MODS schema, backtrace follows:"
    error e.backtrace
    return false
  end

  # We've got some MODS schemas which we identify by location; we keep 'em handy..

  def get_schema location

    # we'll have to add new versions here every now and then...

    case location.downcase

    when 'http://www.loc.gov/standards/mods/v3/mods-3-0.xsd',
         'http://www.loc.gov/standards/mods/v3/mods-3-1.xsd',
         'http://www.loc.gov/standards/mods/v3/mods-3-2.xsd',
         'http://www.loc.gov/standards/mods/v3/mods-3-3.xsd',
         'http://www.loc.gov/standards/mods/v3/mods-3-4.xsd'
      return File.open(File.join(@config.schema_directory, File.basename(location)))
    end

    if location.nil?
      raise "No schema location could be determined for the MODS file '#{@filename}'"
    else
      raise "There was an unexpected/unsupported schema location '#{location}' declared in the MODS file '#{@filename}'"
    end
  end

end
