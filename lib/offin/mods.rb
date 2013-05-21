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
#    @schema_directory must exist and have the expected complement of schemas


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

  MANIFEST_NAMESPACE = 'info:/flvc/manifest/v1'

  include Errors

  attr_reader :xml_document  # won't need external access to prefix after testing

  # TODO: sanity check config

  def initialize config, path

    @filename  = path
    @config    = config
    @valid     = false
    @prefix    = nil         # the SAX parser supplies what prefix corresponds to the MODS namespace within the document


    # TODO: check config file for http_proxy, have nokogiri use it;  e.g.  ENV['http_proxy'] = 'http://localhost:3128/' ??

    @text = File.read(@filename)

    if @text.empty?
      error "MODS file '#{short_filename}' is empty."
      return
    end

    @xml_document = Nokogiri::XML(@text)

    if not @xml_document.errors.empty?
      error "Error parsing MODS file '#{short_filename}':"
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
    return unless valid?

    # We create a new XML Document to avoid the seqfault that re-using the existing @xml_document sometime causes.

    newdoc = Nokogiri::XML(@xml_document.to_xml)
    mods_to_dc = File.read(@config.mods_to_dc_transform_filename)
    xslt = Nokogiri::XSLT(mods_to_dc)
    output =  xslt.transform(newdoc)

    if not output.errors.empty?
      error "When transforming the MODS document '#{short_filename}' to DC with stylesheet '#{@config.mods_to_dc_transform_filename}', the following errors occured:"
      error output.errors
      return
    end

    return output  # a Nokogiri::XML::Document

  rescue => e
    error "Exception '#{e}' occured transforming the MODS document '#{short_filename}' to DC with stylesheet '#{@config.mods_to_dc_transform_filename}', backtrace follows"
    error e.backtrace
    return nil
  end

  def to_s
    @xml_document.to_xml
  end

  def valid?   # we'll have warnings and errors if not
    @valid and not errors?
  end

  def add_islandora_identifier str

    ident = Nokogiri::XML::Node.new("#{format_prefix}identifier", @xml_document)
    ident.content = str                                               # TODO: XML escape
    ident['type'] = 'fedora'

    @xml_document.children[0].add_child(ident)
    ident.before "\n"
    ident.after "\n"

  rescue => e
    error "Can't add islandora identifier '#{str}' to MODS document '#{short_filename}', error #{e.class} - #{e.message}."
  end

  # TODO: this assumes no extension elements present. We'll need it
  # smarter, adding to an existing extension in the mods document if
  # necessary.

  # From the manifest, add owningInstitution, submittingInstitution
  # (defaults to owningInstitution) and optionally one or more
  # objectHistory elements.

  def add_extension_elements manifest

    extension = Nokogiri::XML::Node.new("#{format_prefix}extension", @xml_document)
    extension.add_namespace("man", MANIFEST_NAMESPACE)

    extension << "\n  "
    extension << "<man:owningInstitution>#{manifest.owning_institution}</man:owningInstitution>"
    extension << "\n  "
    extension << "<man:submittingInstitution>#{manifest.submitting_institution || manifest.owning_institution}</man:submittingInstitution>"

    manifest.object_history.each do |record|
      extension << "\n  "
      extension << "<man:objectHistory source=\"#{record['source']}\">#{record['data']}</man:objectHistory>"
    end
    extension << "\n"

    @xml_document.children[0].add_child(extension)
    extension.after "\n"

  rescue => e
    error "Can't add extension elements to MODS document '#{short_filename}', error #{e.class} - #{e.message}."
  end

  private

  # for error messages, give the rightmost directory name along with the filename

  def short_filename
    return $1 if @filename =~ %r{.*/(.*/[^/]+)$}
    return @filename
  end

  # @prefix is the XML element prefix used for the MODS namespace; if
  # @prefix is nil, then MODS is the default namespace here and we
  # don't need prefix:element.

  def format_prefix
    @prefix.nil? ? '' : "#{prefix}:"
  end

  def validates_against_schema?

    sax_document = SaxDocumentExamineMods.new
    Nokogiri::XML::SAX::Parser.new(sax_document).parse(@text)

    @prefix = sax_document.prefix

    if sax_document.warnings? or sax_document.errors?
      warning "SAX parser warnings for '#{short_filename}':"
      warning  sax_document.errors
      warning  sax_document.warnings
    end

    if sax_document.mods_schema_location.nil?
      error "Can't find the MODS schema location in the MODS file '#{short_filename}'"
      return false
    end

    xsd = Nokogiri::XML::Schema(get_schema(sax_document.mods_schema_location))

    issues = []
    xsd.validate(@xml_document).each { |err| issues.push err }

    if not issues.empty?
      error "MODS file '#{short_filename}' had validation errors as follows:"
      error issues
      return false
    end

    return true

    # TODO: catch nokogiri class errors here, others get backtrace
  rescue => e
    error "Exception #{e.class}, #{e.message} occurred when validating '#{short_filename}' against the MODS schema '#{sax_document.mods_schema_location}'."
    ## error e.backtrace
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
      raise "No schema location could be determined for the MODS file '#{short_filename}'"
    else
      raise "There was an unexpected/unsupported schema location '#{location}' declared in the MODS file '#{short_filename}'"
    end
  end

end
