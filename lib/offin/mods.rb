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
  # *) get PURLs, IIDs
  # *) insert extension elements (see manifest.rb for what could go in there)
  # *) run cleanup xslt
  # ... more being added

  MANIFEST_NAMESPACE = 'info:flvc/manifest/v1'
  MODS_NAMESPACE = 'http://www.loc.gov/mods/v3'


  include Errors

  attr_reader :xml_document  # won't need external access to prefix after testing

  # TODO: sanity check config

  def initialize config, path

    @filename  = path
    @config    = config
    @valid     = false
    @prefix    = nil         # the SAX parser supplies what prefix corresponds to the MODS namespace within the document

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


  # Caitlin Nelson developed an XSLT transform that cleans up MODS
  # files, and constructs and inserts a PURL if one doesn't already
  # exist.  It was originally developed for the islandora GUI, but we
  # use it here.
  #
  # Note that this method causes a complete re-parse of the MODS file
  # as a side effect.

  def post_process_cleanup
    newdoc = Nokogiri::XML(@xml_document.to_xml)
    mods_cleanup = File.read(@config.mods_post_processing_filename)
    xslt = Nokogiri::XSLT(mods_cleanup)
    output =  xslt.transform(newdoc)

    if not output.errors.empty?
      error "During post-process cleanup of the MODS document '#{short_filename}' with '#{@config.mods_post_processing_filename}', the following errors occured:"
      error output.errors
      return
    end

    @text = output.to_s

    @xml_document = Nokogiri::XML(@text)

    if not @xml_document.errors.empty?
      error "Error parsing MODS file '#{short_filename}' after application of post-processing with '#{@config.mods_post_processing_filename}':"
      error @xml_document.errors
      return
    end

    @valid = validates_against_schema?

  rescue => e
    error "Exception '#{e}' occured during post-process cleanup of the MODS document '#{short_filename}' with '#{@config.mods_post_processing_filename}':"
    error e.backtrace
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

  def purls
    return @xml_document.xpath("//mods:location[translate(@displayLabel, 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')=\"PURL\"]/mods:url", 'mods' => MODS_NAMESPACE).children.map { |xt| xt.to_s }
  rescue => e
    return []
  end

  # There really should only be one:

  def iids
    return @xml_document.xpath("//mods:identifier[translate(@type, 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')=\"IID\"]", 'mods' => MODS_NAMESPACE).children.map { |xt| xt.to_s }
  rescue => e
    return []
  end

  # There really should only be one:

  def digitool_ids
    return @xml_document.xpath("//mods:identifier[translate(@type, 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')=\"DIGITOOL\"]", 'mods' => MODS_NAMESPACE).children.map { |xt| xt.to_s.strip }.uniq
  rescue => e
    return []
  end


  # There may be many typeOfResource elements, and they may, strictly speaking, have a
  # mix of character data and attributes.  We are really only interested in
  # the character data, particularly 'text' or 'still image'.  We downcase the text.

  def type_of_resource
    return @xml_document.xpath('//mods:typeOfResource', 'mods' => MODS_NAMESPACE).map { |node| node.text.strip.downcase }.select { |text| not text.empty? }
  rescue => e
    return []
  end


  def add_type_of_resource str
    tor = Nokogiri::XML::Node.new("#{format_prefix}typeOfResource", @xml_document)
    tor.content = str
    @xml_document.root.add_child(tor)
    tor.after "\n"
  rescue => e
    error "Can't add typeOfResource '#{str}' to MODS document '#{short_filename}', error #{e.class} - #{e.message}."
  end


  def add_islandora_identifier str
    ident = Nokogiri::XML::Node.new("#{format_prefix}identifier", @xml_document)
    ident.content = str
    ident['type'] = 'fedora'

    @xml_document.root.add_child(ident)
    ident.after "\n"

  rescue => e
    error "Can't add islandora identifier '#{str}' to MODS document '#{short_filename}', error #{e.class} - #{e.message}."
  end


  def add_iid_identifier str
    ident = Nokogiri::XML::Node.new("#{format_prefix}identifier", @xml_document)
    ident.content = str
    ident['type'] = 'IID'
    @xml_document.root.add_child(ident)
    ident.after "\n"

  rescue => e
    error "Can't add islandora identifier '#{str}' to MODS document '#{short_filename}', error #{e.class} - #{e.message}."
  end


  # For our extension data, we need MODS to be of the form
  #
  #  <?xml version="1.0" encoding="UTF-8"?>
  #   <mods xmlns="http://www.loc.gov/mods/v3" xmlns:flvc="info:flvc/manifest/v1" ...
  #   ....
  #   <extension>
  #     <flvc:flvc>
  #       <flvc:owningInstitution>UF</flvc:owningInstitution>
  #       ....
  #     </flvc:flvc>
  #   </extension>
  #
  # What get_prefix_for_flvc_extension does is to check the root
  # element namespace, looking for what prefix is being used for the
  # "info:flvc/manifest/v1" namespace, and returning it ("flvc" in
  # the above example).  If the "info:flvc/manifest/v1" namespace is
  # not present, it is added to the document root with the prefix
  # "flvc" and that string is returned.

  def get_prefix_for_flvc_extension
    @xml_document.namespaces.each do |prefix, namespace|
      return prefix.sub(/^xmlns:/, '') if namespace == MANIFEST_NAMESPACE
    end

    @xml_document.root.add_namespace('flvc', MANIFEST_NAMESPACE)
    return 'flvc'
  end


  # From the manifest, add owningInstitution, submittingInstitution
  # (defaults to owningInstitution) and optionally one or more
  # objectHistory elements.

  def add_flvc_extension_elements manifest

    flvc_prefix = get_prefix_for_flvc_extension

    if flvc_extensions?
        mods_flvc_extension = @xml_document.xpath("//mods:extension/flvc:flvc", 'mods' => MODS_NAMESPACE, 'flvc' => MANIFEST_NAMESPACE)
        mods_flvc_extension.remove()
    end

    str = <<-XML.gsub(/^        /, '')
        <#{format_prefix}extension>
          <#{flvc_prefix}:flvc>
             <#{flvc_prefix}:owningInstitution>#{manifest.owning_institution}</#{flvc_prefix}:owningInstitution>
             <#{flvc_prefix}:submittingInstitution>#{manifest.submitting_institution || manifest.owning_institution}</#{flvc_prefix}:submittingInstitution>
    XML

    manifest.other_logos.each do |other_logo|
       str += "     <#{flvc_prefix}:otherLogo>#{other_logo}</#{flvc_prefix}:otherLogo>\n"
    end

    manifest.object_history.each do |record|
      str += "     <#{flvc_prefix}:objectHistory source=\"#{record['source']}\">#{record['data']}</#{flvc_prefix}:objectHistory>\n"
    end

    str += <<-XML.gsub(/^        /, '')
          </#{flvc_prefix}:flvc>
        </#{format_prefix}extension>
    XML

    @xml_document.root.add_child(str)

  rescue => e
    error "Can't add extension elements to MODS document '#{short_filename}', error #{e.class} - #{e.message}."
  end


  def flvc_extensions?
    not @xml_document.xpath("//mods:extension/flvc:flvc", 'mods' => MODS_NAMESPACE, 'flvc' => MANIFEST_NAMESPACE).empty?
  rescue =>e
    return false
  end


  def flvc_extensions
    return @xml_document.xpath("//mods:extension/flvc:flvc", 'mods' => MODS_NAMESPACE, 'flvc' => MANIFEST_NAMESPACE).children.map { |xt| xt.to_s.strip }.select { |str| not str.empty? }
  end


  # @prefix is the XML element prefix used for the MODS namespace; if
  # @prefix is nil, then MODS is the default namespace here and we
  # don't need prefix:element.

  def format_prefix
    #@prefix.nil? ? '' : "#{@prefix}:"
    # don't want prefix:element
    @prefix.nil? ? '' : ''
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


  private

  # for error messages, give the rightmost directory name along with the filename

  def short_filename
    return $1 if @filename =~ %r{.*/(.*/[^/]+)$}
    return @filename
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
