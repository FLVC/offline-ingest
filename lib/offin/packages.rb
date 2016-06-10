# The main class for ingesting a directory of files and metadata.

require 'nokogiri'
require 'offin/utils'
require 'offin/manifest'
require 'offin/exceptions'
require 'offin/errors'
require 'offin/mets'
require 'offin/mods'
require 'offin/ingestor'
require 'offin/metadata-checkers'
require 'offin/config'
require 'offin/drupal-db'

BASIC_IMAGE_CONTENT_MODEL      =  'islandora:sp_basic_image'
LARGE_IMAGE_CONTENT_MODEL      =  'islandora:sp_large_image_cmodel'
PDF_CONTENT_MODEL              =  'islandora:sp_pdf'
BOOK_CONTENT_MODEL             =  'islandora:bookCModel'
PAGE_CONTENT_MODEL             =  'islandora:pageCModel'
NEWSPAPER_CONTENT_MODEL        =  'islandora:newspaperCModel'
NEWSPAPER_ISSUE_CONTENT_MODEL  =  'islandora:newspaperIssueCModel'
NEWSPAPER_PAGE_CONTENT_MODEL   =  'islandora:newspaperPageCModel'

#  Class Hierarchy:
#
#                            PackageClass (base class)
#                                  |
#                                  |
#        .-------------------------------------------------------.
#        |                   |                |                  |
#        |                   |                |                  |
#  BasicImagePackage   LargeImagePackage   PDFPackage   StructuredPagePackage (base class)
#                                                                |
#                                                                |
#                                                  .-----------------------------.
#                                                  |                             |
#                                                  |                             |
#                                             BookPackage               NewspaperIssuePackage
#
#
# Generally, all the Package classes have objects they create to
# extract information about the package.  The important ones include:
#
#  * Manifest
#  * Mods
#  * Mets (StructuredPagePackages)
#
# TODO: packages have become a God object. Break this stuff out.
# PackageFactory should instantiate the objects, fail to create a
# Package if any are invalid.



# PackageFactory takes a directory path and checks the manifest.xml
# file within it.  It determines what content model that is being
# requested, and returns the appropriate type of package.

class PackageFactory

  attr_reader :config

  def initialize config, updator_class
    @config = config
    @updator_class = updator_class
  end

  def new_package directory

    raise PackageError, "Package directory '#{directory}' doesn't exist."            unless File.exists? directory
    raise PackageError, "Package directory '#{directory}' isn't really a directory." unless File.directory? directory
    raise PackageError, "Package directory '#{directory}' isn't readable."           unless File.readable? directory

    manifest = Utils.get_manifest @config, directory

    return case manifest.content_model
           when BASIC_IMAGE_CONTENT_MODEL;       BasicImagePackage.new(@config, directory, manifest, @updator_class)
           when LARGE_IMAGE_CONTENT_MODEL;       LargeImagePackage.new(@config, directory, manifest, @updator_class)
           when PDF_CONTENT_MODEL;               PDFPackage.new(@config, directory, manifest, @updator_class)
           when BOOK_CONTENT_MODEL;              BookPackage.new(@config, directory, manifest, @updator_class)
           when NEWSPAPER_ISSUE_CONTENT_MODEL;   NewspaperIssuePackage.new(@config, directory, manifest, @updator_class)
           else
             raise PackageError, "Package directory '#{directory}' specifies an unsupported content model '#{manifest.content_model}'"
           end

  rescue PackageError
    raise

  rescue => e
    raise PackageError, "#{e.class}: #{e.message}"
  end
end


# Package serves as a base class, but it might serve to do a basic
# check on a directory's well-formedness as a package.

class Package

  include Errors

  # supported MIME types go here, using a regexp for the 'file' command's returned values (note very general text type recognition, for which file tends to outsmart itself)

  GIF  = %r{image/gif}
  JP2  = %r{image/jp2}
  PNG  = %r{image/png}
  JPEG = %r{image/jpeg}
  TIFF = %r{image/tiff}
  PDF  = %r{application/pdf}
  TEXT = %r{text/}

  attr_reader :bytes_ingested, :collections, :component_objects, :config, :content_model, :directory_name
  attr_reader :directory_path, :manifest, :marc, :mods, :namespace, :pid, :mods_type_of_resource, :owning_institution

  attr_accessor :iid, :label, :owner

  def initialize config, directory, manifest, updator_class

    @bytes_ingested    = 0
    @iid               = nil
    @component_objects = []    # for objects like books, which have page objects - these are islandora PIDs (strings) for those objects
    @collections       = []
    @pid               = nil
    @config            = config
    @content_model     = nil
    @label             = nil
    @owner             = nil
    @directory_name    = File.basename(directory)
    @directory_path    = directory
    @datafiles         = list_other_files()
    @updator           = updator_class.send :new, self
    @drupal_db         = DrupalDataBase.new(config) unless config.test_mode

    @mods_type_of_resource = nil

    # TODO: for *ALL* *SUBCLASSES* group MANIFEST, MODS, METS, processing
    # so all derived methods and instance variables are reliably set
    # before purely package-level semantics are checked.
    #
    # TODO: create a table of classes vs. instance variables and public methods.

    handle_manifest(manifest) or return     # sets up @manifest
    handle_mods or return                   # sets up @mods
    handle_marc or return                   # sets up @marc
    handle_updator or return                # does system-specific checks, e.g. digtitool, prospective, etc
    handle_drupal_check or return           # check that manifest-specified drupal account exists in drupal database.

    return unless valid?

    @namespace   = @manifest.owning_institution.downcase
    @collections = list_collections(@manifest)
    @owning_institution = @namespace
    @inherited_policy_collection_id = get_inherited_policy_collection_id(@config, @collections, @namespace)

  rescue SystemError
    raise
  rescue PackageError => e
    error "Exception for package #{@directory_name}: #{e.message}"
  rescue => e
    error "Unhandled exception for package #{@directory_name}: #{e.class} - #{e.message}, backtrace follows:", e.backtrace
  end

  # An array (possibly empty) of this package's islandora pid concatenated with any component_objects.

  def pids
    return [] unless @pid
    return ([ @pid ] + @component_objects.clone)
  end

  # Attempt to delete this package (and all its component objects) from islandora.

  def delete_from_islandora
    return if pids.empty?
    repository = Rubydora.connect :url => config.fedora_url, :user => config.user, :password => config.password
    pids.each do |p|
      begin
        p = "info:fedora/#{p}" unless p =~ /^info:fedora/
        Rubydora::DigitalObject.find_or_initialize(p, repository).delete
      rescue RestClient::ResourceNotFound => e
      # don't care
      rescue => e
        warning "Could not delete pid #{p} from package #{name}: #{e.class} #{e.message}"
      end
    end
  end

  def name
    @directory_name
  end

  # base classes should re-implement ingest.
  def ingest
    raise PackageError, 'Attempt to ingest an invalid package.' unless valid?
  end

  def valid?
    not errors?
  end

  # Used by subclassess.  Note that a MetadataUpdater call will
  # have to have first been made to properly set up some of these
  # (label, owner).
  #
  # This is normally not used for component objects (e.g. pages)

  def boilerplate ingestor

    @pid = ingestor.pid

    @mods.add_iid_identifier @iid if @mods.iids.empty?   # we do sanity checking and setup the @iid elsewhere
    @mods.add_islandora_identifier ingestor.pid
    @mods.add_flvc_extension_elements @manifest

    if @mods.type_of_resource.empty?
      @mods.add_type_of_resource @mods_type_of_resource
    end

    @mods.post_process_cleanup   # creates purl if necessary, must be done after iid inserted into MODS
    raise PackageError, "Invalid MODS file" unless @mods.valid?

    ingestor.label         = @label
    ingestor.owner         = @owner
    ingestor.content_model = @content_model
    ingestor.collections   = @collections
    ingestor.dc            = @mods.to_dc
    ingestor.mods          = @mods.to_s

    if @marc
      ingestor.datastream('MARCXML') do |ds|
        ds.dsLabel  = "Archived MarcXML"
        ds.content  = @marc
        ds.mimeType = 'text/xml'
      end
    end

    if @manifest.embargo
      @drupal_db.add_embargo @pid, @manifest.embargo['rangeName'], @manifest.embargo['endDate']
    end

    # set POLICY if there is only one collection with same namespace and POLICY datastream
    # if none or more than one collection, do not set POLICY

    if @inherited_policy_collection_id
      ingestor.datastream('POLICY') do |ds|
        ds.dsLabel  = "XACML Policy Stream"
        ds.content  = Utils.get_datastream_contents(@config, @inherited_policy_collection_id, 'POLICY')
        ds.mimeType = 'text/xml'
        ds.controlGroup = 'X'
      end
    end

    ingestor.datastream('RELS-EXT') do |ds|
      ds.dsLabel  = 'Relationships'
      ds.content  = rels_ext(ingestor.pid)
      ds.mimeType = 'application/rdf+xml'
    end
  end

  # subclasses that call the boilerplate() method should redefine rels_ext as appropriate.

  def rels_ext pid

    str = <<-XML
    <rdf:RDF xmlns:fedora="info:fedora/fedora-system:def/relations-external#"
             xmlns:fedora-model="info:fedora/fedora-system:def/model#"
             xmlns:islandora="http://islandora.ca/ontology/relsext#"
             xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
      <rdf:Description rdf:about="info:fedora/#{pid}">
        <fedora-model:hasModel rdf:resource="info:fedora/#{@content_model}"></fedora-model:hasModel>
    XML
    @collections.each do |collection|
      str += <<-XML
        <fedora:isMemberOfCollection rdf:resource="info:fedora/#{collection}"></fedora:isMemberOfCollection>
    XML
    end

    if @manifest.page_progression
      str += <<-XML
        <islandora:hasPageProgression>#{@manifest.page_progression}</islandora:hasPageProgression>
    XML
    end

    if @inherited_policy_collection_id
      str += Utils.rels_ext_get_policy_fields(@config, @inherited_policy_collection_id)
    end

    str += <<-XML
      </rdf:Description>
    </rdf:RDF>
    XML

    return str.gsub(/^    /, '')
  end

  # This is optional - only DigiTool derived MODS files will have it.

  def digitool_id
    return nil if @mods.nil?
    return @mods.digitool_ids.first
  end

  def purls
    return [] if @mods.nil?
    return @mods.purls
  end


  # If this object belongs to collections such that exactly one of them
  # has a POLICY datastream, return the collection id, otherwise nil.

  def get_inherited_policy_collection_id config, collection_list, my_namespace
    parent_policy_ids = []
    collection_list.each do |collection_id|
      collection_namespace = collection_id.partition(':')[0]
      if collection_namespace == my_namespace and Utils.get_datastream_names(config, collection_id).has_key?('POLICY')
        parent_policy_ids.push collection_id
      end
    end
    return (parent_policy_ids.count == 1 ? parent_policy_ids.pop : nil)
  end


  # Self explanatory

  def safe_close *files
    files.each { |file| file.close if file.respond_to? :close and file.respond_to? :closed? and not file.closed? }
  rescue
  end

  # Get a list of all collections this package should be a member of; will check the config file for a list of remappings.
  # TODO: more docs, example fragments of of yaml

  def list_collections manifest
    remapper = @config.remap_collections || {}
    list = []

    manifest.collections.each do |pid|
      pid.downcase!

      case remapper[pid]
      when NilClass
        list.push pid
      when String
        new_pid = remapper[pid].downcase
        list.push new_pid
      # warning "Note: the manifest.xml file specifies collection #{pid}; the configuration file is remapping it to collection #{new_pid}"
      when Array
        new_pids = remapper[pid].map { |p| p.downcase }
        list += new_pids
        # warning "Note: the manifest.xml file specifies collection #{pid}; the configuration file is remapping it to collections #{new_pids.join(', ')}"
      end
    end

    return list
  end

  # List all the files in the directory we haven't already accounted for. Subclasses will need to work through these.
  # Presumably these are all datafiles.

  def list_other_files

    list = []

    Dir["#{@directory_path}/*"].each do |entry|

      raise PackageError, "Found subdirectory '#{entry}' in package" if File.directory?(entry)
      raise PackageError, "Found unreadable file '#{entry}' in package" unless File.readable?(entry)

      filename = File.basename entry
      next if [ '.', '..', 'manifest.xml', 'marc.xml', 'mets.xml', "#{directory_name}.xml" ].include? filename
      list.push filename
    end

    return list
  end


  # set up @manifest

  def handle_manifest manifest

    if manifest.is_a? Manifest
      @manifest = manifest
    else
      @manifest = Utils.get_manifest @config, @directory_path
    end

    if @manifest.errors? or not @manifest.valid?
      error "The package #{@directory_name} doesn't have a valid manifest file."
      error @manifest.errors
    end

    if @manifest.embargo
      if @config.test_mode
        warning "Can't check the drupal database for valid embargo rangeNames in test mode."
      elsif not @drupal_db.check_range_name(@manifest.embargo['rangeName'])
        error "The manifest has a undefined embargo rangeName \"#{@manifest.embargo['rangeName']}\" - valid embargo rangeNames (case insensitive) are \"#{DrupalDataBase.list_ranges.keys.sort.join('", "')}\"."
      end
    end

    return valid?
  end


  def handle_marc
    marc_file = File.join(@directory_path, 'marc.xml')
    @marc = File.read(marc_file) if File.exists?(marc_file)
    return true
  end


  def handle_mods
    @mods = Utils.get_mods @config, @directory_path

    if not @mods.valid?
      error "The package #{@directory_name} doesn't have a valid MODS file."
      error @mods.errors
      return false
    end

    # Because we get a segv fault when trying to add an IID to MODS at this point, we defer inserting it into the MODS XML until later

    iids = @mods.iids

    if iids.length == 1  and iids.first != @directory_name
      error "The MODS file in package #{@directory_name} declares an IID of #{iids.first} which doesn't match the package name."
      return
    elsif iids.length > 1
      error "The MODS file in package #{@directory_name} declares too many IIDs: #{iids.join(', ')}: only one is allowed."
      return
    elsif iids.length == 1
      @iid = iids.first
      if @iid =~ /[^A-Za-z0-9_\.()-]/
        error "The MODS file in package #{@directory_name} declares the IID as '#{@iid}',  which has illegal characters (only 'A'-'Z', 'a'-'z', '0'-'9', ')', '(', '-', '_' and '.'  are allowed)."
        return
      end
    elsif iids.nil? or iids.length == 0
      # warning "MODS file doesn't include an IID, using the package name #{@directory_name}."
      @iid = @directory_name
    end

    if pid = Utils.get_pre_existing_islandora_pid_for_iid(@config, @iid)
      error "The IID for this package, #{@iid}, is alreading being used for islandora object #{pid}. The IID must be unique."
    end

    return valid?
  end

  # A MetdataChecker mediates specialized metadata checks, for instance, rules
  # for digitool migrations vs. prospective ingests.
  #
  # @updator.post_initialization will at least supply a package label,
  # supply the islandora owner, and perhaps run specialized checks. It
  # should be run after all mods, manifest, and marc processing.
  #
  # There is an @updator.post_ingest hook as well.

  def handle_updator
    @updator.post_initialization
    return valid?
  end

  def handle_drupal_check
    return valid? if @config.test_mode

    users = @drupal_db.users
    if not users.include? @owner
      error "The digital object owner, '#{@owner}', is not one of the valid drupal users: '#{users.join("', '")}'"
    end
    return valid?
  end

  # utility to return a nicely formated string of the (sub)class name for error and warning messages

  def pretty_class_name
    return self.class.to_s.split(/(?=[A-Z])/).join(' ')
  end

end # of Package base class



# Subclass of Package for handling basic image content model

class BasicImagePackage < Package

  # At this point we know we have a manifest, mods and maybe a marc file.

  def initialize config, directory, manifest, updator
    super(config, directory, manifest, updator)

    @content_model = BASIC_IMAGE_CONTENT_MODEL
    @mods_type_of_resource = 'still image'

    if @datafiles.length > 1
      error "The #{pretty_class_name} #{@directory_name} contains too many data files (only one expected): #{@datafiles.join(', ')}."
    end

    if @datafiles.length == 0
      error "The #{pretty_class_name}  #{@directory_name} contains no data files."
    end

    return unless valid?

    @image_filename = @datafiles.first
    @image_pathname = File.join(@directory_path, @image_filename)

    @mime_type = Utils.mime_type(@image_pathname)

    case @mime_type
    when GIF, JPEG, PNG
      @image = File.open(@image_pathname, 'rb')

    # TODO: add special support for TIFFs (not needed for digitool migration)

    when TIFF
      raise PackageError, "The #{pretty_class_name} #{@directory_name} contains the TIFF file #{@datafiles.first}, which is currently unsupported (coming soon)."
    else
      raise PackageError, "The #{pretty_class_name} #{@directory_name} contains an unexpected file #{@datafiles.first} with mime type #{type}."
    end

  rescue PackageError => e
    error "Exception for #{pretty_class_name} #{@directory_name}: #{e.message}"
  rescue => e
    error "Exception #{e.class} - #{e.message} for #{pretty_class_name} #{@directory_name}, backtrace follows:", e.backtrace
  end

  def ingest
    medium, thumbnail, medium_error_messages, thumbnail_error_messages = nil

    return if @config.test_mode

    @image.rewind

    ingestor = Ingestor.new(@config, @namespace) do |ingestor|

      boilerplate(ingestor)

      ingestor.datastream('OBJ') do |ds|
        ds.dsLabel  = @image_filename
        ds.content  = @image
        ds.mimeType = @mime_type
      end

      medium, medium_error_messages = Utils.image_resize(@config, @image_pathname, @config.medium_geometry, 'jpeg')

      ingestor.datastream('MEDIUM_SIZE') do |ds|
        ds.dsLabel  = "Medium Size Image"
        ds.content  = medium
        ds.mimeType = 'image/jpeg'
      end

      thumbnail, thumbnail_error_messages = Utils.image_resize(@config, @image_pathname, @config.thumbnail_geometry, 'jpeg')

      ingestor.datastream('TN') do |ds|
        ds.dsLabel  = "Thumbnail Image"
        ds.content  = thumbnail
        ds.mimeType = 'image/jpeg'
      end
    end

    @bytes_ingested = ingestor.size
  ensure
    warning ingestor.warnings if ingestor and ingestor.warnings?

    error   ingestor.errors   if ingestor and ingestor.errors?
    error   [ 'Error creating Thumbnail datastream' ] + thumbnail_error_messages  if thumbnail_error_messages and not thumbnail_error_messages.empty?
    error   [ 'Error creating Medium datastream' ]    + medium_error_messages     if medium_error_messages    and not medium_error_messages.empty?

    safe_close(@image, medium, thumbnail)

    @updator.post_ingest
  end
end


# Subclass of Package for handling large image content model

class LargeImagePackage < Package

  # At this point we know we have a manifest, mods and maybe a marc file.

  attr_reader :image

  def initialize config, directory, manifest, updator
    super(config, directory, manifest, updator)

    @content_model = LARGE_IMAGE_CONTENT_MODEL
    @mods_type_of_resource = 'still image'

    if @datafiles.length > 1
      error "The #{pretty_class_name} #{@directory_name} contains too many data files (only one expected): #{@datafiles.join(', ')}."
    end

    if @datafiles.length == 0
      raise PackageError, "The #{pretty_class_name} #{@directory_name} contains no data files."
    end

    return unless valid?

    @image_filename = @datafiles.first
    @image_pathname = File.join(@directory_path, @image_filename)
    @mime_type = Utils.mime_type(@image_pathname)

    case @mime_type
    when JP2
      @image = File.open(@image_pathname, 'rb')
    when TIFF
      @image = File.open(@image_pathname, 'rb')
    else
      raise PackageError, "The #{pretty_class_name} #{@directory_name} contains an unexpected or unsupported file #{@datafiles.first} with mime type #{type}."
    end

  rescue PackageError => e
    error "Exception for #{pretty_class_name} #{@directory_name}: #{e.message}"
  rescue => e
    error "Exception #{e.class} - #{e.message} for #{pretty_class_name} #{@directory_name}, backtrace follows:", e.backtrace
  end

  def ingest

    return if @config.test_mode

    # We have two cases: a source JP2 or the more generically-supported TIFF.

    # JP2-submitted (implemented)
    #
    #  OBJ    reduced sized TIFF from JP2
    #  JP2    original source
    #  JPG    medium image derived from JP2
    #  TN     thumbnail derived from JP2
    #
    # TIFF-submitted
    #
    #  OBJ    original TIFF
    #  JP2    derived from TIFF
    #  JPG    medium image derived from TIFF
    #  TN     thumbnail derived from TIFF

    medium, thumbnail, medium_error_messages, thumbnail_error_messages = nil

    ingestor = Ingestor.new(@config, @namespace) do |ingestor|

      boilerplate(ingestor)

      case @mime_type

      when TIFF;   ingest_tiff ingestor
      when JP2;    ingest_jp2 ingestor
      else
        raise PackageError, "The #{pretty_class_name} #{@directory_name} contains an unexpected or unsupported file #{@image_filename} with image format #{@image.format}."
      end

      medium, thumbnail, medium_error_messages, thumbnail_error_messages = nil

      medium, medium_error_messages = Utils.image_resize(@config, @image_pathname, @config.medium_geometry, 'jpeg')

      ingestor.datastream('JPG') do |ds|
        ds.dsLabel  = 'Medium sized JPEG'
        ds.content  = medium
        ds.mimeType = 'image/jpeg'
      end

      thumbnail, thumbnail_error_messages = Utils.image_resize(@config, @image_pathname, @config.thumbnail_geometry, 'jpeg')

      ingestor.datastream('TN') do |ds|
        ds.dsLabel  = 'Thumbnail'
        ds.content  = thumbnail
        ds.mimeType = 'image/jpeg'
      end

      @bytes_ingested = ingestor.size
    end

  ensure
    warning ingestor.warnings if ingestor and ingestor.warnings?
    error   ingestor.errors   if ingestor and ingestor.errors?
    warning [ 'Issues creating Thumbnail datastream' ] + thumbnail_error_messages  if thumbnail_error_messages and not thumbnail_error_messages.empty?
    warning [ 'Issues creating Medium datastream' ]    + medium_error_messages     if medium_error_messages    and not medium_error_messages.empty?

    safe_close(@image, medium, thumbnail)

    @updator.post_ingest
  end


  private

  def ingest_tiff ingestor

    ingestor.datastream('OBJ') do |ds|
      ds.dsLabel  = 'Original TIFF ' + @image_filename.sub(/\.(tiff|tiff)$/i, '')
      ds.content  = @image
      ds.mimeType = 'image/tiff'
    end

    jp2k, jp2k_error_messages = Utils.image_to_jp2k(@config, @image_pathname)


    ingestor.datastream('JP2') do |ds|
      ds.dsLabel  = 'JPEG 2000 derived from original TIFF image'
      ds.content  = jp2k
      ds.mimeType = 'image/jp2'
    end

  ensure
    warning  [ 'Issues converting TIFF to JP2K' ] +  jp2k_error_messages  if jp2k_error_messages and not jp2k_error_messages.empty?
    safe_close(@image, jp2k)
  end


  def ingest_jp2 ingestor

    ingestor.datastream('JP2') do |ds|
      ds.dsLabel  = 'Original JPEG 2000 ' + @image_filename.sub(/\.jp2$/i, '')
      ds.content  = @image
      ds.mimeType = 'image/jp2'
    end

    tiff, tiff_error_messages = Utils.image_to_tiff(@config, @image_pathname)

    ingestor.datastream('OBJ') do |ds|
      ds.dsLabel  = 'Reduced TIFF Derived from original JPEG 2000 Image'
      ds.content  =  tiff
      ds.mimeType = 'image/tiff'
    end

  ensure
    warning  [ 'Issues when converting JP2K to TIFF' ] +  tiff_error_messages  if tiff_error_messages and not tiff_error_messages.empty?
    safe_close(@image, tiff)
  end

end


# Subclass of Package for handling the PDF content model

class PDFPackage < Package

  # At this point we know we have a manifest, mods and maybe a marc file.

  def initialize config, directory, manifest, updator
    super(config, directory, manifest, updator)

    @content_model = PDF_CONTENT_MODEL
    @mods_type_of_resource = 'text'

    if @datafiles.length > 2
      raise PackageError, "The #{pretty_class_name} #{@directory_name} contains too many data files (only a PDF and optional OCR file allowed): #{@datafiles.join(', ')}."
    end

    if @datafiles.length == 0
      raise PackageError, "The #{pretty_class_name} #{@directory_name} contains no data files."
    end

    @pdf, @pdf_filename, @pdf_pathname = nil
    @full_text, @full_text_filename, @full_text_pathname = nil

    @datafiles.each do |filename|
      path = File.join(@directory_path, filename)
      type = Utils.mime_type(path)

      case type
      when PDF
        @pdf_filename = filename
        @pdf_pathname = path
        @pdf = true
      when TEXT
        @full_text_filename = filename
        @full_text_pathname = path
        @full_text = true
      else
        raise PackageError, "The #{pretty_class_name} #{@directory_name} contains an unexpected file #{filename} of type #{type}."
      end
    end

    raise PackageError, "The #{pretty_class_name} #{@directory_name} doesn't contain a PDF file."  if @pdf.nil?

    case

    # A full text index file was submitted, which we don't trust much, so cleanup and re-encode to UTF:

    when @full_text
      @full_text_label = "Full text from index file"
      @full_text = Utils.cleanup_text(Utils.re_encode_maybe(File.read(@full_text_pathname)))

      if @full_text.empty?
        warning "The full text file #{@full_text_filename} supplied in package #{@directory_name} was empty; using a single space to preserve the FULL_TEXT datastream."
        @full_text = ' '
      end

    # No full text, so we generate UTF-8 using a unix utility, which we'll still cleanup:
    else
      @full_text_label = 'Full text derived from PDF'

      text_from_pdf_file, errors = Utils.pdf_to_text(@config, @pdf_pathname)

      if not (errors.nil? or errors.empty?)
        warning "When extracting text from #{@pdf_filename} the following issues were encountered:"
        warning errors
      end

      @full_text = Utils.cleanup_text(text_from_pdf_file.read)

      if @full_text.nil?
        warning "Unable to generate full text from #{@pdf_filename} in package #{@directory_name}; using a single space to preserve the FULL_TEXT datastream."
        @full_text = ' '
      elsif @full_text.empty?
        warning "The generated full text from #{@pdf_filename} in package #{@directory_name} was empty; using a single space to preserve the FULL_TEXT datastream."
        @full_text = ' '
      end
    end

  rescue PackageError => e
    error "Exception for #{pretty_class_name} #{@directory_name}: #{e.message}"
  rescue => e
    error "Exception #{e.class} - #{e.message}, for #{pretty_class_name} #{@directory_name} backtrace follows:", e.backtrace
  end

  def ingest
    return if @config.test_mode

    preview, preview_error_messages, thumbnail, thumbnail_error_messages = nil

    ingestor = Ingestor.new(@config, @namespace) do |ingestor|

      boilerplate(ingestor)

      ingestor.datastream('OBJ') do |ds|
        ds.dsLabel  = @pdf_filename.sub(/\.pdf$/i, '')
        ds.content  = File.open(@pdf_pathname)
        ds.mimeType = 'application/pdf'
      end

      ingestor.datastream('FULL_TEXT') do |ds|
        ds.dsLabel  = @full_text_label
        ds.content  = @full_text
        ds.mimeType = 'text/plain'
      end

      preview, preview_error_messages      = Utils.pdf_to_preview(@config, File.join(@directory_path, @pdf_filename))

      ingestor.datastream('PREVIEW') do |ds|
        ds.dsLabel  = 'Preview'
        ds.content  = preview
        ds.mimeType = 'image/jpeg'
      end

      thumbnail, thumbnail_error_messages  = Utils.pdf_to_thumbnail(@config, File.join(@directory_path, @pdf_filename))

      ingestor.datastream('TN') do |ds|
        ds.dsLabel  = 'Thumbnail'
        ds.content  = thumbnail
        ds.mimeType = 'image/jpeg'
      end
    end

    @bytes_ingested = ingestor.size
  ensure
    warning ingestor.warnings if ingestor and ingestor.warnings?
    error   ingestor.errors   if ingestor and ingestor.errors?
    warning [ 'Issues creating Thumbnail datastream' ] + thumbnail_error_messages  if thumbnail_error_messages and not thumbnail_error_messages.empty?
    warning [ 'Issues creating Preview datastream' ]   + preview_error_messages    if preview_error_messages   and not preview_error_messages.empty?

    safe_close(preview, thumbnail)

    @updator.post_ingest
  end
end

# The StructuredPagePackage  the common methods for the BookPackage and NewspaperIssuePackage

class StructuredPagePackage < Package

  attr_reader :mets, :page_filenames, :table_of_contents, :has_mets

  def initialize config, directory, manifest, updator
    super(config, directory, manifest, updator)
    @mods_type_of_resource = 'text'
    @has_mets = File.exists?(File.join(@directory_path, 'mets.xml'))

    raise PackageError, "The #{pretty_class_name} #{@directory_name} contains no data files."  if @datafiles.empty?
  end

  private

  # Note: subclasses MUST impelement the page_rels_ext method.
  # Furthermore, there MUST be a @page_content_model instance
  # variable.

  def ingest_page pagename, label, sequence

    path      = File.join(@directory_path, pagename)
    mime_type = Utils.mime_type(path)

    pdf, pdf_error_messages, thumbnail, thumbnail_error_messages = nil

    ingestor = Ingestor.new(@config, @namespace) do |ingestor|

      ingestor.label         = pagename
      ingestor.owner         = @owner
      ingestor.content_model = @page_content_model
      ingestor.dc            = page_dc(ingestor.pid, pagename)

      case mime_type
      when TIFF;  handle_tiff_page(ingestor, path);
      when JP2;   handle_jp2k_page(ingestor, path);
      when JPEG;  handle_jpeg_page(ingestor, path);
      else
        raise PackageError, "Page image #{pagename} in #{pretty_class_name} #{@directory_name} is of unsupported type #{mime_type}."
      end

      handle_ocr(ingestor, path)

      thumbnail, thumbnail_error_messages = Utils.image_resize(@config, path, @config.thumbnail_geometry, 'jpeg')

      ingestor.datastream('TN') do |ds|
        ds.dsLabel  = 'Thumbnail'
        ds.content  = thumbnail
        ds.mimeType = 'image/jpeg'
      end

      pdf, pdf_error_messages = Utils.image_to_pdf(@config, path)

      ingestor.datastream('PDF') do |ds|
        ds.dsLabel  = 'PDF'
        ds.content  = pdf
        ds.mimeType = 'application/pdf'
      end

      ingestor.datastream('RELS-EXT') do |ds|
        ds.dsLabel  = 'Relationships'
        ds.content  = page_rels_ext(ingestor.pid, label, sequence)
        ds.mimeType = 'application/rdf+xml'
      end

      ingestor.datastream('RELS-INT') do |ds|
        ds.dsLabel  = 'RELS-INT'
        ds.content  = page_rels_int(ingestor.pid, path)
        ds.mimeType = 'application/rdf+xml'
      end

    end

    @bytes_ingested += ingestor.size
    return ingestor.pid

  rescue PackageError => e
    error e
    raise e
  rescue => e
    error "Caught exception processing page number #{sequence} #{pagename},  #{e.class} - #{e.message}.", e.backtrace
    raise e

  ensure
    warning ingestor.warnings if ingestor and ingestor.warnings?
    error   ingestor.errors   if ingestor and ingestor.errors?

    warning [ 'Issues creating Thumbnail datastream' ] + thumbnail_error_messages  if thumbnail_error_messages and not thumbnail_error_messages.empty?
    warning [ 'Issues creating PDF datastream' ]       + pdf_error_messages        if pdf_error_messages       and not pdf_error_messages.empty?

    safe_close(thumbnail, pdf)
  end


  def handle_mets
    mets_filename = File.join(@directory_path, 'mets.xml')

    if File.exists? mets_filename
      @mets =  Mets.new(@config, mets_filename)
    else
      raise PackageError, "The #{pretty_class_name}  #{@directory_name} doesn't contain a mets.xml file."
    end

    if not @mets.valid?
      error "The mets.xml file in the #{pretty_class_name} #{@directory_name} is invalid, errors follow:"
    end

    error mets.errors
    warning mets.warnings

    return valid?
  end


  def create_table_of_contents

    @table_of_contents = TableOfContents.new(@mets.structmap)

    if @table_of_contents.warnings?
      warning "Note: the table of contents derived from the METS file #{@directory_name}/mets.xml has the following issues:"
      warning @table_of_contents.warnings
    end

    if @table_of_contents.errors?
      error "The table of contents derived from the METS file #{@directory_name}/mets.xml is invalid:"
      error @table_of_contents.errors
      return false
    end

    return true
  end

  def check_page_types

    if @page_filenames.empty?
      error  "The #{pretty_class_name} #{directory_name} does not appear to have any page image files."
      return
    end

    issues = []
    @page_filenames.each do |file_name|
      path = File.join(@directory_path, file_name)
      type = Utils.mime_type(path)
      unless  type =~ JP2 or type =~ TIFF or type =~ JPEG
        issues.push "Page file #{file_name} is of unsupported type #{type}, but it must be one of image/jp2, image/jpeg, or image/tiff" \
      end
    end

    unless issues.empty?
      error "The #{pretty_class_name} #{directory_name} has #{ issues.length == 1 ? 'an invalid page image file' : 'invalid page image files'}:"
      error issues
      return
    end

    return true
  end

  # TODO: handle text files some day.

  def create_page_filename_list

    missing, expected = [], []

    if not @has_mets

      # Without METS, there's no way to find out which files should be present, so we won't be able to figure out if something is missing.
      # We also lack knowing what order they should be in.  Ah well...

      # TODO:  we can have a smarter sort here, so we don't have "1.img", "10.img", "2.img" ....

      @datafiles.sort!
      expected += @datafiles.select { |name| name =~ /\.tiff$|\.tif$|\.jp2$|\.jp2k$|\.jpg$|\.jpeg$/i }

    else

      # This checks the filenames in the list @datafiles (what's in the
      # package directory, less the metadata files) against the
      # filenames declared in the METS file table of contents (a
      # structmap).  While datafiles is a simple list of filenames, the
      # table of contents provies a Struct::Page with slots :title,
      # :level, :image_filename, :image_mimetype, and :valid_repeat.
      # A :valid_repeat file is ignored.

      @table_of_contents.pages.each do |entry|
        next if entry.valid_repeat
        expected.push entry.image_filename
        missing.push  entry.image_filename  if @datafiles.grep(entry.image_filename).empty?
      end
    end

    unexpected = @datafiles - expected

    unless unexpected.empty?
      warning "The #{pretty_class_name} #{@directory_name} has the following #{unexpected.count} unexpected #{ unexpected.length == 1 ? 'file' : 'files'} that will not be processed:"
      warning unexpected.map { |name| ' - ' + name }.sort
    end

    unless missing.empty?
      error "The #{pretty_class_name} #{@directory_name} is missing the following #{missing.count} required #{ missing.length == 1 ? 'file' : 'files'} declared in the mets.xml file:"
      error missing.map { |name| ' - ' + name }.sort
      return false
    end

    @page_filenames = expected - missing
  end

  def handle_ocr ingestor, path
    ocr_produced_text = true

    if (text = Utils.ocr(@config, path, @mods.languages))
      ingestor.datastream('OCR') do |ds|
        ds.dsLabel  = 'OCR'
        ds.content  = text
        ds.mimeType = 'text/plain'
      end
    else
      ocr_produced_text = false      # TODO:  break out into own type of warning...
      image_name = path.sub(/^.*\//, '')
      warning "The OCR and HOCR datastreams for image #{image_name} were skipped because no data were produced."
    end

    if ocr_produced_text  and (text = Utils.hocr(@config, path, @mods.languages))
      ingestor.datastream('HOCR') do |ds|
        ds.dsLabel  = 'HOCR'
        ds.content  = text
        ds.mimeType = 'text/html'
      end
    end
  end


  def handle_jpeg_page  ingestor, path
    image = File.open(path)

    jp2k, jp2k_error_messages = nil
    medium, medium_error_messages = nil

    ingestor.datastream('JPG') do |ds|
      ds.dsLabel  = 'Original JPEG ' + path.sub(/^.*\//, '').sub(/\.(jpg|jpeg)$/i, '')
      ds.content  = image
      ds.mimeType = 'image/jpeg'
    end

    jp2k, jp2k_error_messages = Utils.image_to_jp2k(@config, path)    # TODO: check and bail that jp2k is an open file and larger than zero....

    ingestor.datastream('JP2') do |ds|
      ds.dsLabel  = 'JPEG 2000 derived from original JPEG image'
      ds.content  = jp2k
      ds.mimeType = 'image/jp2'
    end

    tiff, tiff_error_messages = Utils.image_resize(@config, path, @config.tiff_from_jp2k_geometry, 'tiff')  ## TODO: everywhere we do a resize, let's bail with a package error if produced file is empty (initally I thought an empty file would be OK)

    ingestor.datastream('OBJ') do |ds|
      ds.dsLabel  = 'Reduced TIFF Derived from original JPEG Image'
      ds.content  = tiff
      ds.mimeType = 'image/tiff'
    end

  ensure
    warning ingestor.warnings if ingestor and ingestor.warnings?
    error   ingestor.errors   if ingestor and ingestor.errors?
    warning [ 'Issues creating JPK2 datastream' ] + jp2k_error_messages  if jp2k_error_messages  and not jp2k_error_messages.empty?
    warning [ 'Issues creating TIFF datastream' ] + tiff_error_messages  if tiff_error_messages  and not tiff_error_messages.empty?

    safe_close(image, jp2k, tiff)
  end


  def handle_jp2k_page ingestor, path
    image = File.open(path)

    ingestor.datastream('JP2') do |ds|
      ds.dsLabel  = 'Original JP2 ' + path.sub(/^.*\//, '').sub(/\.jp2$/i, '')
      ds.content  = image
      ds.mimeType = 'image/jp2k'
    end

    tiff, tiff_error_messages = Utils.image_resize(@config, path, @config.tiff_from_jp2k_geometry, 'tiff')

    ingestor.datastream('OBJ') do |ds|
      ds.dsLabel  = 'Reduced TIFF Derived from original JPEG 2000 Image'
      ds.content  = tiff
      ds.mimeType = 'image/tiff'
    end

    medium, medium_error_messages = Utils.image_resize(@config, path, @config.medium_geometry, 'jpeg')

    ingestor.datastream('JPG') do |ds|
      ds.dsLabel  = 'Medium sized JPEG'
      ds.content  = medium
      ds.mimeType = 'image/jpeg'
    end

  ensure
    warning ingestor.warnings if ingestor and ingestor.warnings?
    error   ingestor.errors   if ingestor and ingestor.errors?
    warning [ 'Issues creating TIFF datastream' ]      + tiff_error_messages    if tiff_error_messages    and not tiff_error_messages.empty?
    warning [ 'Issues creating Medium datastream' ]    + medium_error_messages  if medium_error_messages  and not medium_error_messages.empty?

    safe_close(image, tiff, medium)

  end


  def handle_tiff_page ingestor, path
    image = File.open(path)

    jp2k, jp2k_error_messages = nil
    medium, medium_error_messages = nil

    ingestor.datastream('OBJ') do |ds|
      ds.dsLabel  = 'Original TIFF ' + path.sub(/^.*\//, '').sub(/\.(tiff|tif)$/i, '')
      ds.content  = image
      ds.mimeType = 'image/tiff'
    end

    jp2k, jp2k_error_messages = Utils.image_to_jp2k(@config, path)

    ingestor.datastream('JP2') do |ds|
      ds.dsLabel  = "JP2 derived from original TIFF"
      ds.content  = jp2k
      ds.mimeType = 'image/jp2'
    end

    medium, medium_error_messages = Utils.image_resize(@config, path, @config.medium_geometry, 'jpeg')

    ingestor.datastream('JPG') do |ds|
      ds.dsLabel  = 'Medium sized JPEG'
      ds.content  = medium
      ds.mimeType = 'image/jpeg'
    end

  ensure
    warning ingestor.warnings if ingestor and ingestor.warnings?
    error   ingestor.errors   if ingestor and ingestor.errors?
    warning [ 'Issues creating JPK2 datastream' ]      + jp2k_error_messages    if jp2k_error_messages    and not jp2k_error_messages.empty?
    warning [ 'Issues creating Medium datastream' ]    + medium_error_messages  if medium_error_messages  and not medium_error_messages.empty?

    safe_close(image, jp2k, medium)
  end

  # Standard page rels_int (type application/rdf+xml)

  def page_rels_int page_pid, image_path
    width, height = Utils.size(@config, image_path)

    if not width or not height
      raise PackageError, "Can't determine the size of the image '#{image_path}'."
    end

    return <<-XML.gsub(/^     /, '')
    <rdf:RDF xmlns:islandora="http://islandora.ca/ontology/relsint#"
             xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
       <rdf:Description rdf:about="info:fedora/#{page_pid}/JP2">
         <width xmlns="http://islandora.ca/ontology/relsext#">#{width}</width>
         <height xmlns="http://islandora.ca/ontology/relsext#">#{height}</height>
       </rdf:Description>
     </rdf:RDF>
  XML
  end


  # Standard page DC (type text/xml)

  def page_dc page_pid, page_title

    return <<-XML.gsub(/^    /, '')
    <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
               xmlns:dc="http://purl.org/dc/elements/1.1/"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
      <dc:title>#{Utils.xml_escape(page_title)}</dc:title>
      <dc:identifier>#{page_pid}</dc:identifier>
    </oai_dc:dc>
  XML
  end

  # Derived classes must implement this:

  def page_rels_ext *ignored
    error "Must define page_rels_ext method in your subclass"
  end


end # of class StructuredPagePackage


# Subclass of Package for handling the Book content model

class BookPackage < StructuredPagePackage

  def initialize config, directory, manifest, updator
    super(config, directory, manifest, updator)

    @content_model = BOOK_CONTENT_MODEL
    @page_content_model = PAGE_CONTENT_MODEL

    handle_marc or return  # create @marc if we have a marc.xml
    handle_mets or return  # create @mets and check its validity

    create_table_of_contents   or return       # creates @table_of_contents

    create_page_filename_list  or return       # creates @page_filenames
    check_page_types           or return       # checks @page_filenames file types

  rescue PackageError => e
    error "Error processing #{pretty_class_name} #{@directory_name}: #{e.message}"
  rescue => e
    error "Exception #{e.class} - #{e.message} for #{pretty_class_name} #{@directory_name}, backtrace follows:", e.backtrace
  end

  def ingest
    return if @config.test_mode
    ingest_book
    sleep 60  # Trying to handle a race condition where Solr indexing can't get required RI data for pages, because the book object is still buffered in RI's in-memory cache.
    ingest_pages
  end

  private

  # Ingest the top-level book object

  def ingest_book
    first_page = File.join @directory_path, @page_filenames.first
    @image = File.read(first_page)

    thumbnail, thumbnail_error_messages = nil

    ingestor = Ingestor.new(@config, @namespace) do |ingestor|

      boilerplate(ingestor)

      thumbnail, thumbnail_error_messages = Utils.image_resize(@config, first_page, @config.thumbnail_geometry, 'jpeg')

      ingestor.datastream('TN') do |ds|
        ds.dsLabel  = 'Thumbnail'
        ds.content  =  thumbnail
        ds.mimeType = 'image/jpeg'
      end

      ingestor.datastream('DT-METS') do |ds|
        ds.dsLabel  = 'Archived METS for future reference'
        ds.content  = @mets.text
        ds.mimeType = 'text/xml'
      end

      ingestor.datastream('TOC') do |ds|
        ds.dsLabel  = 'Table of Contents'
        ds.content  = @table_of_contents.to_json(@mets.label)
        ds.mimeType = 'application/json'
      end
    end

    @bytes_ingested += ingestor.size

  ensure
    warning ingestor.warnings if ingestor and ingestor.warnings?
    error   ingestor.errors   if ingestor and ingestor.errors?
    warning [ 'Issues creating Thumbnail datastream' ] + thumbnail_error_messages  if thumbnail_error_messages and not thumbnail_error_messages.empty?

    safe_close(@image, thumbnail)

    @updator.post_ingest
  end

  # Ingest pages for books; we always require a METS file

  def ingest_pages
    @table_of_contents.unique_pages.each_with_index do |entry, index|
      label =  entry ?  Utils.xml_escape(entry.title) : "Page #{index + 1}"
      @component_objects.push ingest_page(entry.image_filename, label, index + 1)
    end

    if @manifest.embargo
      @component_objects.each do |pid|
        @drupal_db.add_embargo pid, @manifest.embargo['rangeName'], @manifest.embargo['endDate']
      end
    end
  end

  # Method ingest_page(), called above, is inherited from the
  # StructuredPagePackage class, which requires us to define
  # page_rels_ext().


  def page_rels_ext page_pid, page_label, sequence

    str = <<-XML
    <rdf:RDF xmlns:fedora="info:fedora/fedora-system:def/relations-external#"
             xmlns:fedora-model="info:fedora/fedora-system:def/model#"
             xmlns:islandora="http://islandora.ca/ontology/relsext#"
             xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
      <rdf:Description rdf:about="info:fedora/#{page_pid}">
        <fedora:isMemberOf rdf:resource="info:fedora/#{@pid}"></fedora:isMemberOf>
        <fedora-model:hasModel rdf:resource="info:fedora/#{PAGE_CONTENT_MODEL}"></fedora-model:hasModel>
        <islandora:isPageOf rdf:resource="info:fedora/#{@pid}"></islandora:isPageOf>
        <islandora:isSequenceNumber>#{sequence}</islandora:isSequenceNumber>
        <islandora:isPageNumber>#{page_label}</islandora:isPageNumber>
        <islandora:isSection>1</islandora:isSection>
   XML

    Utils.langs_supported(@config, @mods.languages).each do |lang|
      str += "        <islandora:hasLanguage>#{lang}</islandora:hasLanguage>"
    end

    if @inherited_policy_collection_id
      str += Utils.rels_ext_get_policy_fields(@config, @pid)
    end

    # TODO: does page_progression get included at the page level as well for books? or just book/issue level

    str += <<-XML
        <islandora:preprocess>false</islandora:preprocess>
      </rdf:Description>
    </rdf:RDF>
  XML
    return str.gsub(/^    /, '')
  end


end   #  of class BookPackage



# A package for handling the Newspaper Issue Content Model

class NewspaperIssuePackage < StructuredPagePackage

  def initialize config, directory, manifest, updator
    super(config, directory, manifest, updator)

    @content_model        = NEWSPAPER_ISSUE_CONTENT_MODEL
    @page_content_model   = NEWSPAPER_PAGE_CONTENT_MODEL

    @issue_sequence       = nil
    @newspaper_id         = nil
    @date_issued          = nil

    raise PackageError, "The #{pretty_class_name} #{@directory_name} contains no data files."  if @datafiles.empty?

    handle_marc or return  # create @marc if we have a marc.xml

    # insert "if @has_mets ... end"  around the next two lines if we decide METS is optional.

    handle_mets               or return  # create @mets and check its validity
    create_table_of_contents  or return  # creates @table_of_contents

    create_page_filename_list   or return  # creates @page_filenames
    check_page_types            or return  # checks @page_filenames file types

    check_issue_manifest        or return  # checks languages declared if any,  sets @date_issued
    check_newspaper_parent      or return  # sets @issue_sequence and @newspaper_id.

  rescue PackageError => e
    error "Error processing #{pretty_class_name} #{@directory_name}: #{e.message}"
  rescue => e
    error "Exception #{e.class} - #{e.message} for #{pretty_class_name} #{@directory_name}, backtrace follows:", e.backtrace
  end

  def ingest
    return if @config.test_mode
    ingest_issue
    sleep 60  # Trying to handle a race condition where Solr indexing can't get required RI data for pages, because the book object is still buffered in RI's in-memory cache.
    ingest_pages
  end


  private


  # Part I. Newspaper Issue level processing.
  #
  # We inherit methods boilerplate() and rels_ext() from the Package
  # class. boilerplate() normally uses the rels_ext() method as defined
  # in the Package class, but here we'll redefine rels_ext() to do a
  # slightly different thing.

  def rels_ext pid
    str = <<-XML
    <rdf:RDF xmlns:fedora="info:fedora/fedora-system:def/relations-external#"
             xmlns:fedora-model="info:fedora/fedora-system:def/model#"
             xmlns:islandora="http://islandora.ca/ontology/relsext#"
             xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
      <rdf:Description rdf:about="info:fedora/#{pid}">
        <fedora-model:hasModel rdf:resource="info:fedora/#{@content_model}"></fedora-model:hasModel>
        <fedora:isMemberOf rdf:resource="info:fedora/#{@newspaper_id}"></fedora:isMemberOf>
    XML

    if @manifest.page_progression
      str +=  "        <islandora:hasPageProgression>#{@manifest.page_progression}</islandora:hasPageProgression>"
    end

    if @inherited_policy_collection_id
      str += Utils.rels_ext_get_policy_fields(@config, @inherited_policy_collection_id)
      str += "        <islandora:inheritXacmlFrom rdf:resource=\"info:fedora/#{@inherited_policy_collection_id}\"></islandora:inheritXacmlFrom>\n"
    end

    str += <<-XML
        <islandora:isSequenceNumber>#{@issue_sequence}</islandora:isSequenceNumber>
        <islandora:dateIssued>#{@date_issued}</islandora:dateIssued>
      </rdf:Description>
    </rdf:RDF>
    XML

    return str.gsub(/^    /, '')   # prettify XML somewhat
  end


  def ingest_issue
    raise PackageError, "Errors were encountering while setting up issue information" unless valid?
    return if @config.test_mode

    thumbnail, thumbnail_error_messages = Utils.image_resize(@config, File.join(@directory_path,  @page_filenames[0]), @config.thumbnail_geometry, 'jpeg')

    ingestor = Ingestor.new(@config, @namespace) do |ingestor|

      boilerplate(ingestor)

      ingestor.datastream('TN') do |ds|
        ds.dsLabel  = "Thumbnail Image"
        ds.content  = thumbnail
        ds.mimeType = 'image/jpeg'
      end

      # At the moment we always require METS,  but it's possible to do this without.

      if @has_mets
        ingestor.datastream('DT-METS') do |ds|
          ds.dsLabel  = 'Archived METS for future reference'
          ds.content  = @mets.text
          ds.mimeType = 'text/xml'
        end

        ingestor.datastream('TOC') do |ds|
          ds.dsLabel  = 'Table of Contents'
          ds.content  = @table_of_contents.to_json(@mets.label)
          ds.mimeType = 'application/json'
        end
      end
    end

    @bytes_ingested = ingestor.size
  ensure
    warning ingestor.warnings if ingestor and ingestor.warnings?

    error   ingestor.errors   if ingestor and ingestor.errors?
    error   [ 'Error creating Thumbnail datastream' ] + thumbnail_error_messages  if thumbnail_error_messages and not thumbnail_error_messages.empty?

    safe_close(thumbnail)
    @updator.post_ingest
  end


  # Part II. Newspaper Page level processing.

  def ingest_pages
    if @has_mets
    # This is basically equivalent to the BookPackage processing.
      sequence = 0
      @table_of_contents.unique_pages.each do |entry|
        sequence += 1
        label =  entry.title ? Utils.xml_escape(entry.title) : "Page #{sequence}"
        @component_objects.push ingest_page(entry.image_filename, label, sequence)
      end
    else
    # NewspaperIssue packages don't necessarily have to have a METS file, so we do the best we can do.
      sequence = 0
      @page_filenames.each do |image_filename|
        sequence += 1
        @component_objects.push ingest_page(image_filename, "Page #{sequence}", sequence)
      end
    end

    if @manifest.embargo
      @component_objects.each do |pid|
        @drupal_db.add_embargo pid, @manifest.embargo['rangeName'], @manifest.embargo['endDate']
      end
    end
  end

  # The ingest_page() method, called above, is inherited from
  # StructuredPagePackage, but we have to provide our own method for
  # page_rels_ext() to use it.

  # TODO: does this need a page progression?

  def page_rels_ext page_pid, page_label, sequence

    # page_label unused for Newspaper pages

    str = <<-XML
    <rdf:RDF xmlns:fedora="info:fedora/fedora-system:def/relations-external#"
             xmlns:fedora-model="info:fedora/fedora-system:def/model#"
             xmlns:islandora="http://islandora.ca/ontology/relsext#"
             xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
      <rdf:Description rdf:about="info:fedora/#{page_pid}">
        <fedora-model:hasModel rdf:resource="info:fedora/#{NEWSPAPER_PAGE_CONTENT_MODEL}"></fedora-model:hasModel>
        <fedora:isMemberOf rdf:resource="info:fedora/#{@pid}"></fedora:isMemberOf>
        <islandora:isPageOf rdf:resource="info:fedora/#{@pid}"></islandora:isPageOf>
        <islandora:isSequenceNumber>#{sequence}</islandora:isSequenceNumber>
        <islandora:isPageNumber>#{sequence}</islandora:isPageNumber>
        <islandora:isSection>1</islandora:isSection>
    XML


    if @inherited_policy_collection_id
      str += Utils.rels_ext_get_policy_fields(@config, @pid)   # note that @inherited_policy_collection_id is the collection for this Issue,  which we've already ingested.  So we get policies from the Issue's datastreams (@pid)
      str += "        <islandora:inheritXacmlFrom rdf:resource=\"info:fedora/#{@pid}\"></islandora:inheritXacmlFrom>\n"
    end

    str += <<-XML
      </rdf:Description>
    </rdf:RDF>
    XML

    return str.gsub(/^    /, '')   # prettify XML somewhat
  end

  # Part III. Internal utilities.

  def oops exception
    error "Unexpected error while checking for issue's parent newspaper object #{exception}"
    error exception.backtrace
    return nil
  end

  # Check manifest for a collection that has a NEWSPAPER_CONTENT_MODEL, and return the object id.

  def get_parent_newspaper_id
    all_newspaper_ids = {}

    Utils.get_newspaper_pids(@config).each do |object_id|
      all_newspaper_ids[object_id] = true if object_id =~ /^#{@namespace}\:/
    end

    manifest_newspaper_ids = []
    @manifest.collections.each do |collection_id|
      manifest_newspaper_ids.push collection_id if all_newspaper_ids[collection_id]
    end

    case
    when manifest_newspaper_ids.empty?
      error "The collection element in the manifest.xml for this package doesn't include a parent newspaper object for #{@owning_institution}."
      error "There must be exactly one collection that is this issue's parent newspaper object."
    when manifest_newspaper_ids.length > 1
      error "The manifest.xml for this package includes too many parent newspaper objects for #{@owning_institution}: #{manifest_newspaper_ids.sort.join(', ')}."
      error "There must be exactly one collection that is this issue's parent newspaper object."
    else
      return manifest_newspaper_ids.pop
    end

  rescue => exception
    oops exception
  end


  def check_newspaper_parent

    @newspaper_id = get_parent_newspaper_id
    if not @newspaper_id
      error "Can't determine parent Newspaper object for this issue."
      return
    end

    if @collections.size > 1
      error "The #{pretty_class_name} #{@directory_name} belongs to more than one collection - there must be exactly one, a parent newspaper object."
      return
    end

    existing_issues_object_ids = Utils.get_newspaper_issues_by_date_issued(@config, @newspaper_id, @date_issued)

    if not existing_issues_object_ids.empty?
      error "The #{pretty_class_name} has an issue date #{@date_issued}, but it is already in use by #{ (existing_issues_object_ids.count == 1 ? "object " : "objects ") + existing_issues_object_ids.join(', ')}."
      return
    end

    @issue_sequence = Utils.get_next_newspaper_issue_sequence config, @newspaper_id
    if not @issue_sequence
      error "There was an error retrieving information about the issues sequences."
      return
    end

    return true
  rescue => exception
    oops exception
  end


  def check_issue_manifest

    warning_message = Utils.langs_unsupported_comment(@config, *@mods.languages)

    if not warning_message.empty?
      warning "Found unsupported OCR languages in MODS file: #{warning_message}."
      warning "Will use #{Utils.langs_to_names(@config, @mods.languages)} for OCR."
    end

    if @mods.date_issued.empty?
      error "The package MODS file does not include the required w3cdtf-encoded dateIssued element"
      return
    end

    if @mods.date_issued.count > 1
      error "The package MODS file includes more than one of the required w3cdtf-encoded dateIssued element"
      return
    end

    unless @mods.date_issued[0] =~ /^\d\d\d\d-\d\d-\d\d$/
      error "The package MODS file has a badly formatted w3cdtf-encoded dateIssued element: it should be of the form 'YYYY-MM-DD' but is '#{@mods.date_issued[0]}'"
      return
    end

    @date_issued = @mods.date_issued[0]

    return true

  rescue => exception
    oops exception
  end

end # of class NewspaperIssuePackage
