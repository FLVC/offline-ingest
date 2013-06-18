require 'offin/utils'
require 'offin/manifest'
require 'offin/exceptions'
require 'offin/errors'
require 'offin/mets'
require 'offin/mods'
require 'offin/ingestor'
require 'offin/metadata-updater'
require 'datyl/config'
require 'RMagick'

BASIC_IMAGE_CONTENT_MODEL = "islandora:sp_basic_image"
LARGE_IMAGE_CONTENT_MODEL = "islandora:sp_large_image_cmodel"
PDF_CONTENT_MODEL         = "islandora:sp_pdf"
BOOK_CONTENT_MODEL        = "islandora:bookCModel"
PAGE_CONTENT_MODEL        = "islandora:pageCModel"

# PackageFactory takes a directory path and checks the manifest.xml
# file within it.  It determines what content model is being
# requested, and returns the appropriate type of package.

class PackageFactory

  attr_reader :config

  def initialize config, *additional_sections
    if config.is_a? Datyl::Config
      @config = config
    else # a string naming a file:
      @config = Datyl::Config.new(config, 'default', *additional_sections)
    end
  rescue => e
    raise SystemError, "#{e.class}: #{e.message}"
  end

  def new_package directory
    raise PackageError, "Package directory '#{directory}' doesn't exist."            unless File.exists? directory
    raise PackageError, "Package directory '#{directory}' isn't really a directory." unless File.directory? directory
    raise PackageError, "Package directory '#{directory}' isn't readable."           unless File.readable? directory

    manifest = Utils.get_manifest @config, directory

    return case manifest.content_model
           when BASIC_IMAGE_CONTENT_MODEL;  BasicImagePackage.new(@config, directory, manifest)
           when LARGE_IMAGE_CONTENT_MODEL;  LargeImagePackage.new(@config, directory, manifest)
           when PDF_CONTENT_MODEL;          PdfPackage.new(@config, directory, manifest)
           when BOOK_CONTENT_MODEL;         BookPackage.new(@config, directory, manifest)
           else
             raise PackageError, "Package directory '#{directory}' specifies an unsupported content model '#{manifest.content_model}'"
           end
  end
end


# Package serves as a base class, but it could serve to do a basic
# check on a directory's well-formedness as a package.

class Package

  include Errors

  # supported MIME types go here, using a regexp of the 'file' command's returned values (note generalization of text, for which file tends to outsmart itself)

  GIF  = %r{image/gif}
  JP2  = %r{image/jp2}
  PNG  = %r{image/png}
  JPEG = %r{image/jpeg}
  TIFF = %r{image/tiff}
  PDF  = %r{application/pdf}
  TEXT = %r{text/}


  attr_reader :manifest, :mods, :marc, :config, :content_model, :namespace, :collections, :label, :owner, :directory_name, :directory_path, :bytes_ingested, :pid

  def initialize config, directory, manifest = nil

    @valid          = true
    @pid            = nil
    @config         = config
    @content_model  = nil
    @label          = nil
    @owner          = nil
    @directory_name = File.basename(directory)
    @directory_path = directory
    @datafiles      = list_other_files()
    @bytes_ingested = 0

    if manifest.is_a? Manifest
      @manifest = manifest
    else
      @manifest = Utils.get_manifest @config, @directory_path
    end

    if @manifest.errors?
      error "The package #{@directory_name} doesn't have a valid manifest file."
      error @manifest.errors
    end

    @valid &&= @manifest.valid?

    @mods = Utils.get_mods @config, @directory_path

    if @mods.errors?
      error "The package #{@directory_name} doesn't have a valid MODS file."
      error @mods.errors
    end

    @valid &&= @mods.valid?

    return unless valid?

    marc_file = File.join(@directory_path, 'marc.xml')

    if File.exists?(marc_file)
      @marc = File.read(marc_file)
    else
      @marc = nil
    end

    @namespace   = @manifest.owning_institution.downcase
    @collections = list_collections()

  rescue PackageError => e
    error "Exception for package #{@directory_name}: #{e.message}"
  rescue => e
    error "Exception for package #{@directory_name}: #{e.class} - #{e.message}, backtrace follows:", e.backtrace
  end

  def list_collections
    remapper = @config.remap_collections || {}
    list = []
    @manifest.collections.each do |pid|
      p = pid.downcase
      list.push(remapper[p] || p)
    end
    return list
  end


  def name
    @directory_name
  end

  # base classes should re-implement ingest.

  def ingest
    raise PackageError, 'Attempt to ingest an invalid package.' unless valid?
  end

  def valid?
    @valid and not errors?
  end


  # A MetdataUpdater mediates metadata updates according to rules you're
  # better off not knowing, and various wildly by originating system.
  #
  # TODO: flesh this out
  #
  # updater needs to:
  #
  # If simple digitool package (basic, pdf, large):  expects MODS, Manifest objects.
  #      MODS gets updated with selected manifest data:
  #      DC gets generated with
  # If books digitool package: expects METS with embedded manifest data (why?)
  #      MODS


  def updater= value
    metadata_updater = value.send :new, @manifest, @mods
    @label = metadata_updater.get_label @directory_name        # probably want to do all of this in ingestor block, or boilerplate.... we'll have the ingest PID at that point...
    @owner = metadata_updater.get_owner
    # metadata_updater.identifiers
  end

  # Used by all subclassess.  Note that a MetadataUpdater call will
  # have to have first been made to properly set up some of these
  # (label, owner).

  def boilerplate ingestor

    @pid = ingestor.pid
    @mods.add_islandora_identifier ingestor.pid
    @mods.add_extension_elements @manifest

    # somewhat order dependent

    ingestor.label         = @label
    ingestor.owner         = @owner
    ingestor.content_model = @content_model
    ingestor.collections   = @collections
    ingestor.dc            = @mods.to_dc
    ingestor.mods          = @mods.to_s

    if @marc
      ingestor.datastream('MARCXML') do |ds|
        ds.dsLabel  = "Archived Digitool MarcXML"
        ds.content  = @marc
        ds.mimeType = 'text/xml'
      end
    end
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
end # of Package base class



class BasicImagePackage < Package

  # At this point we know we have a manifest, mods and maybe a marc file.

  def initialize config, directory, manifest
    super(config, directory, manifest)

    @content_model = BASIC_IMAGE_CONTENT_MODEL

    if @datafiles.length > 1
      error "The Basic Image package #{@directory_name} contains too many data files (only one expected): #{@datafiles.join(', ')}."
    end

    if @datafiles.length == 0
      error "The Basic Image package #{@directory_name} contains no data files."
    end

    return unless valid?

    @image_filename = @datafiles[0]
    path = File.join(@directory_path, @image_filename)
    type = Utils.mime_type(path)

    case type
    when GIF, JPEG, PNG
      @image = Magick::Image.read(path).first

      # TODO: add special support for TIFFs (not needed for digitool migration)

    when TIFF
      raise PackageError, "The Basic Image package #{@directory_name} contains the TIFF file #{@datafiles[0]}, which is currently unsupported (coming soon)."
    else
      raise PackageError, "The Basic Image package #{@directory_name} contains an unexpected file #{@datafiles[0]} with mime type #{type}."
    end

  rescue PackageError => e
    error "Exception for Basic Image package #{@directory_name}: #{e.message}"
  rescue => e
    error "Exception #{e.class} - #{e.message} for Basic Image package #{@directory_name}, backtrace follows:", e.backtrace
  end

  def ingest
    ingestor = Ingestor.new(@config, @namespace) do |ingestor|

      boilerplate(ingestor)

      ingestor.datastream('OBJ') do |ds|
        ds.dsLabel  = @image_filename
        ds.content  = File.open(File.join(@directory_path, @image_filename))
        ds.mimeType = @image.mime_type
      end

      ingestor.datastream('MEDIUM_SIZE') do |ds|
        ds.dsLabel  = "Medium Size Image"
        ds.content  = @image.change_geometry(@config.medium_geometry) { |cols, rows, img| img.resize(cols, rows) }.to_blob
        ds.mimeType = @image.mime_type
      end

      ingestor.datastream('TN') do |ds|
        ds.dsLabel  = "Thumbnail Image"
        ds.content  = @image.change_geometry(@config.thumbnail_geometry) { |cols, rows, img| img.resize(cols, rows) }.to_blob
        ds.mimeType = @image.mime_type
      end
    end

    @bytes_ingested = ingestor.size
  ensure
    warning "Ingest warnings:", ingestor.warnings if ingestor and ingestor.warnings?
    error   "Ingest errors:",   ingestor.errors   if ingestor and ingestor.errors?
    @image.destroy! if @image.class == Magick::Image
  end
end


class LargeImagePackage < Package

  # At this point we know we have a manifest, mods and maybe a marc file.

  attr_reader :image

  def initialize config, directory, manifest
    super(config, directory, manifest)

    @content_model = LARGE_IMAGE_CONTENT_MODEL

    if @datafiles.length > 1
      error "The Large Image package #{@directory_name} contains too many data files (only one expected): #{@datafiles.join(', ')}."
    end

    if @datafiles.length == 0
      raise PackageError, "The Large Image package #{@directory_name} contains no data files."
    end

    return unless valid?

    @image = nil
    @image_filename = @datafiles[0]
    path = File.join(@directory_path, @image_filename)
    type = Utils.mime_type(path)

    # TODO: add basic support for TIFFs (not needed for digitool migration)

    case type
    when JP2
      @image = Magick::Image.read(path).first
    when TIFF
      raise PackageError, "The Large Image package #{@directory_name} contains the TIFF file #{@datafiles[0]}, which is currently unsupported (coming soon)."
    else
      raise PackageError, "The Large Image package #{@directory_name} contains an unexpected or unsupported file #{@datafiles[0]} with mime type #{type}."
    end

  rescue PackageError => e
    error "Exception for Large Image package #{@directory_name}: #{e.message}"
  rescue => e
    error "Exception #{e.class} - #{e.message} for Large Image package #{@directory_name}, backtrace follows:", e.backtrace
  end

  def ingest

    ingestor = Ingestor.new(@config, @namespace) do |ingestor|

      boilerplate(ingestor)

      ingestor.datastream('JP2') do |ds|
        ds.dsLabel  = 'Original JPEG 2000 ' + @image_filename.sub(/\.jp2$/i, '')
        ds.content  = File.open(File.join(@directory_path, @image_filename))
        ds.mimeType = @image.mime_type
      end

      @image.format = 'TIFF'
      @image.compression = Magick::LZWCompression

      ingestor.datastream('OBJ') do |ds|
        ds.dsLabel  = 'Reduced TIFF Derived from original JPEG 2000 Image'
        ds.content  = @image.change_geometry(@config.tiff_from_jp2k_geometry) { |cols, rows, img| img.resize(cols, rows) }.to_blob
        ds.mimeType = @image.mime_type
      end

      @image.format = 'JPG'

      ingestor.datastream('JPG') do |ds|
        ds.dsLabel  = 'Medium sized JPEG'
        ds.content  = @image.change_geometry(@config.large_jpg_geometry) { |cols, rows, img| img.resize(cols, rows) }.to_blob
        ds.mimeType = @image.mime_type
      end

      ingestor.datastream('TN') do |ds|
        ds.dsLabel  = 'Thumbnail'
        ds.content  = @image.change_geometry(@config.thumbnail_geometry) { |cols, rows, img| img.resize(cols, rows) }.to_blob
        ds.mimeType = @image.mime_type
      end
    end

    @bytes_ingested = ingestor.size
  ensure
    warning "Ingest warnings:", ingestor.warnings if ingestor and ingestor.warnings?
    error   "Ingest errors:",   ingestor.errors   if ingestor and ingestor.errors?
    @image.destroy! if @image.class == Magick::Image
  end
end

class PdfPackage < Package

  # At this point we know we have a manifest, mods and maybe a marc file.

  def initialize config, directory, manifest
    super(config, directory, manifest)
    @content_model = PDF_CONTENT_MODEL

    if @datafiles.length > 2
      raise PackageError, "The PDF package #{@directory_name} contains too many data files (only a PDF and optional OCR file allowed): #{@datafiles.join(', ')}."
    end

    if @datafiles.length == 0
      raise PackageError, "The PDF package #{@directory_name} contains no data files."
    end

    @pdf = nil
    @pdf_filename = nil

    @full_text = nil
    @full_text_filename = nil

    @datafiles.each do |filename|
      path = File.join(@directory_path, filename)
      type = Utils.mime_type(path)
      case type
      when PDF
        @pdf_filename = filename
        @pdf = File.read(path)
      when TEXT
        @full_text_filename = filename
        @full_text = File.read(path)
      else
        raise PackageError, "The PDF package #{@directory_name} contains an unexpected file #{filename} of type #{type}."
      end
    end

    raise PackageError, "The PDF package #{@directory_name} doesn't contain a PDF file."  if @pdf.nil?

    case
    # A full text index file was submitted, which we don't trust much, so cleanup and re-encode to UTF:
    when @full_text
      @full_text_label = "Full text from index file"
      @full_text = Utils.cleanup_text(Utils.re_encode_maybe(@full_text))
      if @full_text.empty?
        warning "The full text file #{@full_text_filename} supplied in package #{@directory_name} was empty; using a single space to preserve the FULL_TEXT datastream."
        @full_text = ' '
      end
    # No full text, so we generate UTF-8 using a unix utility, which we'll still cleanup:

    # TODO:  pdf_to_text errors should not be fatal....

    else
      @full_text_label = 'Full text derived from PDF'
      @full_text = Utils.cleanup_text(Utils.pdf_to_text(@config, File.join(@directory_path, @pdf_filename)))
      if @full_text.nil?
        warning "Unable to generate full text from #{@pdf_filename} in package #{@directory_name}; using a single space to preserve the FULL_TEXT datastream."
        @full_text = ' '
      elsif @full_text.empty?
        warning "The generated full text from #{@pdf_filename} in package #{@directory_name} was empty; using a single space to preserve the FULL_TEXT datastream."
        @full_text = ' '
      end
    end

  rescue PackageError => e
    error "Exception for PDF package #{@directory_name}: #{e.message}"
  rescue => e
    error "Exception #{e.class} - #{e.message}, for PDF package #{@directory_name} backtrace follows:", e.backtrace
  end

  def ingest

    # Do image processing upfront so as to fail faster, if fail we must, before ingest is started.

    thumb   = Utils.pdf_to_thumbnail @config, File.join(@directory_path, @pdf_filename)
    preview = Utils.pdf_to_preview @config, File.join(@directory_path, @pdf_filename)

    ingestor = Ingestor.new(@config, @namespace) do |ingestor|

      boilerplate(ingestor)

      ingestor.datastream('OBJ') do |ds|
        ds.dsLabel  = @pdf_filename.sub(/\.pdf$/i, '')
        ds.content  = @pdf
        ds.mimeType = 'application/pdf'
      end

      ingestor.datastream('FULL_TEXT') do |ds|
        ds.dsLabel  = @full_text_label
        ds.content  = @full_text
        ds.mimeType = 'text/plain'
      end

      ingestor.datastream('PREVIEW') do |ds|
        ds.dsLabel  = 'Preview'
        ds.content  = preview
        ds.mimeType = 'image/jpeg'
      end

      ingestor.datastream('TN') do |ds|
        ds.dsLabel  = 'Thumbnail'
        ds.content  = thumb
        ds.mimeType = 'image/jpeg'
      end
    end

    @bytes_ingested = ingestor.size
  ensure
    warning "Ingest warnings:", ingestor.warnings if ingestor and ingestor.warnings?
    error   "Ingest errors:",   ingestor.errors   if ingestor and ingestor.errors?
  end
end


class BookPackage < Package

  attr_reader :mets, :page_filenames, :table_of_contents, :page_pids

  def initialize config, directory, manifest
    super(config, directory, manifest)

    @content_model = BOOK_CONTENT_MODEL

    raise PackageError, "The Book package #{@directory_name} contains no data files."  if @datafiles.empty?

    handle_marc or return  # create @marc if we have a marc.xml
    handle_mets or return  # create @mets and check its validity

    create_table_of_contents or return       # creates @table_of_contents
    reconcile_file_lists     or return       # creates @page_filenames
    check_page_types         or return       # checks @page_filenames file types

  rescue PackageError => e
    error "Exception for Book package #{@directory_name}: #{e.message}"
  rescue => e
    error "Exception #{e.class} - #{e.message} for Book package #{@directory_name}, backtrace follows:", e.backtrace
  end


  def ingest

    ingest_book
    ingest_pages

  end



  private


  # TODO: This following is (probably) a digitool-only case  and really should be pulled out into the metadata-updater class

  def handle_marc
    marc_filename = File.join(@directory_path, 'marc.xml')
    @marc = File.read(marc_filename) if File.exists? marc_filename
    return true
  end

  def handle_mets
    mets_filename = File.join(@directory_path, 'mets.xml')

    if File.exists? mets_filename
      @mets =  Mets.new(@config, mets_filename)
    else
      raise PackageError, "The Book package #{@directory_name} doesn't contain a mets.xml file."
    end

    if not @mets.valid?
      error "The mets.xml file in the Book package #{@directory_name} is invalid, errors follow:"
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
    issues = []
    @page_filenames.each do |file_name|
      path = File.join(@directory_path, file_name)
      type = Utils.mime_type(path)
      issues.push "Page file #{file_name} is of unsupported type #{type}, but must be image/jp2 or image/tiff" if not type =~ JP2 or type =~ TIFF
    end
    unless issues.empty?
      error "The Book Package #{directory_name} has #{ issues.length == 1 ? 'an invalid page image file' : 'invalid page image files'}:"
      error issues
    end
  end


  def reconcile_file_lists

    missing     = []
    expected    = []

    # this checks what's declared in the METS file (the structmap
    # part) against what's in the package directory, less the metadata
    # files (which we have as @datafiles).  Entry is a Struct::Page
    # with slots :title, :level, :image_filename, :image_mimetype,
    # :text_filename, :text_mimetype


    @table_of_contents.pages.each do |entry|
      expected.push entry.image_filename
      missing.push  entry.image_filename  if @datafiles.grep(entry.image_filename).empty?
    end

    unexpected = @datafiles - expected

    unless unexpected.empty?
      warning "The Book package #{@directory_name} has the following unexpected #{ unexpected.length == 1 ? 'file' : 'files'} that will not be processed:"
      warning unexpected
    end

    unless missing.empty?
      error "The Book package #{@directory_name} is missing the following required #{ missing.length == 1 ? 'file' : 'files'} declared in the mets.xml file:"
      error missing
      return false
    end

    @page_filenames = expected - missing
  end


  def ingest_book
    # TODO: in initialization, do a check to make sure that there are *some* page files... we need at least one.

    first_page = File.join @directory_path, @page_filenames[0]
    @image = Magick::Image.read(first_page).first

    ingestor = Ingestor.new(@config, @namespace) do |ingestor|

      boilerplate(ingestor)

      @image.format = 'JPG'

      ingestor.datastream('TN') do |ds|
        ds.dsLabel  = 'Thumbnail'
        ds.content  = @image.change_geometry(@config.thumbnail_geometry) { |cols, rows, img| img.resize(cols, rows) }.to_blob
        ds.mimeType = @image.mime_type
      end

      ingestor.datastream('DT-METS') do |ds|
        ds.dsLabel  = 'Archived DigiTool METS for future reference'
        ds.content  = @mets.text
        ds.mimeType = 'text/xml'
      end

      ingestor.datastream('TOC') do |ds|
        ds.dsLabel  = 'Table of Contents'
        ds.content  = @table_of_contents.to_json(@mets.label)
        ds.mimeType = 'application/json'
      end

    end

    @bytes_ingested = ingestor.size
  ensure
    warning "Ingest warnings:", ingestor.warnings if ingestor and ingestor.warnings?
    error   "Ingest errors:",   ingestor.errors   if ingestor and ingestor.errors?
    @image.destroy! if @image.class == Magick::Image
  end


  def ingest_pages
    sequence = 0
    @page_filenames.each do |pagename|
      begin
        sequence += 1
        @page_pids.push ingest_page(pagename, sequence)
      rescue PackageError => e
        warning "Error ingesting page #{pagename} for Book package #{@directory_name}: #{e.message}"
      rescue => e
        warning "Exception #{e.class} when ingesting page #{pagename} for Book package #{@directory_name}:", e.message, e.backtrace
      end
    end
  end


  # read in the image and branch, appropriately, to TIFF or JP2K handling....
  #
  # RELS-EXT application/rdf+xml
  #
  # <rdf:RDF xmlns:fedora="info:fedora/fedora-system:def/relations-external#"
  #                xmlns:fedora-model="info:fedora/fedora-system:def/model#"
  #                xmlns:islandora="http://islandora.ca/ontology/relsext#"
  #                xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
  #   <rdf:Description rdf:about="info:fedora/islandora:1286">
  #     <islandora:isPageOf rdf:resource="info:fedora/islandora:1285"></islandora:isPageOf>
  #     <islandora:isSequenceNumber>1</islandora:isSequenceNumber>
  #     <islandora:isPageNumber>1</islandora:isPageNumber>
  #     <islandora:isSection>1</islandora:isSection>
  #     <fedora:isMemberOf rdf:resource="info:fedora/islandora:1285"></fedora:isMemberOf>
  #     <fedora-model:hasModel rdf:resource="info:fedora/islandora:pageCModel"></fedora-model:hasModel>
  #     <islandora:hasLanguage>eng</islandora:hasLanguage>
  #     <islandora:preprocess>false</islandora:preprocess>
  #   </rdf:Description>
  # </rdf:RDF>
  #
  # RELS-EXT application/rdf+xml
  #
  # <rdf:RDF xmlns:islandora="http://islandora.ca/ontology/relsint#"
  #          xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
  #   <rdf:Description rdf:about="info:fedora/islandora:1286/JP2">
  #     <width xmlns="http://islandora.ca/ontology/relsext#">2516</width>
  #     <height xmlns="http://islandora.ca/ontology/relsext#">3260</height>
  #   </rdf:Description>
  # </rdf:RDF>
  #
  # DC text/xml
  #
  # <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
  #            xmlns:dc="http://purl.org/dc/elements/1.1/"
  #            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  #            xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
  #   <dc:title>hdtv-000001.tiff</dc:title>
  #   <dc:identifier>islandora:1286</dc:identifier>
  # </oai_dc:dc>
  #
  # HOCR text/html    tesseract image tempfile.html
  # OCR  text/plain   tesseract image tempfile.txt
  #
  # PDF               imagemagick convert (from tiff?), use -compress LZW
  # JPG  image/jpeg - medium size (use large_jpg_geometry)
  # OBJ  image/tiff - if derived form JP2K, same size?  as JP2K but make compressed
  # TN   image/jpeg


  def ingest_page pagename, sequence

    return

    pathname = File.join(@directory_path, pagename)
    image    = Magick::Image.read(pathname).first

    ingestor = Ingestor.new(@config, @namespace) do |ingestor|


      ingestor.label         = @label
      ingestor.owner         = @owner
      ingestor.content_model = PAGE_CONTENT_MODEL

      # if type is JP2,  otherwise if type is TIFF...

      image.format = 'JPG'

      ingestor.datastream('JPG') do |ds|
        ds.dsLabel  = 'Medium sized JPEG'
        ds.content  = image.change_geometry(@config.large_jpg_geometry) { |cols, rows, img| img.resize(cols, rows) }.to_blob
        ds.mimeType = imwage.mime_type
      end

      ingestor.datastream('TN') do |ds|
        ds.dsLabel  = 'Thumbnail'
        ds.content  = image.change_geometry(@config.thumbnail_geometry) { |cols, rows, img| img.resize(cols, rows) }.to_blob
        ds.mimeType = image.mime_type
      end

      ingestor.datastream('HOCR') do |ds|
        ds.dsLabel  = 'HOCR'
        ds.content  = Utils.hocr(@config, pathname)
        ds.mimeType = 'text/html'
      end

      ingestor.datastream('OCR') do |ds|
        ds.dsLabel  = 'OCR'
        ds.content  = Utils.ocr(@config, pathname)
        ds.mimeType = 'text/plain'
      end



    end

    return ingestor.pid

  ensure
    warning "Ingest warnings:", ingestor.warnings if ingestor and ingestor.warnings?
    error   "Ingest errors:",   ingestor.errors   if ingestor and ingestor.errors?
    image.destroy! if image.class == Magick::Image
  end
end
