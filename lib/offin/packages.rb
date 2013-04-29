# TODO: make sure PDF text is UTF8 (no illegal control characters)

require 'offin/utils'
require 'offin/manifest'
require 'offin/exceptions'
require 'offin/mods'
require 'offin/ingestor'
require 'datyl/config'
require 'RMagick'

BASIC_IMAGE_CONTENT_MODEL = "islandora:sp_basic_image"
LARGE_IMAGE_CONTENT_MODEL = "islandora:sp_large_image_cmodel"
PDF_CONTENT_MODEL         = "islandora:sp_pdf"

# PackageFactory takes a directory path and checks the manifest.xml
# file within in it.  It determines what content model is being
# requested, and returns the appropriate type of package.

class PackageFactory

  attr_reader :config

  def initialize config, *additional_sections
    if config.is_a? Datyl::Config
      @config = config
    else # a string naming a file:
      @config = Datyl::Config.new(config, 'default', *additional_sections)
    end
    sanity_check
  rescue => e
    raise SystemError, "#{e.class}: #{e.message}"
  end

  # TODO: sanity check config, that it has what we think we need...

  def sanity_check
    raise "" if false
  end

  def new_package directory
    raise PackageError, "Package directory '#{directory}' doesn't exist."     unless File.exists? directory
    raise PackageError, "Package directory '#{directory}' isn't a driectory." unless File.directory? directory
    raise PackageError, "Package directory '#{directory}' isn't readable."    unless File.readable? directory

    manifest = Utils.get_manifest @config, directory

    return case manifest.content_model
           when BASIC_IMAGE_CONTENT_MODEL;  BasicImagePackage.new(@config, directory, manifest)
           when LARGE_IMAGE_CONTENT_MODEL;  LargeImagePackage.new(@config, directory, manifest)
           when PDF_CONTENT_MODEL:          PdfPackage.new(@config, directory, manifest)
           else
             raise PackageError, "Package directory '#{directory}' specifies an unsupported content model '#{manifest.content_model}'"
           end
  end
end


# Package serves as a base class, but it could serve to do a basic
# check on a directory's well-formedness as a package.

class Package

  # supported MIME types go here, as returned by the file command.

  GIF  = 'image/gif'
  JP2  = 'image/jp2'
  PNG  = 'image/png'
  JPEG = 'image/jpeg'
  TIFF = 'image/tiff'
  PDF  = 'application/pdf'
  TEXT = 'text/plain'

  attr_reader :errors, :warnings, :manifest, :mods, :name, :marc, :config, :content_model, :namespace, :collections

  def initialize config, directory, manifest = nil

    @content_model = nil
    @errors = []
    @warnings = []
    @name = File.basename directory
    @directory = directory
    @config = config
    @datafiles = list_other_files()

    if manifest.is_a? Manifest
      @manifest = manifest
    else
      @manifest = Utils.get_manifest @config, directory    # will raise PackageError on any issues - TODO: change this, check @manifest.errors, copy to our @errors and exit
    end

    @mods = Utils.get_mods @config, directory              # will raise PackageError on any issues - TODO: change this and check @mods.errors, copy to our @errors and exit

    @namespace = @manifest.owning_institution.downcase
    @collections = @manifest.collections

    marc_file = File.join(@directory, 'marc.xml')

    if File.exists?(marc_file)
      @marc = File.read(marc_file)
    else
      @marc = nil
    end

  end

  # Used by subclassess:

  def boilerplate ingestor
    ingestor.collections = @collections.map { |pid| pid.downcase }   # Liang doesn't read my specs...
    ingestor.content_model = @content_model
    ingestor.label = @name           # TODO: get label from complex checks of mods, manifest, etc...
    ingestor.owner = @config.owner   # TODO: same with owner
    ingestor.dc  = @mods.to_dc
    ingestor.mods = @mods.to_s

    if @marc
      ingestor.datastream('MARCXML') do |ds|
        ds.dsLabel  = "Archived Digitool MarcXML"
        ds.content  = @marc
        ds.mimeType = 'text/xml'
      end
    end
  end

  # TODO: remove these if really unused

  def errors?
    @errors.empty?
  end

  def warnings?
    @warnings.empty?
  end

  # List all the files in the directory we haven't already accounted for. Subclasses will need to work through these.
  # Presumably these are all datafiles.

  def list_other_files

    # TODO: throw PackageError if a directory is found.

    list = []
    Dir["#{@directory}/*"].each do |entry|
      filename = File.basename entry
      next if [ '.', '..', 'manifest.xml', 'marc.xml', "#{name}.xml" ].include? filename
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
      raise PackageError, "The Basic Image package #{@name} contains too many data files (only one expected): #{@datafiles.join(', ')}."
    end

    if @datafiles.length == 0
      raise PackageError, "The Basic Image package #{@name} contains no data files."
    end

    @image_filename = @datafiles[0]
    path = File.join(@directory, @image_filename)
    type = Utils.mime_type(path)

    case type
    when GIF, JPEG, PNG
      @image = Magick::Image.read(path).first

      # TODO: add special support for TIFFs (not needed for digitool migration)

    when TIFF
      raise PackageError, "The Basic Image package #{@name} contains the TIFF file #{@datafiles[0]}, which is currently unsupported."
    else
      raise PackageError, "The Basic Image package #{@name} contains an unexpected file #{@datafiles[0]} with mime type #{type}."
    end

  end


  def process
    Ingestor.new(@config, @namespace) do |ingestor|

      boilerplate(ingestor)

      ingestor.datastream('OBJ') do |ds|
        ds.dsLabel  = @image_filename
        ds.content  = @image.to_blob
        ds.mimeType = @image.mime_type
      end

      ingestor.datastream('TN') do |ds|
        ds.dsLabel  = "Thumbnail Image"
        ds.content  = @image.change_geometry(@config.thumbnail_geometry) { |cols, rows, img| img.resize(cols, rows) }.to_blob
        ds.mimeType = @image.mime_type
      end

      ingestor.datastream('MEDIUM_SIZE') do |ds|
        ds.dsLabel  = "Medium Size Image"
        ds.content  = @image.change_geometry(@config.medium_geometry) { |cols, rows, img| img.resize(cols, rows) }.to_blob
        ds.mimeType = @image.mime_type
      end

    end
  ensure
    @image.destroy! if @image and @image.class == Magick::Image
  end
end


class LargeImagePackage < Package

  # At this point we know we have a manifest, mods and maybe a marc file.

  attr_reader :image

  def initialize config, directory, manifest
    super(config, directory, manifest)
    @content_model = LARGE_IMAGE_CONTENT_MODEL

    if @datafiles.length > 1
      raise PackageError, "The Large Image package #{@name} contains too many data files (only one expected): #{@datafiles.join(', ')}."
    end

    if @datafiles.length == 0
      raise PackageError, "The Larg Image package #{@name} contains no data files."
    end

    @image = nil
    @image_filename = @datafiles[0]
    path = File.join(@directory, @image_filename)
    type = Utils.mime_type(path)


    case type
    when JP2

      @image = Magick::Image.read(path).first

      # TODO: add basic support for TIFFs (not needed for digitool migration)

    when TIFF
      raise PackageError, "The Large Image package #{@name} contains the TIFF file #{@datafiles[0]}, which is currently unsupported."
    else
      raise PackageError, "The Large Image package #{@name} contains an unexpected or unsupported file #{@datafiles[0]} with mime type #{type}."
    end

  end


  def process
    Ingestor.new(@config, @namespace) do |ingestor|

      boilerplate(ingestor)

      ingestor.datastream('JP2') do |ds|
        ds.dsLabel  = 'Original JPEG 2000 ' + @image_filename.sub(/\.jp2$/i, '')
        ds.content  = @image.to_blob
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

  ensure
    @image.destroy! if @image and @image.class == Magick::Image
  end
end


class PdfPackage < Package

  # At this point we know we have a manifest, mods and maybe a marc file.

  def initialize config, directory, manifest
    super(config, directory, manifest)
    @content_model = PDF_CONTENT_MODEL


    if @datafiles.length > 2
      raise PackageError, "The PDF package #{@name} contains too many data files (only a PDF and optional OCR file allowed): #{@datafiles.join(', ')}."
    end

    if @datafiles.length == 0
      raise PackageError, "The PDF package #{@name} contains no data files."
    end

    @pdf_filename = nil
    @ocr_filename = nil
    @pdf = nil
    @ocr = nil


    @datafiles.each do |filename|
      path = File.join(@directory, filename)
      type = Utils.mime_type(path)
      case type
      when PDF
        @pdf_filename = filename
        @pdf = File.read(path)
      when TEXT
        @ocr_filename = filename
        @ocr = File.read(path)
      else
        raise PackageError, "The PDF package #{@name} contains an unexpected file #{filename} of type #{type}."
      end
    end

    if @pdf.nil?
      raise PackageError, "The PDF package #{@name} doesn't contain a PDF file."
    end

    text = Utils.pdf_to_text(@config, File.join(@directory, @pdf_filename))

    # TODO: check to make sure text isn't empty

    if @ocr.nil?
      @ocr = text
    end

  end

  def process
    Ingestor.new(@config, @namespace) do |ingestor|

      boilerplate(ingestor)

      ingestor.datastream('OBJ') do |ds|
        ds.dsLabel  = @pdf_filename.sub(/\.pdf$/i, '')
        ds.content  = @pdf
        ds.mimeType = PDF
      end

      ingestor.datastream('FULL_TEXT') do |ds|
        ds.dsLabel  = 'Full Text'
        ds.content  = @ocr
        ds.mimeType = TEXT
      end

      img = Utils.pdf_to_preview @config, File.join(@directory, @pdf_filename)

      # TODO: check to make sure img isn't empty

      ingestor.datastream('PREVIEW') do |ds|
        ds.dsLabel  = 'Preview'
        ds.content  = img
        ds.mimeType = JPEG
      end

      img = Utils.pdf_to_thumbnail @config, File.join(@directory, @pdf_filename)

      # TODO: check to make sure img isn't empty

      ingestor.datastream('TN') do |ds|
        ds.dsLabel  = 'Thumbnail'
        ds.content  = img
        ds.mimeType = JPEG
      end
    end
  end
end
