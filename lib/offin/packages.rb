require 'offin/utils'
require 'offin/manifest'
require 'offin/exceptions'
require 'offin/mods'
require 'offin/ingestor'
require 'datyl/config'
require 'RMagick'


# TODO: do we need to scope this?

BASIC_IMAGE_CONTENT_MODEL = "islandora:sp_basic_image"
LARGE_IMAGE_CONTENT_MODEL = "islandora:sp_large_image_cmodel"
PDF_CONTENT_MODEL         = "islandora:sp_pdf"

# We need a object-level read for rubydora to use;  TODO: this will be a problem with large data

class Magick::Image
  def read
    # STDERR.puts "Reading #{self}"
    self.to_blob
  end
end

# PackageFactory takes a directory path and checks the manifest.xml
# file within in it.  It determines what content model is being
# requested, and returns the appr

class PackageFactory

  attr_reader :config

  def initialize config, *additional_sections
    if config.is_a? Datyl::Config
      @config = config
    else
      @config = Datyl::Config.new(config_filename, 'default', *additional_sections)
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

  attr_reader :errors, :warnings, :manifest, :mods, :name, :marc, :config, :content_model, :namespace, :collections

  def initialize config, directory, manifest = nil

    @content_model = nil
    @errors = []
    @warnings = []
    @name = File.basename directory
    @directory = directory
    @config = config
    @datafiles = list_other_files

    if manifest.is_a? Manifest
      @manifest = manifest
    else
      @manifest = Utils.get_manifest @config, directory    # will raise PackageError on any issues - TODO: change this and check @manifest.errors
    end

    @mods = Utils.get_mods @config, directory              # will raise PackageError on any issues - TODO: change this and check @mods.errors

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
    ingestor.label = @name   # TODO: get label from complex checks of mods, manifest, etc...
    ingestor.owner = @config.owner
    ingestor.dc  = @mods.to_dc.to_s
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
        ds.content  = @image
        ds.mimeType = @image.mime_type
      end

      ingestor.datastream('TN') do |ds|
        ds.dsLabel  = "Thumbnail Image"
        ds.content  = @image.change_geometry(@config.thumbnail_geometry) { |cols, rows, img| img.resize(cols, rows) }
        ds.mimeType = @image.mime_type
      end

      ingestor.datastream('MEDIUM_SIZE') do |ds|
        ds.dsLabel  = "Medium Size Image"
        ds.content  = @image.change_geometry(@config.medium_geometry) { |cols, rows, img| img.resize(cols, rows) }
        ds.mimeType = @image.mime_type
      end

      ingestor.ingest
    end
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
    return  #############


    Ingestor.new(@config, @namespace) do |ingestor|

      boilerplate(ingestor)

      ingestor.datastream('JP2') do |ds|
        ds.dsLabel  = @image_filename
        ds.content  = @image
        ds.mimeType = @image.mime_type
      end

      # JP2 gets original
      # OBJ gets 1024x1024 tiff
      # JPG gets 600x800 jpeg
      # TN gets 200x200 jpeg



      ingestor.ingest
    end
  end
end

class PdfPackage < Package


  # At this point we know we have a manifest, mods and maybe a marc file.

  def initialize config, directory, manifest
    super(directory, manifest)
    @content_model = PDF_CONTENT_MODEL
  end
end
