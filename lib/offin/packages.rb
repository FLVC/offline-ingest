require 'offin/utils'
require 'offin/manifest'
require 'offin/exceptions'
require 'offin/mods'
require 'datyl/config'
require 'RMagick'

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

  BASIC_IMAGE_MODEL = "islandora:sp_basic_image"
  LARGE_IMAGE_MODEL = "islandora:sp_large_image_cmodel"
  PDF_MODEL         = "islandora:sp_pdf"

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
           when BASIC_IMAGE_MODEL;  BasicImagePackage.new(@config, directory, manifest)
           when LARGE_IMAGE_MODEL;  LargeImagePackage.new(@config, directory, manifest)
           when PDF_MODEL:          PdfPackage.new(@config, directory, manifest)
           else
             raise PackageError, "Package directory '#{directory}' specifies an unsupported content model '#{manifest.content_model}'"
           end
  end
end


# Package serves as a base class, but it could be used to do a basic
# check on a directory's well-formedness as a package.

class Package

  GIF  = 'image/gif'
  JP2  = 'image/jp2'
  JPEG = 'image/jpeg'
  PNG  = 'image/png'
  TIFF = 'image/tiff'

  attr_reader :errors, :warnings, :manifest, :mods, :name, :marc, :config

  def initialize config, directory, manifest = nil
    @errors = []
    @warnings = []
    @name = File.basename directory
    @directory = directory
    @config = config
    @datafiles = list_other_files

    if manifest.is_a? Manifest
      @manifest = manifest
    else
      @manifest = Utils.get_manifest @config, directory
    end

    @mods = Utils.get_mods @config, directory

    marc_file = File.join(@directory, 'marc.xml')

    if File.exists?(marc_file)
      @marc = File.read(marc_file)
    else
      @marc = nil
    end

  end

  def errors?
    @errors.empty?
  end

  def warnings?
    @warnings.empty?
  end

  # List all the files in the directory we haven't already accounted for. Subclasses will need to work through these.
  # Presumably these are all datafiles.

  def list_other_files

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

  attr_reader :image

  def initialize config, directory, manifest
    super(config, directory, manifest)

    if @datafiles.length > 1
      raise PackageError, "The Basic Image package #{@name} contains too many data files (only one expected): #{@datafiles.join(', ')}."
    end

    if @datafiles.length == 0
      raise PackageError, "The Basic Image package #{@name} contains no data files."
    end

    image_filename = File.join(@directory, @datafiles[0])
    type = Utils.mime_type(image_filename)

    case type
    when GIF, JPEG, PNG
      @image = Magick::Image.read(image_filename).first

    # TODO: add special support for TIFFs (not needed for digitool migration)

    when TIFF
      raise PackageError, "The Basic Image package #{@name} contains the TIFF file #{@datafiles[0]}, which is currently unsupported."
    else
      raise PackageError, "The Basic Image package #{@name} contains an unexpected file #{@datafiles[0]} with mime type #{type}."
    end

  end

end


class LargeImagePackage < Package

  # At this point we know we have a manifest, mods and maybe a marc file.

  def initialize config, directory, manifest
    super(config, directory, manifest)
  end

end

class PdfPackage < Package

  # At this point we know we have a manifest, mods and maybe a marc file.

  def initialize config, directory, manifest
    super(directory, manifest)
  end

end
