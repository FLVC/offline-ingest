require 'offin/exceptions'
require 'offin/errors'
require 'offin/manifest'
require 'offin/mods'


# The metadata updater class wraps behavior; we have packages arriving
# from many sources, and metadata may arrive embedded in a
# manifest.xml file, a MODS file, METS, MARC, DC or other set.   This
# class is meant to organize such behavoir.
#


# Currently we have digitool-specified behavoirs and a slightly less
# restrictive prospective set.


class MetadataChecker

  include Errors

  def initialize package
    @package = package
  end

  def post_initialization
    if @package.manifest and @package.manifest.label
      @package.label = @package.manifest.label

    elsif @package.mods and @package.mods.title
      @package.label = @package.mods.title[0,255]

    elsif @package.directory_name
      @package.label = @package.directory_name

    else
      @package.error "#{self.class} can't determine label for this package."
    end
  end

  def post_ingest
  end

end


class ProspectiveMetadataChecker < MetadataChecker


  def initialize package
    super(package)
  end

  def post_initialization
    super

    if (@package.manifest and @package.manifest.owning_user)
      @package.owner = @package.manifest.owning_user
    else
      @package.error "#{self.class} can't determine owner for this package."
    end
  end


end



class DigitoolMetadataChecker < MetadataChecker

  def initialize package
    super(package)
  end

  def post_initialization
    super

    digitool_ids = @package.mods.digitool_ids
    @package.error "No DigiTool ID present in MODS file." if digitool_ids.empty?
    @package.error "Too many DigiTool IDs present in MODS file: #{digitool_ids.join(', ')}." if digitool_ids.length > 1
    @package.error "The Digitool ID #{digitool_ids.first} from the MODS file is not numeric." if digitool_ids.first =~ /[^\d]/

    purls  = @package.mods.purls
    if purls.empty?
      @package.error "The MODS file in package #{@package.directory_name} does not have a PURL declaration: at least one is required."
    else
      @package.purls = purls
    end

    @package.owner = 'Digitool Migration Assistant'
  end


end
