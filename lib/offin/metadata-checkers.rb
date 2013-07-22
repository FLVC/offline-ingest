require 'offin/exceptions'
require 'offin/errors'
require 'offin/manifest'
require 'offin/mods'

# The metadata updater class wraps behavior; we have packages arriving
# from many sources, and metadata may arrive embedded in a
# manifest.xml file, a MODS file, METS, MARC, DC, or defaults.
#
# Right now, we only have DigiTool-specific behaviours



class MetadataChecker

  include Errors

  def initialize manifest, mods, mets
    @manifest = manifest
    @mods = mods
    @mets = mets
  end

  def get_label fallback
    @manifest.label || @mods.title || fallback
  end

  def get_owner
    return 'fedoraAdmin'
  end

  def load_check
  end

end


class DigitoolMetadataChecker < MetadataChecker

  def initialize manifest, mods, mets
    super(manifest, mods, mets)
  end

  def load_check
    digitool_ids = @mods.digitool_ids
    error "No DigiTool ID present in MODS file." if digitool_ids.empty?
    error "Too many DigiTool IDs present in MODS file: #{digitool_ids.join(', ')}." if digitool_ids.length > 1
    error "The Digitool ID #{digitool_ids.first} from the MODS file is not numeric." if digitool_ids.first =~ /[^\d]/
  end

  def get_owner
    return 'Digitool Migration Assistant'
  end

end
