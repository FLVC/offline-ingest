require 'offin/exceptions'
require 'offin/errors'
require 'offin/manifest'
require 'offin/mods'

# The metadata updater class wraps behavior; we have packages arriving
# from many sources, and metadata may arrive embedded in a
# manifest.xml file, a MODS file, METS, MARC, DC, or defaults.

#  This whole idea may turn out to be unnecessary, we'll see...


class MetadataUpdater

  include Errors

  def initialize manifest, mods

    raise PackageError, "MetadataUpdater: invalid manifest." unless manifest.valid?
    raise PackageError, "MetadataUpdater: invalid MODS."     unless mods.valid?

    @manifest = manifest
    @mods = mods
  end

  def get_label fallback
    @manifest.label || @mods.title || fallback
  end
end




class DigitoolMetadataUpdater < MetadataUpdater

  def initialize manifest, mods
    super(manifest, mods)
  end

  def get_owner
    'Digitool Migration Assistant'
  end

end
