require 'offin/exceptions'
require 'offin/errors'
require 'offin/manifest'
require 'offin/mods'



# The metadata updater class wraps behavior; we have packages arriving
# from many sources, and metadata may arrive embedded in a
# manifest.xml file, a MODS file (e.g. digitool), METS, MARC, DC, or
# defaults (this is caused, at least in part, because librarians are
# playing at software design at FLVC).


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

  # Base classes must over-ride these. These are more for
  # documentation than expected to be executed, since base classes
  # would never call the unsupported methods.

  def set_label label
    raise PackageError, "Updating MODS <titleinfo> not implemented."
  end

  def set_identifiers *identifiers
    raise PackageError, "Updating MODS/DC identifiers not implemented."
  end

  # more of these...

end




class DigitoolMetadataUpdater < MetadataUpdater

  def initialize manifest, mods
    super(manifest, mods)
    sanity

  end

  # We assume digitool has a very restricted set of data in the MODS
  # and manifest.xml files.  This method checks those assumptions.

  def sanity

    # manifest is not expected to have: owner, other_logos, identifiers, or submitting_institution
    # if present, it would be ignored, so let's carp now

    # ....

    # manifest must have non-empty collections and a owning_institution. It may have a submitting_institution.

    # ....

    # mods must not have an <extension> section....

  end


  def set_idenfifiers *identifiers
    raise "TODO"
  end

  def set_label label
    raise "TODO"
  end


  def get_extension
    return <<-XML.gsub(/^    /, '')
    <extension xmlns="info:/flvc/manifest/v1">
      <owningInstitution>#{@manifest.owning_institution}</owningInstitution>
      <submittingInstitution>#{@manifest.submitting_institution || @manifest.owning_institution}</submittingInstitution>
    </extension>
    XML
  end

  def get_owner
    'Digitool Migration Assistant'
  end

end
