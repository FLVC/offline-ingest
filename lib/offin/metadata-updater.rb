require 'offin/exceptions'
require 'offin/manifest'
require 'offin/mods'


# we'll need various of these eventually,  but right now this one for digitoool suffices


class MetadataUpdater

  def initialize manifest, mods

    raise PackageError, "MetadataUpdater: invalid manifest." unless manifest.valid?
    raise PackageError, "MetadataUpdater: invalid MODS."     unless mods.valid?

    @manifest = manifest
    @mods = mods

  end

  # digitool always has Title?

  def get_label fallback
    manifest.label || mods.title || fallback
  end

  def set_label label
    raise PackageError, "Updating MODS <titleinfo> not yet implemented."
    mods.title = label
  end

  def set_identifiers *identifiers
    raise PackageError, "Updating MODS/DC identifiers not yet implemented."
    mods.identifiers << identifiers
  end


  # TODO: add otherLogo.

  def get_extension
    return <<-XML.gsub(/^    /, '')
    <extension xmlns="info:/flvc/manifest/v1">
      <owningInstitution>#{@manifest.owning_institution}</owningInstitution>
      <submittingInstitution>#{@manifest.submitting_institution || @manifest.owning_institution}</submittingInstitution>
    </extension>
    XML
  end
end




class DigitoolMetadataUpdater < MetadataUpdater

  def initialize manifest, mods
    super(manifest, mods)
  end

end
