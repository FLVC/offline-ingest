require 'offin/exceptions'
require 'offin/errors'
require 'offin/manifest'
require 'offin/mods'
require 'purlz'


# The metadata updater class wraps behavior; we have packages arriving
# from many sources, and metadata may arrive embedded in a
# manifest.xml file, a MODS file, METS, MARC, DC or other set.   This
# class is meant to organize such behavoir.



# Currently we have digitool-specified behavoirs and a slightly less
# restrictive prospective set.


class MetadataChecker

  include Errors

  attr_reader :package

  def initialize package
    @package = package
  end

  def post_initialization
    if package.manifest and package.manifest.label
      package.label = package.manifest.label

    elsif package.mods and package.mods.title
      package.label = package.mods.title[0,255]

    elsif package.directory_name
      package.label = package.directory_name

    else
      package.error "#{self.class} can't determine label for this package."
    end
  end

  def post_ingest
  end

end


class ProspectiveMetadataChecker < MetadataChecker
  ALLOWED_PURL_SERVERS = [ 'purl.flvc.org', 'purl.fcla.edu' ]   # presumably, @config.purl_server maps to one of these, but it doesn't have to


  def initialize package
    super(package)
  end

  def post_initialization   # TODO: rename to pre_ingest
    super

    if (package.manifest and package.manifest.owning_user)
      package.owner = package.manifest.owning_user
    else
      package.error "#{self.class} can't determine owner for this package."
    end
  end


  def post_ingest
    super
    return unless package.valid?   # should not have to do this...

    purl_server = Purlz.new(package.config.purl_server, package.config.purl_administrator, package.config.purl_password)

    package.purls.each do |purl|

      puri = URI.parse(purl)

      unless ALLOWED_PURL_SERVERS.include? puri.host.downcase
        package.error "PURL was #{purl}, but the server must be one of #{ALLOWED_PURL_SERVERS}."
      end

      target = sprintf("http://%s/islandora/object/%s", package.config.site,  package.pid)

      # The purl server we use is from config.purl_server above, which may not actuually match the purl from the package metadata.

      result = purl_server.set(puri.path, target, 'flvc', 'fcla', package.owning_institution.downcase)

      unless result
        package.error "PURL #{purl} with owning_institution #{package.owning_institution} and target #{target} could not be created (perhaps bad owning_institution or tombstoned purl?)."
      end
    end

    raise PackageError "Failure in post-ingest PURL creation." unless package.valid?

    # TODO: over time, classify errors returned and sort out which
    # should be system errors (e.g. network connection to purl server
    # failed) or package errors.

  rescue PackageError => e
    package.error e.message
    raise e

  rescue => e
    package.error "Failure in post-ingest PURL creation: #{e.message}", e.backtrace
    raise PackageError, e.message
  end

end



class DigitoolMetadataChecker < MetadataChecker

  def initialize package
    super(package)
  end

  def post_initialization
    super

    digitool_ids = package.mods.digitool_ids
    package.error "No DigiTool ID present in MODS file." if digitool_ids.empty?
    package.error "Too many DigiTool IDs present in MODS file: #{digitool_ids.join(', ')}." if digitool_ids.length > 1
    package.error "The Digitool ID #{digitool_ids.first} from the MODS file is not numeric." if digitool_ids.first =~ /[^\d]/

    if package.mods.purls.empty?
      package.error "The MODS file in package #{package.directory_name} does not have a PURL declaration: at least one is required."
    end

    package.owner = 'Digitool Migration Assistant'
  end
end
