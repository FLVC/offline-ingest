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

  def post_initialization                               # TODO: rename to pre_ingest
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
      package.error "#{self.class} can't determine owner for this package (this is normally the 'owningUser' element in the manifest.xml file. 'owningUser' refers to a valid drupal account)."
    end
  end

  def post_ingest
    super
    return unless package.valid?   # should not have to do this...

    purl_server = Purlz.new(package.config.purl_server, package.config.purl_administrator, package.config.purl_password)

    package.purls.each do |purl|

      puri = URI.parse(purl)

      unless ALLOWED_PURL_SERVERS.include? puri.host.downcase
        package.error "The server for PURL #{purl} must be one of #{ALLOWED_PURL_SERVERS.inspect}."
        next
      end

      if purl_server.tombstoned?(puri.path)
        package.error "The PURL #{purl} was tombstoned and cannot be recreated."
        next
      end

      if puri.path =~ /^\/fcla\//i
        package.error "PURL #{puri} is using 'fcla' as the domain (first component) of the PURL #{purl}, which is not supported."
        next
      end

      institution_code = package.owning_institution.downcase

      if puri.path !~ /^\/#{institution_code}\//i
        package.error "PURL #{purl} must have the owning institution code as the domain (first component) of the PURL path: http://#{puri.host}/#{institution_code}/...."
        next
      end

      target      = sprintf("http://%s/islandora/object/%s", package.config.site,  package.pid)
      maintainers = [ 'flvc', institution_code ]

      if data = purl_server.get(puri.path)
        pre_existing_maintainers = data[:uids] + data[:gids]
        potential_surprises = pre_existing_maintainers.map{ |x| x.strip.downcase } - maintainers.map{ |y| y.strip.downcase }
        potential_surprises.delete_if { |x| x == "admin" }  # commonly happens, don't worry about this one
        package.warning "When creating prospective PURL #{purl}, found an existing purl. Keeping existing maintainers #{potential_surprises.inspect})." unless potential_surprises.empty?
        maintainers += pre_existing_maintainers
      end

      # The purl server we use is from config.purl_server above, which may not actually match the purl from the package metadata.

      unless purl_server.set(puri.path, target, *maintainers)
        package.error "PURL #{purl} with owning institution #{package.owning_institution} and target #{target} could not be created (perhaps owning institution is not a purl group for this server?)."
        next
      end

    end  # of package.purls.each do |purl|

    raise PackageError, "Failure in post-ingest PURL creation." unless package.valid?

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
