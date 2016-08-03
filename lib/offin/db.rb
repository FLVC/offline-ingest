require 'data_mapper'
require 'dm-migrations'
require 'dm-types'
require 'time'
require 'offin/exceptions'

# A datamapper-based module for ingest admininstrative data.  It is
# used by ingest scripts and the admin web service. See
# http://datamapper.org/docs/ for a tutorial on using datamapper.

module DataBase

  @@debug = false

  FEDORA_INFO_REGEXP = /^info:fedora\//i


  class IslandoraSite
    include DataMapper::Resource

    property  :id,          Serial
    property  :hostname,    String,   :required => true, :index => true, :unique => true;

    has n, :islandora_packages
    has n, :ftp_packages
    has n, :ftp_containers

    before :update do
      self.hostname.downcase!
    end

    before :save do
      self.hostname.downcase!
    end
  end


  class FtpPackage
    include DataMapper::Resource

    property :id,                  Serial
    property :time_submitted,      DateTime,    :required => true, :index => true
    property :time_processed,      DateTime,    :required => true, :index => true

    property :package_name,        String
    property :status,              Enum[ :error, :warning, :success ]

    belongs_to :islandora_site;
  end

  class FtpContainer
    include DataMapper::Resource

    property :id,                 Serial
    property :container_name,     String

    belongs_to :islandora_site

    def self.next_container_name hostname
      rec = first_or_create(:islandora_site => DataBase::IslandoraSite.first_or_create(:hostname => hostname))
      if not rec.container_name
        rec.container_name = 'aaaa'
      else
        rec.container_name = rec.container_name.succ
      end
      rec.save
      return rec.container_name
    end

  end


  class IslandoraPackage
    include DataMapper::Resource

    property  :id,                Serial

    property  :package_name,      String,      :required => true, :index => true
    property  :time_started,      DateTime,    :required => true, :index => true
    property  :time_finished,     DateTime,    :required => true, :index => true
    property  :success,           Boolean,     :required => true, :index => true, :default => false
    property  :bytes_ingested,    Integer,     :default => 0, :index => true, :min => 0, :max => 2**48

    # NULL means inapplicable for these (e.g., it was never ingested, or there was no content_type declared, etc)

    property  :digitool_id,       Integer,     :min => 0, :max => 2**48, :index => true
    property  :islandora_pid,     String,      :index => true
    property  :title,             String,      :length => 255,    :index => true
    property  :content_model,     String,      :length => 255,    :index => true

    has n,  :warning_messages
    has n,  :error_messages
    has n,  :purls
    has n,  :islandora_collections
    has n,  :component_objects

    belongs_to  :islandora_site

    def iid
      self.package_name
    end

    def content_model_title
      return case self.content_model
             when  "islandora:sp_basic_image";        "Basic Image"
             when  "islandora:sp_large_image_cmodel"; "Large Image"
             when  "islandora:sp_pdf";                "PDF"
             when  "islandora:bookCModel";            "Book"
             when  "islandora:pageCModel";            "Page"
             when  "islandora:newspaperIssueCModel";  "Newspaper Issue"
             else;                                     self.content_model
             end
    end

    def add_warnings *messages
      return unless messages or messages.empty?
      messages.flatten.each do |str|
        self.warning_messages << WarningMessage.new(:warning => str)
      end
    end

    def get_warnings
      self.warning_messages.map { |rec| rec.warning }
    end

    def warnings?
      not self.warning_messages.empty?
    end

    def add_errors *messages
      return unless messages or messages.empty?
      messages.flatten.each do |str|
        self.error_messages << ErrorMessage.new(:error => str)
      end
    end

    def get_errors
      self.error_messages.map { |rec| rec.error }
    end

    def errors?
      not self.error_messages.empty?
    end

    def add_purls *urls
      return unless urls or urls.empty?
      urls.flatten.each do |str|
        self.purls << Purl.new(:purl => str)
      end
    end

    def get_purls
      self.purls.map { |rec| rec.purl }
    end

    def add_collections *collections
      return unless collections or collections.empty?
      collections.flatten.each do |str|
        self.islandora_collections << IslandoraCollection.new(:collection_code => str.sub(FEDORA_INFO_REGEXP, ''))
      end
    end

    def get_collections
      self.islandora_collections.map { |rec| rec.collection_code }
    end

    def add_components *pids
      return unless pids or pids.empty?
      pids.flatten.each do |str|
        self.component_objects << ComponentObject.new(:pid => str.sub(FEDORA_INFO_REGEXP, ''))
      end
    end

    def get_components
      self.component_objects.map { |rec| rec.pid }
    end

  end # of class IslandoraPackage


  class IslandoraCollection
    include DataMapper::Resource

    property    :id,                Serial
    property    :collection_code,   String,  :required => true

    belongs_to  :islandora_package


    after :create do
      self.collection_code.sub!(FEDORA_INFO_REGEXP, '')
    end

    before :update do
      self.collection_code.sub!(FEDORA_INFO_REGEXP, '')
    end

    before :save do
      self.collection_code.sub!(FEDORA_INFO_REGEXP, '')
    end

  end

  class WarningMessage
    include DataMapper::Resource

    property    :id,         Serial
    property    :warning,    Text,  :required => true

    belongs_to  :islandora_package
  end

  class ErrorMessage
    include DataMapper::Resource

    property    :id,       Serial
    property    :error,    Text,  :required => true

    belongs_to  :islandora_package
  end

  class Purl
    include DataMapper::Resource

    property    :id,      Serial
    property    :purl,    String,  :length => 128, :required => true

    belongs_to  :islandora_package
  end

  class ComponentObject
    include DataMapper::Resource

    property    :id,      Serial
    property    :pid,     String,  :required => true

    belongs_to  :islandora_package
  end

  # Set to TRUE before setup if you want this:

  def self.debug= bool
    @@debug = bool
  end

  def self.setup config
    DataMapper::Logger.new($stderr, :debug)  if @@debug
    dm = DataMapper.setup(:default, config.ingest_database)

    repository(:default).adapter.resource_naming_convention = DataMapper::NamingConventions::Resource::UnderscoredAndPluralizedWithoutModule
    DataMapper.finalize

    # ping database so we can fail fast

    return dm.select('select 1 + 1') == [ 2 ]
  end

  # Warning: deletes all data in database.

  def self.create config
    self.setup config
    DataMapper.auto_migrate!
    DataMapper.repository(:default).adapter.execute("ALTER TABLE islandora_packages ALTER time_started TYPE timestamp with time zone")
    DataMapper.repository(:default).adapter.execute("ALTER TABLE islandora_packages ALTER time_finished TYPE timestamp with time zone")
    DataMapper.repository(:default).adapter.execute("ALTER TABLE ftp_packages ALTER time_submitted TYPE timestamp with time zone")
    DataMapper.repository(:default).adapter.execute("ALTER TABLE ftp_packages ALTER time_processed TYPE timestamp with time zone")
  end

  # just add the new ftp tables (TODO: build in support for this for dashboard)

  def self.add_ftp_tables config
    self.setup config
    FtpPackage.auto_migrate!
    FtpContainer.auto_migrate!
    DataMapper.repository(:default).adapter.execute("ALTER TABLE ftp_packages ALTER time_submitted TYPE timestamp with time zone")
    DataMapper.repository(:default).adapter.execute("ALTER TABLE ftp_packages ALTER time_processed TYPE timestamp with time zone")
  end

  def self.dump
    packages = DataBase::IslandoraPackage.all(:order => [ :time_started.desc ])
    packages.each do |p|

      puts "[#{p.time_started.strftime('%c')}] #{p.package_name} => #{p.islandora_pid or 'n/a'} #{p.islandora_site.hostname}//#{p.get_collections.join(',')} #{p.success ? 'succeeded' : 'failed'}"

      errors   = p.get_errors.map   { |m| ' * ' + m }
      warnings = p.get_warnings.map { |m| ' * ' + m }

      puts "Purls: #{p.get_purls.join(', ')};  DigiTool ID: #{p.digitool_id}"

      puts "Errors: ",   errors   unless errors.empty?
      puts "Warnings: ", warnings unless warnings.empty?

      puts ''
    end

  end # of class component object
end   # of module database



# The FtpPackage logs information


module DataBase


end # of module Database
