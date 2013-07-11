require 'data_mapper'
require 'dm-migrations'
require 'time'

module DataBase

  @@debug = false

  FEDORA_INFO_REGEXP = /^info:fedora\//i

  class IslandoraSite
    include DataMapper::Resource

    property  :id,          Serial
    property  :hostname,    String,   :required => true, :index => true, :unique => true;

    has n, :islandora_packages

    before :update do
      self.hostname.downcase!
    end

    before :save do
      self.hostname.downcase!
    end

  end


  class IslandoraPackage
    include DataMapper::Resource

    property  :id,                Serial

    property  :package_name,      String,      :required => true, :index => true
    property  :success,           Boolean,     :required => true, :index => true, :default => false
    property  :time_started,      Integer,     :min => 0, :max => 2**48, :required => true, :index => true
    property  :time_finished,     Integer,     :min => 0, :max => 2**48, :required => true, :index => true

    property  :bytes_ingested,    Integer,     :min => 0, :max => 2**48, :default => 0, :index => true

    # NULL means inapplicable for these (e.g., it was never ingested, or there was no content_type declared, etc)

    property  :iid,               String,      :index => true
    property  :islandora_pid,     String,      :index => true
    property  :title,             String,      :length => 255,    :index => true
    property  :content_model,     String,      :length => 255,    :index => true

    has n,  :warning_messages
    has n,  :error_messages
    has n,  :purls
    has n,  :islandora_collections
    has n,  :component_objects

    belongs_to  :islandora_site

    def add_warnings *messages
      return unless messages or messages.empty?
      messages.flatten.each do |str|
        self.warning_messages << WarningMessage.new(:warning => str)
      end
    end

    def get_warnings
      self.warning_messages.map { |rec| rec.warning }
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
  end


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
    dm = DataMapper.setup(:default, config.database)

    repository(:default).adapter.resource_naming_convention = DataMapper::NamingConventions::Resource::UnderscoredAndPluralizedWithoutModule
    DataMapper.finalize

    # ping database

    return dm.select('select 1 + 1') == [ 2 ]
  rescue => e
    raise SystemError, "Fatal error: can't connect to database: #{e.class}: #{e.message}"
  end

  def self.create config
    self.setup config
    DataMapper.auto_migrate!
  end


  def self.dump
    packages = DataBase::IslandoraPackage.all(:order => [ :time_started.desc ])
    packages.each do |p|


      puts "#{Time.at(p.time_started).to_s.sub(/\s+[-+]\d+$/, '')} #{p.package_name} => #{p.islandora_site.hostname}:(#{p.get_collections.join(', ')}) #{p.success ? 'succeeded' : 'failed'}"

      errors   = p.get_errors.map   { |m| ' * ' + m }
      warnings = p.get_warnings.map { |m| ' * ' + m }

      puts "Errors: ",   errors   unless errors.empty?
      puts "Warnings: ", warnings unless warnings.empty?

      puts ''
    end
  end



end
