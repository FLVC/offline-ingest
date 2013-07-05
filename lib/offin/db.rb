require 'data_mapper'
require 'dm-migrations'

module DataBase


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

    property  :islandora_pid,     String,      :index => true
    property  :title,             String,      :length => 255,    :index => true
    property  :content_type,      String,      :index => true


    has n,  :warning_messages
    has n,  :error_messages
    has n,  :purls

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
    property    :purl,    Text,  :required => true

    belongs_to  :islandora_package
  end

  def self.setup config
    DataMapper::Logger.new($stdout, :debug)
    DataMapper.setup(:default, config.database)

    repository(:default).adapter.resource_naming_convention = DataMapper::NamingConventions::Resource::UnderscoredAndPluralizedWithoutModule
    DataMapper.finalize
  end

  def self.create config
    self.setup config
    DataMapper.auto_migrate!
  end
end
