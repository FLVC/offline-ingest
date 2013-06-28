require 'data_mapper'
require 'dm-migrations'

module DataBase

  # need purl, size

  class IslandoraPackage
    include DataMapper::Resource

    property  :id,            Serial
    property  :package_name,  String,    :required => true, :index => true
    property  :islandora_pid, String,    :required => true, :index => true
    property  :success,       Boolean,   :default => true,  :index => true
    property  :content_type,  String
    property  :title,         String,    :length => 255,    :index => true
    property  :started,       DateTime
    property  :finished,      DateTime

    has n,  :warning_messages
    has n,  :error_messages

    def warning *messages
      return unless messages or messages.empty?
      messages.flatten.each do |str|
        self.warning_messages << WarningMessage.new(:text => str)
      end
    end


    def error *messages
      return unless messages or messages.empty?
      messages.flatten.each do |str|
        self.error_messages << ErrorMessage.new(:text => str)
      end
    end

  end

  class WarningMessage
    include DataMapper::Resource

    property    :id,      Serial
    property    :text,    Text,  :required => true

    belongs_to  :islandora_package
  end

  class ErrorMessage
    include DataMapper::Resource

    property    :id,      Serial
    property    :text,    Text,  :required => true

    belongs_to  :islandora_package
  end

  def self.setup config
    self.common config
  end

  def self.create config
    self.common config
    DataMapper.auto_migrate!
  end

  def self.common config
    DataMapper::Logger.new($stdout, :debug)
    DataMapper.setup(:default, config.database)

    repository(:default).adapter.resource_naming_convention = DataMapper::NamingConventions::Resource::UnderscoredAndPluralizedWithoutModule
    DataMapper.finalize
  end

end
