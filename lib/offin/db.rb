require 'data_mapper'
require 'dm-migrations'

module DataBase

  class IslandoraPackage
    include DataMapper::Resource

    property  :id,                Serial
    property  :package_name,      String,      :required => true, :index => true
    property  :islandora_pid,     String,      :required => true, :index => true
    property  :title,             String,      :length => 255,    :index => true
    property  :purl,              String
    property  :success,           Boolean
    property  :content_type,      String
    property  :started,           DateTime
    property  :finished,          DateTime
    property  :bytes,             Integer,     :min => 0, :max => 2**48, :default => 0

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
