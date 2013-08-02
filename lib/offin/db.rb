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

    # display support

    def islandora_link_base path, text, css = ''
      "<a #{css} href=\"http://#{self.islandora_site.hostname}/islandora/object/#{path}\">#{text}</a>"
    end

    def islandora_thumbnail_link  css = ''
      return if not self.success
      return "<img #{css} src=\"http://#{self.islandora_site.hostname}/islandora/object/#{self.islandora_pid}/datastream/TN/view\">"
    end

    # provide a link to the islandora page, if possible, otherwise just the package name

    def islandora_description_link text, css = ''
      return self.package_name unless self.success and self.islandora_pid
      return islandora_link_base(self.islandora_pid + '#tabs-2', text, css)
    end

    def islandora_summary_link text, css = ''
      return self.package_name unless self.success and self.islandora_pid
      return islandora_link_base(self.islandora_pid, text, css)
    end

    def digitool_link text, css = ''
      return unless self.digitool_id
      return "<a #{css} href=\"http://digitool.fcla.edu/R/?func=dbin-jump-full&object_id=#{self.digitool_id}\">#{text}</a>"
    end

    def islandora_collection_links title_mapping = {},  css = ''
      collections = self.get_collections
      return collections.map { |pid| islandora_link_base(pid, title_mapping[pid] ? title_mapping[pid] + " (#{pid})" : pid, css) }
    end

    # provide a 'drill-down' url with text 'success', 'warning', 'error' depending;  assumes a relative link

    def admin_status_url css = ''
      url = "<a #{css} href=\"#{self['id']}\">"
      return url + case
                   when (not get_errors.empty?);     'error</a>'
                   when (not get_warnings.empty?);   'warning</a>'
                   else;                             'success</a>'
                   end
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
    DataMapper.repository(:default).adapter.execute("ALTER TABLE islandora_packages ALTER time_started TYPE timestamp with time zone")
    DataMapper.repository(:default).adapter.execute("ALTER TABLE islandora_packages ALTER time_finished TYPE timestamp with time zone")
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
  end
end
