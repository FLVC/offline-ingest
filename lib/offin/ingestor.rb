require 'rubydora'
require 'offin/document-parsers'
require 'offin/mods'

class Ingestor

  # TODO: sanity check on config object, throw error that should stop all processing
  # TODO: this should get an already vetted repo and config, and be focused on one object
  # TODO: error handling for: repository (can't connect?, etc);  create (???); ....
  # TODO: stash error and warning messages

  attr_reader :repository, :pid, :namespace, :object, :collection

  def initialize  config
    @config = config

    @repository = Rubydora.connect :url => @config.url, :user => @config.user, :password => @config.password
    @namespace = @config.namespace
    @pid = getpid
    @object = @repository.create(@pid)

    # we set these later...

    @image  = nil
    @mods   = nil
    @label  = nil
    @dc     = nil
  end

  def getpid
    sax_document = SaxDocumentGetNextPID.new
    Nokogiri::XML::SAX::Parser.new(sax_document).parse(@repository.next_pid(:namespace => @namespace))
    return sax_document.pids.shift
  end

  def add_basic_image filename
    @image = Magick::Image.read(filename).first
  end

  def add_mods filename
    @mods = File.read(filename)
  end

  def add_dc content
    @dc = content
  end

  # def add_mods filename_or_data
  #   if File.exist? filename_or_data
  #     @mods = File.read(filename_or_data)
  #   else
  #     @mods = filename_or_data
  #   end
  # end

  ### TODO: check where used and xml-ify it if necessary.  Need to make sure it's UTF-8 as well.

  def add_label text
    @label = text
  end


  # def dc_text
  #   return <<-EOF.gsub(/^    /, '')
  #   <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
  #     <dc:title>#{@label}</dc:title>
  #     <dc:identifier>#{@pid.sub('info:fedora/','')}</dc:identifier>
  #   </oai_dc:dc>
  # EOF
  # end

  # ingest the object into the collection indicated by the collection object

  def ingest collection
    raise "You haven't set the DC yet - use add_dc(data)" unless @dc
    raise "You haven't set the MODS file yet - use add_mods(filename)" unless @mods
    raise "You haven't set the image file yet - use add_basic_image(filename)" unless @image
    raise "You haven't set the label yet - use add_label(filename)" unless @label

    object.memberOfCollection << collection.pid
    object.models << 'info:fedora/islandora:sp_basic_image'
    object.label = @label
    object.ownerId = @config.object_owner

    ds = object.datastreams['DC']
    ds.dsLabel  = "Dublin Core Record"
    ds.content  = @dc
    ds.mimeType = 'text/xml'

    ds = object.datastreams['MODS']
    ds.dsLabel  = "MODS Record"
    ds.content  = @mods
    ds.mimeType = 'text/xml'

    ds = object.datastreams['OBJ']
    ds.dsLabel  = @label
    ds.content  = @image
    ds.mimeType = @image.mime_type

    # TODO: check to make sure we're not expanding image sizes here

    ds = object.datastreams['TN']
    ds.dsLabel  = "Thumbnail Image"
    ds.content  = @image.change_geometry(@config.thumbnail_geometry) { |cols, rows, img| img.resize(cols, rows) }
    ds.mimeType = @image.mime_type

    ds = object.datastreams['MEDIUM_SIZE']
    ds.dsLabel  = "Medium Size Image"
    ds.content  = @image.change_geometry(@config.medium_geometry) { |cols, rows, img| img.resize(cols, rows) }
    ds.mimeType = @image.mime_type

    # TODO: DC transform
    object.save

  end

end # of class Ingestor
