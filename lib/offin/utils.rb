# This file is like that kitchen drawer with the assorted unsortables.

# TODO: the image processing, especially the jp2k stuff, needs an
# overhaul. openjpeg in particular seems able to handle problematic
# jp2k files better, as a last resort.

require 'rubydora'
require 'fileutils'
require 'iconv'
require 'offin/exceptions'
require 'offin/config'
require 'open3'
require 'stringio'
require 'tempfile'
require 'timeout'
require 'restclient'
require 'uri'
require 'nokogiri'
require 'time'

begin
  warn_level = $VERBOSE
  $VERBOSE = nil
  require 'offin/sql-assembler'  # requires data_mapper,  which redefines CSV, which complains.
ensure
  $VERBOSE = warn_level
end

class Utils

  # make this conditional

  QDATATMP = '/qdata/tmp/'

  TESSERACT_TIMEOUT = 500   # tesseract can waste a lot of time on certain kinds of images
  QUICKLY_TIMEOUT   = 30    # seconds before giving up on fedora

  # Yuck - these don't really belong here... (copied from packages.rb)

  BASIC_IMAGE_CONTENT_MODEL      = 'islandora:sp_basic_image'
  LARGE_IMAGE_CONTENT_MODEL      = 'islandora:sp_large_image_cmodel'
  PDF_CONTENT_MODEL              = 'islandora:sp_pdf'
  BOOK_CONTENT_MODEL             = 'islandora:bookCModel'
  PAGE_CONTENT_MODEL             = 'islandora:pageCModel'
  NEWSPAPER_CONTENT_MODEL        = 'islandora:newspaperCModel'
  NEWSPAPER_ISSUE_CONTENT_MODEL  = 'islandora:newspaperIssueCModel'
  NEWSPAPER_PAGE_CONTENT_MODEL   = 'islandora:newspaperPageCModel'


  def Utils.tempdir()
    return File.exists?(QDATATMP) ? QDATATMP : '/tmp/'
  end


  def Utils.ingest_usage
    program = $0.sub(/.*\//, '')
    STDERR.puts "Usage: #{program} <directory>"
    exit 1
  end

  def Utils.xml_unescape str
    return str.gsub('&lt;', '<').gsub('&gt;', '>').gsub('&amp;', '&').gsub('&apos;', "'").gsub(/\&\#([0-9]+);/) { |i| $1.to_i.chr }
  end

  # Don't use this for escaping attribute data; that requires '"' => '&quot;'

  def Utils.xml_escape str
    return str.gsub('&', '&amp;').gsub("'", '&apos;').gsub('<', '&lt;').gsub('>', '&gt;')
  end


  # For creating a solr query string, we need to escape some characters with "\".

  def Utils.solr_escape str
    escaped = str.dup
    chars = [ '\\', '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '~', '*', '?', ':', '"', ';', ' ' ]
    chars.each { |c| escaped.gsub!(c, '\\' + c) }
    return escaped
  end

  # This is mostly to silence the "require 'datamapper'" that causes the annoying warning "CSV constant redefined".

  def Utils.silence_warnings(&block)
    warn_level = $VERBOSE
    $VERBOSE = nil
    result = block.call
  ensure
    $VERBOSE = warn_level
    result
  end

  def Utils.field_system_error message = ''
    yield
  rescue => e
    raise SystemError,  "#{e.class} - #{message}: #{e.message}"  # ick
  end


  # NOTE: this doesn't play well with the process monitor's logger
  # when the stream used for that logging is silenced below; the
  # process monitor never gets the original on reopen.

  def Utils.silence_streams(*streams)  # after some rails code by DHH

    on_hold = streams.collect { |stream| stream.dup }
    streams.each do |stream|
      stream.reopen('/dev/null')
      stream.sync = true
    end
    yield

  ensure
    streams.each_with_index do |stream, i|
      stream.reopen(on_hold[i])
    end
  end

  def Utils.pretty_elapsed elapsed
    hours, elapsed = elapsed / 3600, elapsed % 3600
    minutes, seconds  = elapsed / 60, elapsed % 60

    texts = []
    texts.push  "#{hours} hour#{ hours == 1 ? '' : 's'}"       if hours > 0
    texts.push  "#{minutes} minute#{ minutes == 1 ? '' : 's'}" if minutes > 0
    texts.push  "#{seconds} second#{ seconds == 1 ? '' : 's'}" if seconds > 0

    return '0 seconds' if texts.empty?
    return texts.join(', ')
  end

  def Utils.quickly time = QUICKLY_TIMEOUT
    Timeout.timeout(time) do
      yield
    end
  rescue Timeout::Error => e
    raise "Timed out after #{time} seconds"
  end

  def Utils.get_pre_existing_islandora_pid_for_iid config, iid

    # we check solr to see if this iid has already been assigned.
    # we'll get and parse a document as follows if we get a hit.
    #
    #
    # <?xml version="1.0" encoding="UTF-8"?>
    # <response>
    # <lst name="responseHeader">
    #   <int name="status">0</int>
    #   <int name="QTime">0</int>
    #   <lst name="params">
    #     <str name="indent">on</str>
    #     <str name="version">2.2</str>
    #     <str name="fl">PID,mods_identifier_iid_ms</str>
    #     <str name="q">mods_identifier_iid_ms:FSDT2854731</str>
    #   </lst>
    # </lst>
    # <result name="response" numFound="1" start="0">
    #   <doc>
    #     <str name="PID">fsu:122</str>
    #     <arr name="mods_identifier_iid_ms"><str>FSDT2854731</str></arr>
    #   </doc>
    # </result>
    # </response>

    # return if config.testing

    return if config.test_mode and not config.solr_url   # user specified testing mode without specifying server - technicaly OK?

    url = "#{config.solr_url}/select/?q=mods_identifier_iid_mls:#{Utils.solr_escape(iid)}&version=2.2&indent=on&fl=PID,mods_identifier_iid_ms"
    uri = URI.encode(url)
    doc = quickly { RestClient.get(uri) }
    xml = Nokogiri::XML(doc)

    element = xml.xpath("//result/doc/str[@name='PID']")[0]
    return element.child.text if element and element.child
    return nil

  rescue RestClient::Exception => e
    raise SystemError, "Can't obtain IID from solr at '#{url}': #{e.class} #{e.message}"

  rescue => e
    raise SystemError, "Can't process IID obtained from solr at '#{url}': : #{e.class} #{e.message}"
  end


  # Return a mapping from short islandora PIDs (e.g. fsu:foobar, not info:fedora/fsu:foobar) and their titles.

  def Utils.get_collection_names config
    query = "SELECT DISTINCT ?object ?title FROM <#ri> " +
            " WHERE { ?object <fedora-model:label> ?title ;" +
            "                 <fedora-model:hasModel> <info:fedora/islandora:collectionCModel> ;" +
            "                 <fedora-model:state> <fedora-model:Active> . }"

    repository = ::Rubydora.connect :url => config.fedora_url, :user => config.user, :password => config.password

    quickly do
      repository.ping
    end

    hash = {}
    repository.sparql(query).each do |x|
      hash[x[0].sub('info:fedora/', '')] = x[1]
    end
    return hash
  rescue => e
    return {}
  end

  # This is a placeholder for a useful SPARQL query as an example - we
  # actually use trimmed down specialized derivatives of this. Given a
  # newspaper_pid, return all the issues:
  #
  #     PREFIX islandora-rels-ext: <http://islandora.ca/ontology/relsext#>
  #     PREFIX fedora-rels-ext: <info:fedora/fedora-system:def/relations-external#>
  #
  #     SELECT ?object ?sequence ?label ?issued
  #     FROM <#ri>
  #     WHERE {
  #       ?object fedora-rels-ext:isMemberOf <info:fedora/#{newspaper_pid.sub(/^info:fedora\//, '')}> ;
  #            <fedora-model:hasModel> <info:fedora/islandora:newspaperIssueCModel> ;
  #            <fedora-model:label> ?label .
  #       ?object islandora-rels-ext:isSequenceNumber ?sequence
  #       OPTIONAL { ?object islandora-rels-ext:dateIssued ?issued }
  #     }
  #     ORDER BY ?sequence
  #
  # which returns data like:
  #
  # <CSV::Row "object":"info:fedora/fsu:162918" "sequence":"360" "label":"secolo" "issued":"1885-05-06">
  # <CSV::Row "object":"info:fedora/fsu:162926" "sequence":"361" "label":"secolo" "issued":"1885-05-07">


  def Utils.get_next_newspaper_issue_sequence config, newspaper_pid
    query = <<-SPARQL.gsub(/^        /, '')
        PREFIX islandora-rels-ext: <http://islandora.ca/ontology/relsext#>
        PREFIX fedora-rels-ext: <info:fedora/fedora-system:def/relations-external#>

        SELECT ?object ?sequence
        FROM <#ri>
        WHERE {
          ?object fedora-rels-ext:isMemberOf <info:fedora/#{newspaper_pid.sub(/^info:fedora\//, '')}> ;
               <fedora-model:hasModel> <info:fedora/islandora:newspaperIssueCModel> .
          ?object islandora-rels-ext:isSequenceNumber ?sequence
        }
        ORDER BY ?sequence
    SPARQL

    repository = ::Rubydora.connect :url => config.fedora_url, :user => config.user, :password => config.password

    quickly do
      repository.ping
    end

    # The sparql query returns a (possibly empty list) of rows along the lines of
    #
    # #<CSV::Row "object":"info:fedora/fsu:161312" "sequence":"245">
    # #<CSV::Row "object":"info:fedora/fsu:161320" "sequence":"246">
    # #<CSV::Row "object":"info:fedora/fsu:161328" "sequence":"247">


    last =  repository.sparql(query).map { |row_rec| row_rec['sequence'].to_i }.max

    return last ? last + 1 : 1

  rescue => e
    return
  end

  def Utils.get_newspaper_pids config

    query = <<-SPARQL.gsub(/^        /, '')
        SELECT ?object
        FROM <#ri>
        WHERE { ?object <fedora-model:hasModel> <info:fedora/islandora:newspaperCModel> }
        ORDER BY ?object
    SPARQL

    repository = ::Rubydora.connect :url => config.fedora_url, :user => config.user, :password => config.password

    quickly do
      repository.ping
    end

    return repository.sparql(query).map { |row_rec| row_rec['object'].sub('info:fedora/', '') }
  end


  # get_datastream_names(config, islandora_pid) => hash
  #
  # parse XML for dsid/label pairs, as from this example document:
  #
  # <?xml version="1.0" encoding="UTF-8"?>
  # <objectDatastreams xmlns="http://www.fedora.info/definitions/1/0/access/"
  #                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  #                    xsi:schemaLocation="http://www.fedora.info/definitions/1/0/access/
  #                                        http://www.fedora-commons.org/definitions/1/0/listDatastreams.xsd"
  #                    pid="fsu:1775" baseURL="http://islandorad.fcla.edu:8080/fedora/">
  #   <datastream dsid="DC" label="Dublin Core Record" mimeType="text/xml"/>
  #   <datastream dsid="RELS-EXT" label="Relationships" mimeType="application/rdf+xml"/>
  #   <datastream dsid="MODS" label="MODS Record" mimeType="text/xml"/>
  #   <datastream dsid="MARCXML" label="Archived MarcXML" mimeType="text/xml"/>
  #   <datastream dsid="TN" label="Thumbnail" mimeType="image/jpeg"/>
  #   <datastream dsid="DT-METS" label="Archived METS for future reference" mimeType="text/xml"/>
  #   <datastream dsid="TOC" label="Table of Contents" mimeType="application/json"/>
  # </objectDatastreams>
  #
  # The above doc would return a hash of strings (key/value pairs):
  #
  #   DC       =>  Dublin Core Record
  #   RELS-EXT =>  Relationships
  #   MODS     =>  MODS Record
  #   MARCXML  =>  Archived MarcXML
  #   TN       =>  Thumbnail
  #   DT-METS  =>  Archived METS for future reference
  #   TOC      =>  Table of Contents

  def Utils.get_datastream_names config, pid

    doc = quickly do
      url = URI.encode("#{config.fedora_url}/objects/#{pid.sub('info:fedora/', '')}/datastreams?format=xml")
      request = RestClient::Request.new(:user => config.user,
                                        :password => config.password,
                                        :method => :get,
                                        :url => url)
      request.execute
    end
    xml = Nokogiri::XML(doc)

    hash = {}
    xml.xpath('//xmlns:objectDatastreams/xmlns:datastream', 'xmlns' => 'http://www.fedora.info/definitions/1/0/access/').each do |ds|
      hash[ds.attributes['dsid'].to_s] =  ds.attributes['label'].to_s
    end
    return hash

  rescue => e
    return {}
  end

  # This can't quite do what we want anymore, since we're checking
  # HTTPS but not logged in.  We could use an admin account and
  # include the https://login:password@site/ but I hate hate hate
  # that.  It would of course come out of the config file in that
  # case.
  #
  # TODO: go ahead and mandate drupal admin/password in config file
  # (or maybe in DB?)  for doing this via something like:
  #
  #     request = RestClient::Request.new(:user => config.drupal_admin_user,
  #                                       :password => config.drupal_admin_password,
  #                                       :method => :get,
  #                                       :url => url)


  def Utils.ping_islandora_for_object islandora_site, pid
    return :missing unless pid
    response = quickly do
      RestClient.head(URI.encode("https://#{islandora_site}/islandora/object/#{pid}/"))
    end
    if (response.code > 199 and response.code < 400)
      return :present
    else
      return :error
    end
  rescue RestClient::Forbidden => e
    return :forbidden
  rescue RestClient::ResourceNotFound => e
    return :missing
  rescue => e
    return :error
  end

  def Utils.ping_islandora_for_objects islandora_site, pids
    map = {}
    pids.each do |pid|
      map[pid] = Utils.ping_islandora_for_object islandora_site, pid
    end
    return map
  end

  def Utils.get_manifest config, directory

    manifest_filename = File.join(directory, 'manifest.xml')

    if not File.exists? manifest_filename
      raise PackageError, "Package directory #{directory} does not contain a manifest.xml file."
    end

    return Manifest.new(config, manifest_filename)
  end


  def Utils.get_mods config, directory

    name = File.basename directory
    mods_filename = File.join(directory, "#{name}.xml")

    if not File.exists? mods_filename
      raise PackageError, "Package directory #{directory} does not contain a MODS file named #{name}.xml."
    end

    return Mods.new(config, mods_filename)
  end


  if RUBY_VERSION < "1.9.0"
    CLEANUP_REGEXP = eval '/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F\xEF\xFF]/m'    # disallowed control characters and embedded deletes.
  else
    CLEANUP_REGEXP = eval '/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F\u00EF\u00FF]/m'
  end

  def Utils.cleanup_text text
    return text unless text.class == String
    return text.gsub(CLEANUP_REGEXP, ' ').strip
  end


  # TODO: need spec tests here

  # TODO: get warning, error messages from here to calling program:

  if RUBY_VERSION < "1.9.0"
    require 'rchardet'

    def Utils.re_encode_maybe text
      detector = CharDet.detect(text)

      return text if ['utf-8', 'ascii'].include? detector['encoding'].downcase

      if detector['confidence'] > 0.66  # somewhat arbitrary
        converter = Iconv.new('UTF-8', detector['encoding'])
        return converter.iconv(text)
      end

    rescue => e
      return text
    end

  else

    def Utils.re_encode_maybe text
      return text.force_encoding("UTF-8")
    rescue => e
      return text
    end

  end

  private

  # To help us create a smaller RAM footprint when processing huge files,  we use a lot of temporary files;  this lets us pass around a file object

  def Utils.temp_file         # creat an anonymous file handle
    tempfile = Tempfile.new('image-process-', Utils.tempdir)

    if RUBY_VERSION < '2.0.0'   # actually, I don't have 1.9.x version to test against, only a 1.8.7 system
      #### return tempfile.open
      return File.open(tempfile.path, 'w+b')
    else
      return open(tempfile, 'w+b')
    end

  ensure
    FileUtils.rm_f tempfile.path
  end

  # split text message into array of lines, removing any bogus error messages.

  def Utils.format_error_messages text

    return text unless text.class == String

    errors = []
    text.split(/\n/).each do |line|
      next if line =~ /Tag 42034: Rational with zero denominator/i
      next if line =~ /warning: component data type mismatch/i
      next if line =~ /warning: superfluous BPCC box/i
      next if line =~ /ICC Profile CS 52474220/i
      next if line =~ /warning: empty layer generated/i
      next if line =~ /bad value 0 for "orientation" tag/i
      next if line.empty?
      errors.push line
    end
    return errors
  end

  #### TODO:  need to catch errors, make sure we always provide an opened file, even it's File.open('/dev/null', 'rb')

  # return an open filehandle to a converted image;

  def Utils.image_processing config, image_filepath, command, error_title
    error_text = nil
    errors = []
    image = Utils.temp_file
    Open3.popen3(command) do |stdin, stdout, stderr|
      stdin.close
      while (data = stdout.read(1024*1024))
        image.write data
      end
      image.rewind
      error_text = stderr.read
    end

    errors = Utils.format_error_messages(error_text)
    errors.unshift error_title unless (errors.nil? or errors.empty?)

    return image, errors
  end

  def Utils.pass_through filepath
    return File.open(filepath, 'rb'), []
  end


  public

  def Utils.size config, image_filepath
    file, errors = Utils.image_processing(config, image_filepath,
                                          "#{config.image_convert_command} -identify #{Utils.shellescape(image_filepath)} null:",
                                          "When determining the size of the image of '#{image_filepath}' with command '#{config.image_convert_command} -identify' the following message was encountered:")

    data = file.gets
    file.close
    if data =~ /\s+(\d+)x(\d+)\s+/i
      width, height = $1, $2
      return width.to_i, height.to_i
    else
      return nil
    end
  rescue => e
    return nil
  end

  # run the convert command on an image file

  def Utils.pdf_to_thumbnail config, pdf_filepath
    return Utils.image_processing(config, pdf_filepath,
                                  "#{config.pdf_convert_command} -resize #{config.thumbnail_geometry} #{Utils.shellescape(pdf_filepath + '[0]')} jpg:-",
                                  "When creating a thumbnail image from the PDF '#{pdf_filepath}' with command '#{config.pdf_convert_command}' the following message was encountered:")
  end

  def Utils.pdf_to_preview config, pdf_filepath
    return Utils.image_processing(config, pdf_filepath,
                                  "#{config.pdf_convert_command} -resize #{config.pdf_preview_geometry} #{Utils.shellescape(pdf_filepath + '[0]')} jpg:-",
                                  "When creating a preview image from the PDF '#{pdf_filepath}' with command '#{config.pdf_convert_command}' the following message was encountered:")
  end

  def Utils.pdf_to_text config, pdf_filepath
    return Utils.image_processing(config, pdf_filepath,
                                  "#{config.pdf_to_text_command} #{Utils.shellescape(pdf_filepath)} -",
                                  "When extracting text from the PDF '#{pdf_filepath}' with command '#{config.pdf_to_text_command}' the following message was encountered:")

  end

  private

  # We need to check for certain problematic JP2K files;  we'll do it by using a (relatively) fast command from convert.

  def Utils.jp2k_ok? config, image_filepath
    file, errors = Utils.image_processing(config, image_filepath, "#{config.image_convert_command} -identify #{Utils.shellescape(image_filepath)} null:", "")
    return (errors.nil? or errors.empty?)
  ensure
    file.close if file.respond_to? :close and not file.closed?
  end


  ##### TODO: handle errors!

  def Utils.convert_jp2k_maybe config, image_filepath
    unused = nil
    yield image_filepath, nil unless Utils.mime_type(image_filepath) == 'image/jp2'
    yield image_filepath, nil if Utils.jp2k_ok?(config, image_filepath)

    temp_image_filepath = Tempfile.new('image-kakadu-', Utils.tempdir).path + '.tiff'
    unused, errors = Utils.image_processing(config, image_filepath,
                                            "#{config.kakadu_expand_command} -i #{Utils.shellescape(image_filepath)} -o #{Utils.shellescape(temp_image_filepath)}",
                                            "Failed attempt to convert #{image_filepath} using kakadu after JP2 image failure.")
    yield temp_image_filepath, errors
  ensure
    unused.close if unused.respond_to? :close and not unused.closed?
  end


  public

  def Utils.image_to_jpeg config,  image_filepath
    return Utils.pass_through(image_filepath) if Utils.mime_type(image_filepath) == 'image/jpeg'
    working_image_filepath = nil
    Utils.convert_jp2k_maybe(config, image_filepath) do |working_image_filepath, errors|
      return open('/dev/null'), errors unless errors.nil? or errors.empty?
      return Utils.image_processing(config, working_image_filepath,
                                    "#{config.image_convert_command} #{Utils.shellescape(working_image_filepath)} jpeg:-",
                                    "When creating a JEPG from the image '#{working_image_filepath}' with command '#{config.image_convert_command}' the following message was encountered:" )
    end
  ensure
    FileUtils.rm_f working_image_filepath if working_image_filepath and working_image_filepath != image_filepath
  end

  def Utils.image_to_pdf config, image_filepath
    return Utils.pass_through(image_filepath) if Utils.mime_type(image_filepath) == 'application/pdf'
    working_image_filepath = nil
    Utils.convert_jp2k_maybe(config, image_filepath) do |working_image_filepath, errors|
      return open('/dev/null'), errors unless errors.nil? or errors.empty?
      return Utils.image_processing(config, working_image_filepath,
                                    "#{config.image_convert_command} #{Utils.shellescape(working_image_filepath)} pdf:-",
                                    "When creating a PDF from the image '#{working_image_filepath}' with command '#{config.image_convert_command}' the following message was encountered:" )
    end
  ensure
    FileUtils.rm_f working_image_filepath if working_image_filepath and working_image_filepath != image_filepath
  end

  def Utils.image_to_tiff config,  image_filepath
    return Utils.pass_through(image_filepath) if Utils.mime_type(image_filepath) == 'image/tiff'
    working_image_filepath = nil
    Utils.convert_jp2k_maybe(config, image_filepath) do |working_image_filepath, errors|
      return open('/dev/null'), errors unless errors.nil? or errors.empty?
      return Utils.image_processing(config, working_image_filepath,
                                    "#{config.image_convert_command} #{Utils.shellescape(working_image_filepath)} tiff:-",
                                    "When creating a TIFF from the image '#{working_image_filepath}' with command '#{config.image_convert_command}' the following message was encountered:" )
    end
  ensure
    FileUtils.rm_f working_image_filepath if working_image_filepath and working_image_filepath != image_filepath
  end


  def Utils.extended_image_filepath(image_filepath)
    case Utils.mime_type(image_filepath)
    when 'application/pdf', 'image/tiff'
      return image_filepath + '[0]'
    else
      return image_filepath
    end
  end

  def Utils.image_to_jp2k config, image_filepath
    return Utils.pass_through(image_filepath) if Utils.mime_type(image_filepath) == 'image/jp2'

    please_jesus_forgive_me = "/usr/bin/convert -quiet -quality 75 -define jp2:prg=rlcp -define jp2:numrlvls=7 -define jp2:tilewidth=1024 -define jp2:tileheight=1024"
    return Utils.image_processing(config, image_filepath,
                                  "#{please_jesus_forgive_me} #{Utils.shellescape(Utils.extended_image_filepath(image_filepath))} jp2:-",
                                  "When creating a JP2K from the image '#{image_filepath}' with command '#{please_jesus_forgive_me}' the following message was encountered:" )
  end

  # Geometry is something like "200x200" - resizing preserves the
  # aspect ration (i.e., the image is uniformly scaled down to fit
  # into a 200 x 200 box).  The type of image is preserved unless the
  # optional new_format is supplied.  It won't hurt anything to
  # specify the same output type as the supplied image, if in doubt.

  def Utils.image_resize config, image_filepath, geometry, new_format = nil
    working_image_filepath = nil
    Utils.convert_jp2k_maybe(config, image_filepath) do |working_image_filepath, errors|
      return open('/dev/null'), errors unless errors.nil? or errors.empty?
      return Utils.image_processing(config, working_image_filepath,
                                    "#{config.image_convert_command} #{shellescape(working_image_filepath)} -resize #{geometry} #{ new_format.nil? ?  '-' :  new_format + ':-'}",
                                    "When creating a resized image (#{geometry}) from the image '#{working_image_filepath}' with command '#{config.image_convert_command}' the following message was encountered:" )
    end
  ensure
    FileUtils.rm_f working_image_filepath if working_image_filepath and working_image_filepath != image_filepath
  end

  private

  # Take a list of ISO 639-2b language codes, find the supported ones,
  # translate to the codes tesseract uses and return the options for a
  # command line.

  # config.supported_ocr_languages looks something like this, keyed by iso639b codes:
  #
  #  {  "fre" => { "tesseract" => "fra", "name" => "French" },  "ger" => { "tesseract" => "deu", "name" => "German" }, .... }

  def Utils.langs_to_tesseract_command_line config, *requested_languages
    supported = config.supported_ocr_languages
    wanted = []
    requested_languages.each { |lang| wanted.push(supported[lang]['tesseract']) if supported[lang] }

    return "-l eng" if wanted.empty?
    return wanted.map { |w| "-l " + w }.join(" ")
  end

  # like above,  but extracts message of what was selected

  def Utils.langs_to_names config, *requested_languages
    supported = config.supported_ocr_languages
    wanted = []
    requested_languages.each { |lang| wanted.push(supported[lang]['name']) if supported[lang] }

    return "English" if wanted.empty?
    return wanted.join(", ")
  end

  # String for when a requested language was not supported

  def Utils.langs_unsupported_comment config, *requested_languages
    supported = config.supported_ocr_languages
    unsupported = []
    requested_languages.each { |lang| unsupported.push(lang) unless supported[lang] }

    return unsupported.join(", ")
  end

  # array of ISO 639 3-letter codes - we take the subset of requested languages from our list of supported languages.

  def Utils.langs_supported config, requested_languages
    all_supported = config.supported_ocr_languages
    supported = []
    requested_languages.each { |lang| supported.push(lang) if all_supported[lang] }
    return  supported
  end


  def Utils.tesseract config, image_filepath, do_hocr, *langs

    tempfiles = []
    errors = []

    tempfiles.push converted_filepath = Tempfile.new('tesseract-jp2k', Utils.tempdir).path
    tempfiles.push tiff_filepath = converted_filepath + '.tiff'

    if Utils.mime_type(image_filepath) == 'image/jp2'
      if Utils.jp2k_ok?(config, image_filepath)
        temphandle, errors =  Utils.image_processing(config, image_filepath,
                                                     "#{config.image_convert_command} #{Utils.shellescape(image_filepath)} tiff:-",
                                                     "When creating a JP2K from the image '#{image_filepath}' with command '#{config.image_convert_command}' the following message was encountered:" )
        return nil, errors unless errors.nil? or errors.empty?

        File.open(tiff_filepath, 'w+b') do |tiff|
          while buff = temphandle.read(1024 * 1024)
            tiff.write buff
          end
        end
        temphandle.close
      else
        unused, errors = Utils.image_processing(config, image_filepath,
                                                    "#{config.kakadu_expand_command} -i #{Utils.shellescape(image_filepath)} -o #{Utils.shellescape(tiff_filepath)}",
                                                    "Failed attempt to convert #{image_filepath} using kakadu after JP2 image failure.")
        return nil, errors unless errors.nil? or errors.empty?
      end

      image_filepath = tiff_filepath
    end


    tempfiles.push base_filename = Tempfile.new('tesseract-', Utils.tempdir).path
    tempfiles.push text_filename = base_filename + (do_hocr ? '.html' : '.txt')

    err = ""

    ### TODO: doh!   popen3 doesn't need shellescape if cmdline is an array - fix EVERYWHERE

    cmdline = config.tesseract_command + ' ' + Utils.langs_to_tesseract_command_line(config, *langs)   + ' ' + Utils.shellescape(image_filepath) +  ' ' + base_filename + (do_hocr ? ' hocr' : '')

    Timeout.timeout(TESSERACT_TIMEOUT) do
      Open3.popen3(cmdline) do |stdin, stdout, stderr|
        stdin.close
        stdout.close
        while (data = stderr.read(1024 * 8)) do; err += data; end
        stderr.close
      end
      if not err.nil? and not err.empty?
        errors = [ "When producing OCR output from the command '#{cmdline}', the following errors were produced:" ]
        errors += err.split(/\n+/).map{ |line| line.strip }.select { |line| not line.empty? }
      end
    end

    if not File.exists? text_filename
      return nil, errors
    end

    return File.read(text_filename).strip, errors


  rescue Timeout::Error => e
    errors.push "Tesseract command '#{cmdline}' timed-out after #{TESSERACT_TIMEOUT} seconds"
    return nil, errors
  ensure
    FileUtils.rm_f(tempfiles)
  end


  public

  # use tesseract to create an HOCR file; strip out the DOCTYPE to avoid hitting w3c for the DTD:

  def Utils.hocr config, image_filepath, *langs
    text, errors = Utils.tesseract(config, image_filepath, true, *langs)

    return unless text.class == String
    return if text.empty?
    return text.gsub(/<!DOCTYPE\s+html.*?>\s+/mi, '')    # avoids schema lookup when indexing
  end

  # use tesseract to create an OCR file

  def Utils.ocr config, image_filepath, *langs
    text, errors = Utils.tesseract(config, image_filepath, false, *langs)
    return unless text.class == String
    return if text.empty?
    return text
  end


  # from shellwords.rb:

  def Utils.shellescape(str)
    # An empty argument will be skipped, so return empty quotes.
    return "''" if str.empty?

    str = str.dup

    # Process as a single byte sequence because not all shell
    # implementations are multibyte aware.
    str.gsub!(/([^A-Za-z0-9_\-.,:\/@\n])/n, "\\\\\\1")

    # A LF cannot be escaped with a backslash because a backslash + LF
    # combo is regarded as line continuation and simply ignored.
    str.gsub!(/\n/, "'\n'")

    return str
  end

  # We use the 'file' command to determine a file type. Argument FILE
  # may be a filename or open IO object.  If the latter, and it
  # supports rewind, it will be rewound when finished.

  def Utils.mime_type file

    type = error = nil

    case
    when file.is_a?(IO)
      Open3.popen3("/usr/bin/file", "--mime-type", "-b", "-") do |stdin, stdout, stderr|

        file.rewind if file.methods.include? 'rewind'

        stdin.write file.read(1024 * 8)  # don't need too much of this...
        stdin.close

        type   = stdout.read
        error  = stderr.read
      end

      file.rewind if file.respond_to? :rewind and not file.closed?

    when file.is_a?(String)  # presumed a filename

      raise "file '#{file}' not found"    unless File.exists? file
      raise "file '#{file}' not readable" unless File.readable? file
      Open3.popen3("/usr/bin/file", "--mime-type", "-b", file) do |stdin, stdout, stderr|

        type   = stdout.read
        error  = stderr.read
      end

      if file =~ /\.jp2/i and type.strip == 'application/octet-stream'
        type = 'image/jp2'
      end

    else
      error = "Unexpected input to /usr/bin/file: file argument was a #{file.class}"
    end

    raise PackageError, "Utils.mime_type error: #{error}" if not error.empty?
    return type.strip
  end


  # if not dates, FROM and TO will be nulls.

  def Utils.parse_dates from, to
    from, to = (from or to), (to or from)  # if only one provided, start with both the saem
    return unless from
    from, to = (from < to ? from : to), (to > from ? to : from)  # reorder if necessary
    t1 = Time.parse(from)
    return  if t1.strftime('%F') != from
    t2 = Time.parse(to)
    return  if t2.strftime('%F') != to
    return t1.to_s, (t2 + 86399).to_s
  rescue => e
    return
  end

  # Returns hash of collection codes offline ingest has actually used.

  def Utils.available_collection_codes
    sql = SqlAssembler.new
    sql.set_select 'SELECT DISTINCT collection_code FROM islandora_collections'
    hash = {}
    sql.execute.each do |collection_code|
      hash[collection_code] = true
    end
    return hash
  end

  # Helper for PackageListPaginator, CsvProvider.

  def Utils.setup_basic_filters sql, params

    from, to = Utils.parse_dates(params['from'], params['to'])
    sql.add_condition('time_started > ?', from)                     if from
    sql.add_condition('time_started < ?', to)                       if to
    sql.add_condition('islandora_site_id = ?', params['site_id'])   if params['site_id']
    sql.add_condition('content_model = ?', params['content-type'])  if params['content-type']
    sql.add_condition('title ilike ?', "%#{params['title']}%")      if params['title']
    sql.add_condition('(package_name ilike ? OR CAST(digitool_id AS TEXT) ilike ? OR islandora_pid ilike ?)', [ "%#{params['ids']}%" ] * 3)                if params['ids']
    sql.add_condition('islandora_packages.id IN (SELECT islandora_package_id FROM islandora_collections WHERE collection_code = ?)', params['collection']) if params['collection']
    sql.add_condition('islandora_packages.id IN (SELECT warning_messages.islandora_package_id FROM warning_messages)')                                     if params['status'] == 'warning'
    sql.add_condition('islandora_packages.id IN (SELECT error_messages.islandora_package_id FROM error_messages)')                                         if params['status'] == 'error'

    return sql
  end


  def Utils.get_datastream_contents config, pid, dsid

    url = URI.encode "#{config.fedora_url}/objects/#{pid}/datastreams/#{dsid}/content"
    request = RestClient::Request.new(:user => config.user,
                                      :password => config.password,
                                      :method => :get,
                                      :url => url)

    doc = quickly do
      request.execute
    end

    return doc
  end

  def Utils.rels_ext_get_policy_fields config, collection_pid
    str = <<-XML
        <islandora:inheritXacmlFrom rdf:resource="info:fedora/#{collection_pid}"/>
    XML

    rels_ext_content = Utils.get_datastream_contents(config, collection_pid, 'RELS-EXT')
    rels_ext_xml = Nokogiri::XML(rels_ext_content)

    # I know this is very bad but I can't get my head around the errors with multiple namespaces -Gail

    rels_ext_xml.remove_namespaces!
    view_rule_count = 0
    manage_rule_count = 0

    rels_ext_xml.xpath("//isViewableByUser").each do |node|
      view_rule_count += 1
      if (node.text != 'fedoraAdmin')
        str += <<-XML
        <islandora:isViewableByUser>#{node.text}</islandora:isViewableByUser>
    XML
      end
    end

    rels_ext_xml.xpath("//isViewableByRole").each do |node|
      view_rule_count += 1
      if (node.text != 'administrator')
        str += <<-XML
        <islandora:isViewableByRole>#{node.text}</islandora:isViewableByRole>
    XML
      end
    end

    if view_rule_count > 0
      str += <<-XML
        <islandora:isViewableByUser>fedoraAdmin</islandora:isViewableByUser>
        <islandora:isViewableByRole>administrator</islandora:isViewableByRole>
    XML
    end

    rels_ext_xml.xpath("//isManageableByUser").each do |node|
      manage_rule_count += 1
      if (node.text != 'fedoraAdmin')
        str += <<-XML
        <islandora:isManageableByUser>#{node.text}</islandora:isManageableByUser>
    XML
      end
    end

    rels_ext_xml.xpath("//isManageableByRole").each do |node|
      manage_rule_count += 1
      if (node.text != 'administrator')
        str += <<-XML
        <islandora:isManageableByRole>#{node.text}</islandora:isManageableByRole>
    XML
      end
    end

    if manage_rule_count > 0
      str += <<-XML
        <islandora:isManageableByUser>fedoraAdmin</islandora:isManageableByUser>
        <islandora:isManageableByRole>administrator</islandora:isManageableByRole>
    XML
    end

    return str
  end

  # find_appropriate_admin_config(config_file, server_name) is used
  # by the admin web service code.
  #
  # By convention, we are running a web service as
  # 'admin.school.digital.flvc.org' (or more recently
  # 'school.admin.digital.flvc.org') where 'school.digital.flvc.org'
  # is the drupal server.  So we delete the leading 'admin.' to find
  # the appropriate server.  The we read the config file for all
  # sections and probe each section in turn for "site:
  # school.digital.flvc.org".  Once we have a hit, we return the
  # appropriate config object.  We return nil if not found or on
  # error.

  def Utils.find_appropriate_admin_config config_file, server_name
    site = server_name.sub(/admin\./, '')

    Datyl::Config.new(config_file, 'default').all_sections.each do |section|
      site_config = Datyl::Config.new(config_file, 'default', section)
      return site_config if (site_config.site and site_config.site.downcase == site.downcase)
    end
    return nil
  rescue => e
    STDERR.puts "Error reading config file for #{site} section: #{e.class}: #{e.message}"
    return nil
  end


  def Utils.video_config_check(config)

    var = config.ffmpeg_command

    unless var
      return false, "The configuration file does not set the 'ffmpeg_command' variable."
    end

    unless File.exists? var
      return false, "The configuration 'ffmpeg_command' variable is set to '#{var}', but it can't be located."
    end

    var = config.video_default_thumbnail_filename

    unless var
      return false, "The configuration file does not set the 'video_default_thumbnail_filename' variable."
    end

    unless File.exists? var
      return false, "The configuration 'video_default_thumbnail_filename' variable is set to '#{var}', but it can't be located."
    end

    return true

  rescue => e
    return false, "Error when checking video configuration variables: #{e}"
  end

  # video_create_mp4(CONFIG, VIDEO_FILENAME) => IO-object, error-text
  #
  # Use the program ffmpeg (path determined by the CONFIG object) to create an islandora-ready MP4. On success return a pair:
  # a file descriptor opened on the newly-created MP4 file and NIL. On error return the pair NIL and some error text to report.

  def Utils.video_create_mp4(config, input_video_filename)
    output_video_filename = Tempfile.new('ffmpeg-', Utils.tempdir).path

    command_output_text = ""
    cpus = config.ffmpeg_cpus ? config.ffmpeg_cpus : 1

    command = [ config.ffmpeg_command, "-i", input_video_filename,
                "-f", "mp4", "-vcodec", "libx264", "-preset",  "medium",  "-crf", "20", "-acodec", "libfdk_aac",
                "-ab", "128k", "-ac", "2", "-async", "1", "-movflags", "faststart",
                "-loglevel", "error", "-nostdin", "-threads", cpus.to_s, "-y",
                output_video_filename ]

    Open3.popen3(*command) do |stdin, stdout, stderr|
      command_output_text += stdout.read.strip
      command_output_text += stderr.read.strip
    end

    unless command_output_text.empty?
      errors = [ "Error when running '#{command.join(' ')}', can't create an MP4 derivative." ] + command_output_text.split(/\n/)
      return nil, errors
    end

    unless File.exists?(output_video_filename) and File.stat(output_video_filename).size > 0
      return nil, [ "Unknown error when running '#{command.join(' ')}', can't create an MP4 derivative." ]
    end

    return File.open(output_video_filename, 'rb'), nil
  ensure
    FileUtils.rm_f output_video_filename
  end

  # video_duration(CONFIG, VIDEO_FILENAME) => integer
  #
  # Get the duration of VIDEO_FILENAME in seconds. On any kind of error return 0.
  #
  # We use
  #
  #   ffmpeg -i video_filename
  #
  # which produces (on stderr?!) something like:
  #
  #     ffmpeg version 1.1.1 Copyright (c) 2000-2013 the FFmpeg developers
  #       built on Mar  5 2014 15:22:32 with gcc 4.4.7 (GCC) 20120313 (Red Hat 4.4.7-4)
  #       ...
  #     Input #0, mov,mp4,m4a,3gp,3g2,mj2, from 'fsjc-video.obj.mp4':
  #       Metadata:
  #         major_brand     : mp42
  #         ...
  #       Duration: 00:30:43.25, start: 0.000000, bitrate: 1381 kb/s
  #         Stream #0:0(und): Video: h264 (Main) (avc1 / 0x31637661), yuv420p, 720x480 [SAR 8:9 DAR 4:3], 1210 kb/s, 29.97 fps, 29.97 tbr, 90k tbn, 180k tbc
  #         ...
  # Grab the line that includes 'Duration:', split '00:30:43.25"" to hours:minutes:seconds

  def Utils.video_duration(config, video_filename)
    command = [ config.ffmpeg_command, "-i", video_filename ]
    command_output_text = ""

    Open3.popen3(*command) do |stdin, stdout, stderr|
      command_output_text += stdout.read.strip
      command_output_text += stderr.read.strip
    end

    hours = minutes = seconds = 0

    command_output_text.split(/\n/).each do |line|
      if line =~ /duration:\s+(\d+):(\d+):(\d+)/i
        hours, minutes, seconds = $1, $2, $3
        break
      end
    end

    return hours.to_i * 360 + minutes.to_i * 60  + seconds.to_i
  rescue => e
    return 0
  end

  # video_create_thumbnail(CONFIG, VIDEO_FILENAME) => IO-Object, [ text, .. ]
  #
  # Use ffmpeg to determine the duration of VIDEO_FILENAME, then
  # extract a frame from the middle of the video, returning the
  # thumbnail as an opened JPEG IO stream. On any sort of error, open
  # and return the default video thumbnail.

  def Utils.video_create_thumbnail(config, video_filename)

    output_filename = Tempfile.new('ffmpeg-', Utils.tempdir).path
    duration = video_duration(config, video_filename)

    raise "Error determining the duration of video #{video_filename}, will use the default thumbnail." if duration < 2


    command = [ config.ffmpeg_command,
                '-itsoffset', '-2',  '-ss', (duration/2).to_s, '-i', video_filename,
                '-vcodec', 'mjpeg', '-vframes', '1', '-an', '-f', 'rawvideo',
                '-loglevel', 'quiet', '-y', '-nostdin', output_filename ]

    Open3.popen3(*command) { |stdin, stdout, stderr| stdout.read; stderr.read }

    raise "Error creating thumbnail running '#{command.join(' ')}', will use the default thumbnail." unless File.exists?(output_filename) and File.stat(output_filename).size > 0

    return File.open(output_filename), nil

  rescue => e
    return open(config.video_default_thumbnail_filename, 'rb'), [ e.message ]
  ensure
    FileUtils.rm_f output_filename
  end

  # Send request to update Solr index for object PID via Fedora GSearch REST API
  # This should be called for every ingested object so that offline ingest
  # does not rely on the Fedora message queue for indexing.

  def Utils.request_index_update_for_pid config, pid

    return if config.test_mode and not config.gsearch_url
    return if pid.nil? or pid.empty?

    pid = pid.sub(/^info:fedora\//, '')

    url = "#{config.gsearch_url}/?operation=updateIndex&action=fromPid&value=#{pid}"
    uri = URI.encode(url)
    results = quickly { RestClient::Request.execute(:method => :get, :url => uri, :user => config.user, :password => config.password) }

    return true if results.include? "<td>Inserted number of index documents: 1</td>"
    return false

  rescue RestClient::Exception => e
    raise SystemError, "Failed to update Solr index via Fedora GSearch for #{pid}: #{e.class} #{e.message}"

  end

  # Send request to delete from Solr index for object PID via Fedora GSearch REST API
  # This should be called for every deleted object so that offline ingest
  # does not rely on the Fedora message queue.

  def Utils.request_index_delete_for_pid config, pid

    return if config.test_mode and not config.gsearch_url
    return if pid.nil? or pid.empty?

    pid = pid.sub(/^info:fedora\//, '')
    url = "#{config.gsearch_url}/?operation=updateIndex&action=deletePid&value=#{pid}"
    uri = URI.encode(url)
    results = quickly { RestClient::Request.execute(:method => :get, :url => uri, :user => config.user, :password => config.password) }

    return true if results.include? "<td>Deleted number of index documents: 1</td>"
    return false

  rescue RestClient::Exception => e
    raise SystemError, "Failed to delete from Solr index via Fedora GSearch for #{pid}: #{e.class} #{e.message}"

  end

  def Utils.get_metadata_from_object config, pid

    # requesting metadata for an existing object
    # we'll get and parse a document as follows if we get a hit.
    #
    # <?xml version="1.0" encoding="UTF-8"?>
    # <response>
    # <lst name="responseHeader">
    #   <int name="status">0</int>
    #   <int name="QTime">0</int>
    #   <lst name="params">
    #     <str name="indent">on</str>
    #     <str name="version">2.2</str>
    #     <str name="fl">PID,mods_identifier_iid_ms</str>
    #     <str name="q">mods_identifier_iid_ms:FSDT2854731</str>
    #   </lst>
    # </lst>
    # <result name="response" numFound="1" start="0">
    #   <doc>
    #     <str name="PID">fsu:122</str>
    #     <arr name="mods_identifier_iid_ms"><str>FSDT2854731</str></arr>
    #   </doc>
    # </result>
    # </response>

    return if config.test_mode and not config.solr_url   # user specified testing mode without specifying server - technicaly OK?

    numFound = 0
    hasModel = ''
    iid = ''
    langCode = ''
    rootPID = ''

    url = "#{config.solr_url}/select/?q=PID:#{Utils.solr_escape(pid)}&version=2.2&indent=on&fl=PID,RELS_EXT_hasModel_uri_ms,mods_identifier_iid_ms,mods_language_languageTerm_code_ms,site_collection_id_ms"
    uri = URI.encode(url)
    doc = quickly { RestClient.get(uri) }
    xml = Nokogiri::XML(doc)

    element = xml.xpath("//result")[0]
    numFound = element.attr('numFound').to_i if element

    element = xml.xpath("//result/doc/arr[@name='RELS_EXT_hasModel_uri_ms']/str")[0]
    hasModel = element.child.text.sub(/^info:fedora\//, '') if element and element.child

    element = xml.xpath("//result/doc/arr[@name='mods_identifier_iid_ms']/str")[0]
    iid = element.child.text if element and element.child

    element = xml.xpath("//result/doc/arr[@name='mods_language_languageTerm_code_ms']/str")[0]
    langCode = element.child.text if element and element.child

    return numFound, hasModel, iid, langCode

  rescue RestClient::Exception => e
    raise SystemError, "Can't obtain metadata from solr at '#{url}': #{e.class} #{e.message}"

  rescue => e
    raise SystemError, "Can't process metadata obtained from solr at '#{url}': : #{e.class} #{e.message}"
  end

  # Given a parent pid, return the next page sequence number

  def Utils.get_next_page_sequence config, parent_pid, parent_model

    query = <<-SPARQL.gsub(/^        /, '')
        PREFIX islandora-rels-ext: <http://islandora.ca/ontology/relsext#>
        PREFIX fedora-rels-ext: <info:fedora/fedora-system:def/relations-external#>

        SELECT ?object ?sequence
        FROM <#ri>
        WHERE {
          ?object fedora-rels-ext:isMemberOf <info:fedora/#{parent_pid.sub(/^info:fedora\//, '')}> ;
               <fedora-model:hasModel> <info:fedora/#{parent_model}> ;
               <fedora-model:state> <fedora-model:Active> .
          ?object islandora-rels-ext:isSequenceNumber ?sequence
        }
        ORDER BY ?sequence
    SPARQL

    repository = ::Rubydora.connect :url => config.fedora_url, :user => config.user, :password => config.password

    quickly do
      repository.ping
    end

    last =  repository.sparql(query).map { |row_rec| row_rec['sequence'].to_i }.max

    return last ? last + 1 : 1

  rescue => e
    return
  end

  # Given a pid, check to see if it exists in Islandora

  def Utils.object_exists config, pid

    numFound = 0

    url = "#{config.solr_url}/select/?q=PID:#{Utils.solr_escape(pid)}&version=2.2&indent=on&fl=PID"
    uri = URI.encode(url)
    doc = quickly { RestClient.get(uri) }
    xml = Nokogiri::XML(doc)

    element = xml.xpath("//result")[0]
    numFound = element.attr('numFound').to_i if element

    return numFound > 0 ? true : false

  rescue RestClient::Exception => e
    raise SystemError, "Can't check if object exists from solr at '#{url}': #{e.class} #{e.message}"

  rescue => e
    raise SystemError, "Can't check if object exists from solr at '#{url}': : #{e.class} #{e.message}"
  end

end # of class Utils
