# This file is like that kitchen drawer with the assorted unsortables.

require 'rubydora'
require 'RMagick'
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


# Extend RI mixins to include itql queries:

module Rubydora
  module ResourceIndex
    def itql query
      if CSV.const_defined? :Reader
        FasterCSV.parse(self.risearch(query, :lang => 'itql'), :headers => true)
      else
        CSV.parse(self.risearch(query, :lang => 'itql'), :headers => true)
      end
    end
  end
end




class Utils

  TESSERACT_TIMEOUT = 500   # tesseract can waste a lot of time on certain kinds of images
  QUICKLY_TIMEOUT   = 10   # seconds before giving up on fedora

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
    chars = [ '\\', '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '~', '*', '?', ':', '"', ';', ' ' ]
    chars.each { |c| str.gsub!(c, '\\' + c) }
    return str
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
    query = "select $object $title from <#ri> " +
      "where $object <fedora-model:label> $title " +
      "and $object <fedora-model:hasModel> <info:fedora/islandora:collectionCModel>"

    repository = ::Rubydora.connect :url => config.fedora_url, :user => config.user, :password => config.password

    quickly do
      repository.ping
    end

    hash = {}
    repository.itql(query).each do |x|
      hash[x[0].sub('info:fedora/', '')] = x[1]
    end
    return hash
  rescue => e
    return {}
  end

  # get_datastream_names(config, islandora_pid) => hash
  #
  # parse XML for dsid/label pairs, as from the example document:
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

  def Utils.get_datastream_names config, pid
    doc = quickly do
      url = "http://" + config.user + ":" + config.password + "@" + config.fedora_url.sub(/^http:\/\//, '') +
        "/objects/#{pid.sub('info:fedora', '')}/datastreams?format=xml"
      RestClient.get(URI.encode(url))
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

      if detector['confidence'] > 0.66
        ## STDERR.puts "Attempting to convert from #{detector['encoding']} (confidence #{detector['confidence']})."
        converter = Iconv.new('UTF-8', detector['encoding'])
        return converter.iconv(text)
      end

    rescue => e
      ##  STDERR.puts "Error converting with #{detector.inspect}}, returning original text"
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
    tempfile = Tempfile.new('rspec')

    if RUBY_VERSION < '2.0.0'   # actually, I don't have 1.9.x version to test against, only a 1.8.7 system
      return tempfile.open
    else
      return open(tempfile, 'w+b')
    end

  ensure
    File.unlink tempfile.path
  end


  def Utils.image_processing config, image_filepath, command, error_title
    error = nil
    errors = []
    image = Utils.temp_file
    Open3.popen3(command) do |stdin, stdout, stderr|
      stdin.close
      while (data = stdout.read(1024*1024))
          image.write data
      end
      image.rewind
      error = stderr.read
    end
    if not error.nil? and not error.empty?
      errors = [ error_title ]
      errors += error.split(/\n+/).map{ |line| line.strip }.select { |line| not line.empty? }
    end
    return image, errors
  end

  public



  # run the convert command on an image file

  def Utils.image_to_pdf config, image_filepath
    return Utils.image_processing(config, image_filepath,
                                  "#{config.image_to_pdf_command} #{Utils.shellescape(image_filepath)} pdf:-",
                                  "When creating a PDF from the image '#{image_filepath}' with command '#{config.image_to_pdf_command}' the following message was produced:" )
  end


  def Utils.pdf_to_thumbnail config, pdf_filepath
    return Utils.image_processing(config, pdf_filepath,
                                  "#{config.pdf_convert_command} -resize #{config.thumbnail_geometry} #{Utils.shellescape(pdf_filepath + '[0]')} jpg:-",
                                  "When creating a thumbnail image from the PDF '#{pdf_filepath}' with command '#{config.pdf_convert_command}' the following message was produced:")
  end

  def Utils.pdf_to_preview config, pdf_filepath
    return Utils.image_processing(config, pdf_filepath,
                                  "#{config.pdf_convert_command} -resize #{config.pdf_preview_geometry} #{Utils.shellescape(pdf_filepath + '[0]')} jpg:-",
                                  "When creating a preview image from the PDF '#{pdf_filepath}' with command '#{config.pdf_convert_command}' the following message was produced:")
  end


  def Utils.pdf_to_text config, pdf_filepath
    return Utils.image_processing(config, pdf_filepath,
                                  "#{config.pdf_to_text_command} #{Utils.shellescape(pdf_filepath)} -",
                                  "When extracting texy from the PDF '#{pdf_filepath}' with command '#{config.pdf_convert_command}' the following message was produced:")
  end


  def Utils.image_magick_to_tiff config,  image_filepath
    return Utils.image_processing(config, image_filepath,
                                  "#{config.image_convert_command} #{Utils.shellescape(image_filepath)} tiff:-",
                                  "When creating a TIFF from the image '#{image_filepath}' with command '#{config.image_convert_command}' the following message was produced:" )


  end


  # kdu_expand on jp2k

  def Utils.kakadu_jp2k_to_tiff config, jp2k_filepath

    temp_image_filename = Tempfile.new('image-kakadu-').path + '.tiff'

    unused, errors = Utils.image_processing(config, jp2k_filepath,
                                            "#{config.kakadu_expand_command} -i #{Utils.shellescape(jp2k_filepath)} -o #{temp_image_filename}",
                                            "Image processing error: could not process JP2 image '#{jp2k_filepath}'")

    if File.exists? temp_image_filename
      file = open(temp_image_filename, 'r+b')
    else
      file = open('/dev/null', 'r+b')
    end
    return file, errors
  ensure
    unused.close if unused and unused.respond_to? :close
    FileUtils.rm_f(temp_image_filename)
  end



  # TODO: fold this into something....

  # ImageMagick sometimes fails on JP2K,  so we punt to kakadu, which we'll munge into a TIFF and call ImageMagick on *that*.
  # Kakadu only produces uncompressed TIFFs, so we don't want to use kakadu indiscriminately or in place of ImageMagick.

  def Utils.be_careful_with_that_jp2_now config, jp2k_filepath
    Utils.silence_streams(STDERR) do
      return Magick::Image.read(jp2k_filepath).first
    end
  rescue Magick::ImageMagickError => e
    return Utils.kakadu_jp2k_to_tiff(config, jp2k_filepath, "ImageMagick: #{e.message}")
  end






  # expects image or pathname, image must be a format understood by tesseract (no jp2)

  def Utils.tesseract config, image_or_filename, hocr = nil

    tempfiles = []

    image_filepath =   case image_or_filename
                       when String
                         image_or_filename
                       when Magick::Image
                         tempfiles.push  temp_image_filename = Tempfile.new('image-magick-').path
                         image_or_filename.write temp_image_filename
                         temp_image_filename
                       else
                         return
                       end

    tempfiles.push base_filename = Tempfile.new('tesseract-').path
    tempfiles.push text_filename = base_filename + (hocr ? '.html' : '.txt')

    error = nil

    cmdline = config.tesseract_command + ' ' + Utils.shellescape(image_filepath) +  ' ' + base_filename + (hocr ? ' hocr' : '')

    Timeout.timeout(TESSERACT_TIMEOUT) do
      Open3.popen3(cmdline) do |stdin, stdout, stderr|
        stdin.close
        stdout.close
        error = stderr.read
      end
    end

    return unless File.exists?(text_filename)
    return if (text = File.read(text_filename).strip).empty?
    return text

  rescue Timeout::Error => e
    return
  ensure
    FileUtils.rm_f(tempfiles)
  end


  # use tesseract to create an HOCR file; strip out the DOCTYPE to avoid hitting w3c for the DTD:

  def Utils.hocr config, image_filepath
    text = Utils.tesseract(config, image_filepath, :hocr)
    return unless text.class == String
    return text.gsub(/<!DOCTYPE\s+html.*?>\s+/mi, '')
  end

  # use tesseract to create an OCR file

  def Utils.ocr config, image_filepath
    return Utils.tesseract(config, image_filepath)
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

  # use the 'file' command to determine a file type. Argument FILE may be a filename or open IO object.  If the latter, and it supports rewind, it will be rewound when finished.

  def Utils.mime_type file

    type = error = nil

    case
    when file.is_a?(Magick::Image)

      error = []
      type = case file.format
             when 'GIF'  ; 'image/gif'
             when 'JP2'  ; 'image/jp2'
             when 'JPEG' ; 'image/jpeg'
             when 'PNG'  ; 'image/png'
             when 'TIFF' ; 'image/tiff'
             else;         'application/octet-stream'
             end

    when file.is_a?(IO)

      Open3.popen3("/usr/bin/file --mime-type -b -") do |stdin, stdout, stderr|

        file.rewind if file.methods.include? 'rewind'

        stdin.write file.read(1024 * 8)  # don't need too much of this...
        stdin.close

        type   = stdout.read
        error  = stderr.read
      end

      file.rewind if file.methods.include? 'rewind'

    when file.is_a?(String)  # presumed a filename

      raise "file '#{file}' not found"    unless File.exists? file
      raise "file '#{file}' not readable" unless File.readable? file
      Open3.popen3("/usr/bin/file --mime-type -b " + shellescape(file)) do |stdin, stdout, stderr|

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

  # Returns hash HASH of collection codes offline ingest has actually used.

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
    sql.add_condition('time_started > ?', from) if from
    sql.add_condition('time_started < ?', to) if to

    if val = params['site_id']
      sql.add_condition('islandora_site_id = ?', val)
    end

    if val = params['title']
      sql.add_condition('title ilike ?', "%#{val}%")
    end

    if val = params['ids']
      sql.add_condition('(package_name ilike ? OR CAST(digitool_id AS TEXT) ilike ? OR islandora_pid ilike ?)', [ "%#{val}%" ] * 3)
    end

    if val = params['content-type']
      sql.add_condition('content_model = ?', val)
    end

    if val = params['collection']
      sql.add_condition('islandora_packages.id IN (SELECT islandora_package_id FROM islandora_collections WHERE collection_code = ?)', val)
    end

    if params['status'] == 'warning'
      sql.add_condition('islandora_packages.id IN (SELECT warning_messages.islandora_package_id FROM warning_messages)')
    end

    if params['status'] == 'error'
      sql.add_condition('islandora_packages.id IN (SELECT error_messages.islandora_package_id FROM error_messages)')
    end

    return sql
  end


  def Utils.get_datastream_contents config, pid, dsid

    url = "http://" + config.user + ":" + config.password + "@" + config.fedora_url.sub(/^http:\/\//, '') + "/objects/#{pid}/datastreams/#{dsid}/content"
    doc = quickly do
      RestClient.get(URI.encode(url))
    end

    return doc
  end

  def Utils.rels_ext_get_policy_fields config, collection_pid
    str = <<-XML.gsub(/^    /, '')
        <islandora:inheritXacmlFrom>#{collection_pid}</islandora:inheritXacmlFrom>
    XML

    rels_ext_content = Utils.get_datastream_contents(config, collection_pid, 'RELS-EXT')
    rels_ext_xml = Nokogiri::XML(rels_ext_content)

    # I know this is very bad but I can't get my head around the errors with multiple namespaces

    rels_ext_xml.remove_namespaces!
    view_rule_count = 0
    manage_rule_count = 0

    rels_ext_xml.xpath("//isViewableByUser").each do |node|
      view_rule_count += 1
      if (node.text != 'fedoraAdmin')
        str += <<-XML.gsub(/^    /, '')
        <islandora:isViewableByUser>#{node.text}</islandora:isViewableByUser>
    XML
      end
    end

    rels_ext_xml.xpath("//isViewableByRole").each do |node|
      view_rule_count += 1
      if (node.text != 'administrator')
        str += <<-XML.gsub(/^    /, '')
        <islandora:isViewableByRole>#{node.text}</islandora:isViewableByRole>
    XML
      end
    end

    if view_rule_count > 0
      str += <<-XML.gsub(/^    /, '')
        <islandora:isViewableByUser>fedoraAdmin</islandora:isViewableByUser>
        <islandora:isViewableByRole>administrator</islandora:isViewableByRole>
    XML
    end

    rels_ext_xml.xpath("//isManageableByUser").each do |node|
      manage_rule_count += 1
      if (node.text != 'fedoraAdmin')
        str += <<-XML.gsub(/^    /, '')
        <islandora:isManageableByUser>#{node.text}</islandora:isManageableByUser>
    XML
      end
    end

    rels_ext_xml.xpath("//isManageableByRole").each do |node|
      manage_rule_count += 1
      if (node.text != 'administrator')
        str += <<-XML.gsub(/^    /, '')
        <islandora:isManageableByRole>#{node.text}</islandora:isManageableByRole>
    XML
      end
    end

    if manage_rule_count > 0
      str += <<-XML.gsub(/^    /, '')
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
  # 'admin.school.digital.flvc.org' where 'school.digital.flvc.org' is
  # the drupal server.  So we delete the leading 'admin.' to find the
  # appropriate server.  The we read the config file for all sections
  # and probe each section in turn for "site:
  # school.digital.flvc.org".  Once we have a hit, we return the
  # appropriate config object.  We return nil if not found or on error.

  def Utils.find_appropriate_admin_config config_file, server_name
    site = server_name.sub(/^admin\./, '')

    Datyl::Config.new(config_file, 'default').all_sections.each do |section|
      site_config = Datyl::Config.new(config_file, 'default', section)
      return site_config if (site_config.site and site_config.site.downcase == site.downcase)
    end
    return nil
  rescue => e
    STDERR.puts "Error reading config file for #{site} section: #{e.class}: #{e.message}"
    return nil
  end

end # of class Utils
