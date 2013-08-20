require 'rubydora'
require 'RMagick'
require 'fileutils'
require 'iconv'
require 'offin/exceptions'
require 'open3'
require 'stringio'
require 'tempfile'
require 'timeout'
require 'rest_client'
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

  TESSERACT_TIMEOUT = 60 # tesseract can waste a lot of time on certain kinds of images

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
    raise SystemError,  message + ': ' + e.message # ick
  end


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

  def Utils.quickly
    Timeout.timeout(2) do
      yield
    end
  rescue Timeout::Error => e
    raise 'timed out after 2 seconds'
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

    url = "#{config.solr_url}/select/?q=mods_identifier_iid_ms:#{iid}&version=2.2&indent=on&fl=PID,mods_identifier_iid_ms"
    doc = quickly do
      RestClient.get(url)
    end

    xml = Nokogiri::XML(doc)
    element = xml.xpath("//result/doc/str[@name='PID']")[0]
    return element.child.text if element and element.child
    return nil

  rescue RestClient::Exception => e
    raise SystemError, "Can't obtain IID from solr at '#{url}': #{e.class} #{e.message}"

  rescue => e
    raise SystemError, "Can't process IID from solr: : #{e.class} #{e.message}"
  end


  # return a mapping from short islandpora pids (e.g. fsu:foobar, not info:fedora/fsu:foobar) and their titles

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
  #   <datastream dsid="MARCXML" label="Archived Digitool MarcXML" mimeType="text/xml"/>
  #   <datastream dsid="TN" label="Thumbnail" mimeType="image/jpeg"/>
  #   <datastream dsid="DT-METS" label="Archived DigiTool METS for future reference" mimeType="text/xml"/>
  #   <datastream dsid="TOC" label="Table of Contents" mimeType="application/json"/>
  # </objectDatastreams>

  def Utils.get_datastream_names fedora_url, pid
    doc = quickly do
      RestClient.get(fedora_url.sub(/\/+$/, '') + "/objects/#{pid.sub('info:fedora', '')}/datastreams?format=xml")
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

  def Utils.ping_islandora_for_object islandora_site, pid
    return :missing unless pid
    response = quickly do
      RestClient.head "http://#{islandora_site}/islandora/object/#{pid}/"
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
      raise PackageError, "Package directory #{directory} does not contain a manifest file."
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


  # ImageMagick sometimes fails on JP2K,  so we punt to kakadu, which we'll munge into a TIFF and call ImageMagick on *that*.
  # Kakadu only produces uncomressed TIFFs, so we don't want to use kakadu indiscriminately or in place of ImageMagick.

  def Utils.careful_with_that_jp2 config, jp2k_filepath
    Utils.silence_streams(STDERR) do
      return Magick::Image.read(jp2k_filepath).first
    end
  rescue Magick::ImageMagickError => e
    return Utils.kakadu_jp2k_to_tiff(config, jp2k_filepath, "ImageMagick: #{e.message}")
  end

  def Utils.kakadu_jp2k_to_tiff config, jp2k_filepath, previous_error_message = ''
    temp_image_filename = Tempfile.new('image-kakadu-').path + '.tiff'
    text  = ''
    error = ''

    Open3.popen3("#{config.kakadu_expand_command} -i #{Utils.shellescape(jp2k_filepath)} -o #{temp_image_filename}") do |stdin, stdout, stderr|
      stdin.close
      text  = stdout.read
      error = stderr.read
    end
    error.strip!

    message = " #{previous_error_message}; " if not previous_error_message.empty?

    raise PackageError, "Image processing error: could not process JP2 image #{jp2k_filepath.sub(/.*\//, '')}:#{message}#{error.gsub("\n", ' ')}" unless error.empty?

    return Magick::Image.read(temp_image_filename).first
  ensure
    FileUtils.rm_f(temp_image_filename)
  end


  def Utils.pdf_to_text config, pdf_filepath

    text = nil
    error = nil

    Open3.popen3("#{config.pdf_to_text_command} #{Utils.shellescape(pdf_filepath)} -") do |stdin, stdout, stderr|
      stdin.close
      text  = stdout.read
      error = stderr.read
    end

    #  raise PackageError, "Processing #{pdf_filepath} resulted in these errors: #{error}" if not error.empty?

    # TODO: capture errors.  We occasionally get things like "Bad
    # Annotation Destination" that are warnings. We'd like to add
    # these to the generated warnings, perhaps.

    return text if text.length > 50   # pretty arbitrary...
    return nil
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


  def Utils.image_to_pdf config, image_filepath
    pdf = nil
    error = nil
    errors = []
    Open3.popen3("#{config.image_to_pdf_command} #{Utils.shellescape(image_filepath)} pdf:-") do |stdin, stdout, stderr|
      stdin.close
      pdf = stdout.read
      error = stderr.read
    end
    return pdf
  end


  def Utils.pdf_to_thumbnail config, pdf_filepath
    image = nil
    error = nil
    errors = []
    Open3.popen3("#{config.pdf_convert_command} -resize #{config.thumbnail_geometry} #{Utils.shellescape(pdf_filepath + '[0]')} jpg:-") do |stdin, stdout, stderr|
      stdin.close
      image = stdout.read
      error = stderr.read
    end
    if not error.nil? and not error.empty?
      errors = [ "When creating a thumbnail image from the PDF with command '#{config.pdf_convert_command}' the following message was produced:" ]
      errors += error.split(/\n+/).map{ |line| line.strip }.select { |line| not line.empty? }
    end
    return image, errors
  end

  def Utils.pdf_to_preview config, pdf_filepath

    image = nil
    error = nil
    errors = []
    Open3.popen3("#{config.pdf_convert_command} -resize #{config.pdf_preview_geometry} #{Utils.shellescape(pdf_filepath + '[0]')} jpg:-") do |stdin, stdout, stderr|
      stdin.close
      image = stdout.read
      error = stderr.read
    end
    if not error.nil? and not error.empty?
      errors  = [ "When creating a preview image from the PDF with command '#{config.pdf_convert_command}' the following message was produced:" ]
      errors += error.split(/\n+/).map{ |line| line.strip }.select { |line| not line.empty? }
    end
    return image, errors
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
    return Utils.tesseract(config, image_filepath, :hocr).gsub(/<!DOCTYPE\s+html.*?>\s+/mi, '')
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

        stdin.write file.read(1024 ** 2)  # don't need too much of this...
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


  def Utils.parse_dates from, to
    from, to = (from or to), (to or from)  # if only one provide, start with both
    return unless from
    from, to = (from < to ? from : to), (to > from ? to : from)  # reorder if necessaet
    t1 = Time.parse(from)
    return  if t1.strftime('%F') != from
    t2 = Time.parse(to)
    return  if t2.strftime('%F') != to
    return t1.to_s, (t2 + 86399).to_s
  rescue => e
    return
  end

  # return hash of collection codes offline ingest has actually used

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

end # of class Utils
