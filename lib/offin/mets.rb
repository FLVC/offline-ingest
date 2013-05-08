# For testing, remove soon:

    Kernel.trap('INT')  { STDERR.puts "Interrupt"    ; exit -1 }
    Kernel.trap('HUP')  { STDERR.puts "Hangup"       ; exit -2 }
    Kernel.trap('PIPE') { STDERR.puts "Pipe Closed"  ; exit -3 }

    $LOAD_PATH.unshift "#{ENV['HOME']}/WorkProjects/offline-ingest/lib/"


require 'nokogiri'
require 'offin/document-parsers'
require 'offin/errors'


class Mets

  include Errors

  attr_reader :xml_document, :sax_document

  def initialize config, path

    @filename  = path
    @config    = config
    @valid     = true

    @text = File.read(@filename)

    if @text.empty?
      error "METS file '#{@filename}' is empty."
      @valid = false
      return
    end

    @xml_document = Nokogiri::XML(@text)

    if not @xml_document.errors.empty?
      error "Error parsing METS file '#{@filename}':"
      error @xml_document.errors
      @valid = false
      return
    end

    @valid &&= validates_against_schema?

    create_sax_document
  end

  def create_sax_document
    @sax_document = SaxDocumentExamineMets.new
    Nokogiri::XML::SAX::Parser.new(@sax_document).parse(@text)

    if @sax_document.warnings? or @sax_document.errors?
      warning "SAX parser warnings for '#{@filename}'"
      warning  @sax_document.errors
      warning  @sax_document.warnings
    end
  end

  def valid?
    @valid
  end

  # TODO: check METS file for mets schema location if it makes sense

  def validates_against_schema?
    schema_path = File.join(@config.schema_directory, 'mets.xsd')
    xsd = Nokogiri::XML::Schema(File.open(schema_path))

    issues = []
    xsd.validate(@xml_document).each { |err| issues.push err }

    if not issues.empty?
      error "METS file '#{@filename}' had validation errors as follows:"
      error *issues
      return false
    end
    return true

    # TODO: catch nokogiri class errors here, others get backtrace
  rescue => e
    error "Exception #{e.class}, #{e.message} occurred when validating '#{@filename}' against the METS schema '#{schema_path}'."
    # error e.backtrace
    return false
  end
end







# TESTING

Struct.new('MockConfig', :schema_directory)

config = Struct::MockConfig.new
config.schema_directory = File.join(ENV['HOME'], 'WorkProjects/offline-ingest/lib/include/')

SaxDocumentExamineMets.debug = true

mets = Mets.new(config, ARGV[0])

puts 'Errors: ',   mets.errors   if mets.errors?
puts 'Warnings: ', mets.warnings if mets.warnings?

#  mets.sax_document.print_file_dictionary


if mets.valid?
  puts "METS is valid"
else
  puts "METS is invalid"
end
