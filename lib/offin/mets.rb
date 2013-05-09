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


  # sax document will parse and produce a file dictionary, label, structmaps, which we'll process

  def create_sax_document
    @sax_document = SaxDocumentExamineMets.new
    Nokogiri::XML::SAX::Parser.new(@sax_document).parse(@text)

    # sax parser errors may not be fatal, so store them to warnings.

    if @sax_document.warnings? or @sax_document.errors?
      warning "SAX parser warnings for '#{@filename}'"
      warning  @sax_document.warnings
    end

    if @sax_document.errors?
      warning "SAX parser errors for '#{@filename}'"
      warning  @sax_document.errors
    end
  end



  def process_structmap

    # @sax_document.number_files

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

SaxDocumentExamineMets.debug = false


mets = Mets.new(config, ARGV[0])


dict = mets.sax_document.file_dictionary

mets.sax_document.structmaps.each do |map|
  map.each do |entry|
    indent = '..'
    puts indent * (entry.level - 1)  + (entry.is_page ? 'PAGE: ' : 'CHAPTER: ') + entry.title
    if entry.is_page
      entry.fids.each do |fid|
        f = dict[fid]
        puts indent * entry.level + f.use + ' ' + f.mimetype + ' ' + f.href
      end
    end
  end
end


puts 'Errors: ',   mets.errors   if mets.errors?
puts 'Warnings: ', mets.warnings if mets.warnings?

#  mets.sax_document.print_file_dictionary


if mets.valid?
  puts "METS is valid"
else
  puts "METS is invalid"
end



<<NOTES

need list of page objects, created from structmap and dictionary.  Will have image file, maybe text files + mime, sequence number (implied), pagelabel
  <PAGE  image_filename, image_type, text_filename, text_type, label, level>
  <CHAPTER  label, level>

select best structmap (has most fids), if same, find ones that have proper :use (reference/index) or mimetype of image or texts; warn when one is dropped.
make sure all things marked pages have fids; warn and remove from page list if not.
make sure all files for pages exist; error if not


create TOC





NOTES
