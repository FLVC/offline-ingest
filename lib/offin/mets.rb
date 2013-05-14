require 'nokogiri'
require 'offin/document-parsers'
require 'offin/errors'
require 'json'


# helper classes for maintaining mets data

Struct.new('Page',    :title, :level, :image_filename, :image_mimetype, :text_filename, :text_mimetype)
Struct.new('Chapter', :title, :level)

class TableOfContents

  include Errors  # because I don't have enough of my own.

  def initialize structmap
    @sequence = []
    @valid = true
    @structmap = structmap

    # The structmap object (class MetsStructMap) provides an ordered list of these objects:
    #    <Struct:MetsDivData  :level, :title, :is_page, :fids, :files>
    # where :files is a list of
    #    <Struct::MetsFileDictionaryEntry :sequence, :href, :mimetype, :use, :fid>

    @structmap.each do |div_data|
      if div_data.is_page

        entry = Struct::Page.new
        entry.title = div_data.title
        entry.level = div_data.level

        div_data.files.each do |f|
          if f.mimetype =~ /image/
            entry.image_filename = f.href
            entry.image_mimetype = f.mimetype
          elsif f.mimetype =~ /text/
            entry.text_filename = f.href
            entry.text_mimetype = f.mimetype
          end
        end

      else
        entry = Struct::Chapter.new
        entry.title = div_data.title
        entry.level = div_data.level
      end
      @sequence.push entry
    end

    cleanup_chapter_titles
    cleanup_page_titles
    check_for_page_images
  end

  def each
    @sequence.each { |entry| yield entry }
  end

  def pages
    @sequence.select{ |elt| elt.class == Struct::Page }
  end

  def chapters
    @sequence.select{ |elt| elt.class == Struct::Chapter }
  end

  def to_json label = nil

    list = []
    list.push( { 'title' => label } )  unless label.nil? || label.empty?

    toc = []
    seq = 1
    @sequence.each do |entry|
      rec = { 'level' => entry.level, 'title' => entry.title, 'pagenum' => seq  }
      case entry
      when Struct::Chapter
        rec['type'] = 'chapter'
      when Struct::Page
        rec['type'] = 'page'
        seq += 1
      end
      toc.push rec
    end

    list.push( { 'table_of_contents' => toc } ) unless toc.empty?

    return JSON.pretty_generate list
  end

  def valid?
    @valid and not errors?
  end


  def print
    @sequence.each do |entry|
      case entry
      when Struct::Chapter
        indent = '++ ' * entry.level
      when Struct::Page
        indent = '-- ' * entry.level
      end
      puts indent + entry.title
    end

  end


  private

  # TODO: not too sure how to approach this yet; it may be too early
  # to do filename checks (we have to do some checking in the package)
  #
  # So this may be a fatal error (@valid => false) but let's wait and
  # experiment for now; just issue warnings.

  def check_for_page_images

    issues = []
    pages.each do |p|
      issues.push  "#{p.title} does not have an associated image file." unless p.image_filename
    end

    warning issues unless issues.empty?
  end

  # strip off the extension the filename (no directory components)

  def file_name name
    File.basename(name).sub(/\.[^\.]*/, '')
  end

  # clean up page names

  def cleanup_page_titles
    pages.each do |p|
      new_title = p.title.sub(/^page\s*/i, '').strip
      p.title = new_title
    end

    pages.each do |p|
      if p.title.empty?
        if p.image_filename                    # we need to be able to make the assumption this exists
          p.title = file_name(p.image_filename)
        end
      end
    end

    seen = {}
    sequence = 1
    problems = []
    pages.each do |p|
      if p.title.empty?
        p.title = sequence.to_s
      end
      if seen[p.title]
        p.title += " (#{sequence})"
        problems.push p.title
      end
      seen[p.title] = true
      sequence += 1
    end

    if not problems.empty?
      warning "Some page labels were not unique; the sequence number was appended: '" + problems.join("', '") + "'."
    end
  end

  def cleanup_chapter_titles
    chapters.each { |c| c.title = 'Chapter' if (not c.title or c.title.empty?) }
  end

end # of class TableOfContents




class Mets

  include Errors

  attr_reader :xml_document, :sax_document, :filename, :structmap

  def initialize config, path

    @filename  = path
    @config    = config
    @valid     = true
    @structmap = nil

    @text = File.read(@filename)

    if @text.empty?
      error "METS file '#{short_filename}' is empty."
      @valid = false
      return
    end

    @xml_document = Nokogiri::XML(@text)

    if xml_syntax_errors? @xml_document.errors
      @valid = false
      return
    end

    if not validates_against_schema?
      @valid = false
      return
    end

    @sax_document = create_sax_document
    @structmap = select_best_structmap @sax_document.structmaps
  end


  # not sure which should get precedence here:

  def label
    @sax_document.label || @structmap.label
  end

  def valid?
    @valid and not errors?
  end

  private

  def xml_syntax_errors? list
    return false if list.empty?

    validation_warnings = []
    validation_errors   = []
    list.each do |err|
      next unless err.class == Nokogiri::XML::SyntaxError
      mesg = " on line #{err.line}: #{err.message}"
      case
      when (err.warning? or err.none?);   validation_warnings.push 'Warning' + mesg
      when err.error?;                    validation_errors.push   'Error' + mesg
      when err.fatal?;                    validation_errors.push   'Fatal Error' + mesg
      end
    end

    unless validation_warnings.empty?
      warning "Validation of the METS file '#{short_filename}' produced the following warnings:"
      warning  validation_warnings
    end

    unless validation_errors.empty?
      error "Validation of the METS file '#{short_filename}' produced the following errors:"
      error  validation_errors
      return true
    end

    return false
  end

  # for error messages, give the rightmost directory name along with the filename

  def short_filename
    return $1 if @filename =~ %r{.*/(.*/[^/]+)$}
    return @filename
  end

  # sax document will parse and produce a file dictionary, label, structmaps.

  def create_sax_document
    sax_document = SaxDocumentExamineMets.new
    Nokogiri::XML::SAX::Parser.new(sax_document).parse(@text)

    # sax parser errors may not be fatal, so store them to warnings.

    if sax_document.warnings? or sax_document.errors?
      warning "SAX parser warnings for '#{short_filename}':"
      warning  sax_document.warnings
    end

    # SAX errors just treated as warnings (for now).

    if sax_document.errors?
      warning "SAX parser errors for '#{short_filename}':"
      warning  sax_document.errors
    end

    return sax_document
  end


  def select_best_structmap list
    if list.empty?
      @valid = false
      error "No valid structMaps were found in the METS document."
      return
    end

    # If there's only one, it's the best.

    return list.pop if list.length == 1
    scores = {}
    list.each { |sm| scores[sm] = sm.number_files }

    # If there are two or more, and one has more file references than the others, select it.

    if scores.values.uniq == scores.values
      max = scores.values.max
      warning "Multiple structMaps found in METS file '#{short_filename}', discarding the shortest (least number of files)."
      scores.each { |sm,num| return sm if num == max }
    end

    # Otherwise, we need to do lots more work.  This tries to do some sort of scoring based on file information.

    scores = {}
    list.each do |sm|
      score = 0
      sm.each do |div_data|
        next unless div_data.is_page
        div_data.files.each do |file_data|
          score += case file_data.use
                   when 'reference'; 2
                   when 'index'    ; 1
                   when 'archive'  ; 0
                   else;            -1
                   end
          if  file_data.mimetype =~ /image/
            score += 2
          elsif file_data.mimetype =~ /text/
            score += 1
          else
            score = -1
          end
          scores[sm] = score
        end
      end
    end

    if scores.values.uniq == scores.values
      max = scores.values.max
      warning "Multiple structMaps found in METS file '#{short_filename}', selecting the most likely."
      scores.each { |sm,num| return sm if num == max }
    end

    error "Can't determine best of multiple structMaps found in METS file '#{short_filename}'."
    @valid = false
    return
  end


  def validates_against_schema?
    schema_path = File.join(@config.schema_directory, 'mets.xsd')
    xsd = Nokogiri::XML::Schema(File.open(schema_path))

    return ! xml_syntax_errors?(xsd.validate(@xml_document))

    # TODO: catch nokogiri class errors here, others should get backtrace
  rescue => e
    error "Exception #{e.class}, #{e.message} occurred when validating '#{short_filename}' using the METS schema at '#{schema_path}'."
    # error e.backtrace
    return false
  end

end # of class Mets
