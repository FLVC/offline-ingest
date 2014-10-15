require 'nokogiri'
require 'offin/document-parsers'
require 'offin/errors'
require 'json'


# This file contains helper classes/structs for analyzing METS data.


# Class TableOfContents is initialized by a METS structmap, which is
# provided by a Mets object method.  The Package object makes use of
# TableOfContents to produce json output for the page
# turning application.
#
# The structmap object (class MetsStructMap) provides an ordered list of these objects:
#
#    <Struct:MetsDivData  :level, :title, :is_page, :fids, :files>
#
# where :files is an array of
#
#    <Struct::MetsFileDictionaryEntry :sequence, :href, :mimetype, :use, :fid>
#
# We're transforming this into a sequence of Struct::Page and Struct::Chapter objects
# that are slightly more uniform.

Struct.new('Page',          :title, :level, :image_filename, :image_mimetype, :text_filename, :text_mimetype, :fid)
Struct.new('Chapter',       :title, :level)

class TableOfContents

  include Errors  # because I don't have enough of my own.

  def initialize structmap

    @sequence = []
    @valid = true
    @structmap = structmap


    structmap.each do |elt|
      str = sprintf("%-9s %-50s ", '* ' * elt.level, elt.title)

      if elt.is_page
        files = elt.files.map{ |e| e.href  }.join(', ')
        fids  = elt.fids.join(', ')
        # str +=  "   FIDS: [#{fids}] - FILES: [#{files}]"
        str +=  "   [#{files}]"
      end
      puts str
    end


    @structmap.each do |div_data|
      if div_data.is_page

        entry = Struct::Page.new
        entry.title = (div_data.title || '').strip
        entry.level = div_data.level

        div_data.files.each do |f|
          if f.mimetype =~ /image/
            entry.image_filename = f.href
            entry.image_mimetype = f.mimetype
            entry.fid = f.fid
          elsif f.mimetype =~ /text/             # what was I thinking here? no way this is going to work....
            entry.text_filename = f.href
            entry.text_mimetype = f.mimetype
            entry.fid = f.fid
          end
        end

      else
        entry = Struct::Chapter.new
        entry.title = div_data.title || ''
        entry.level = div_data.level
      end

      @sequence.push entry
    end


    check_for_page_images
    nip_it_in_the_bud              # TODO: this is proabably mistaken now that we can telescope pages
    # telescope_pages
    cleanup_chapter_titles
    cleanup_page_titles
   end


  # We can sometimes have structmaps that repeat pages, notably for the case where there are two chapters on one page.  For instance:
  #
  # Chapter            <METS:div LABEL="Baby-Land" TYPE="section">
  #
  # Page                 <METS:div LABEL="3" TYPE="page">            <METS:fptr FILEID="FID6"/> </METS:div>
  # Page                 <METS:div LABEL="4" TYPE="page">            <METS:fptr FILEID="FID7"/> </METS:div>        </METS:div>
  #
  # Chapter            <METS:div LABEL="Who Is She" TYPE="section">
  # Page                 <METS:div LABEL="5" TYPE="page">            <METS:fptr FILEID="FID8"/> </METS:div>        </METS:div>
  #
  # Chapter            <METS:div LABEL="Niddlety Noddy" TYPE="section">
  # Page                 <METS:div LABEL="5" TYPE="page">            <METS:fptr FILEID="FID8"/> </METS:div>        </METS:div>
  #
  # Chapter            <METS:div LABEL="Little Frogs At School" TYPE="section">
  # Page                 <METS:div LABEL="6" TYPE="page">            <METS:fptr FILEID="FID10"/> </METS:div>        </METS:div>
  #
  # In the above, we have the page with FILEID FID8 (LABEL="5") repeated twice,  which won't do.   More briefly the data look like:
  #
  # Chapter  Baby-Land
  # Page       3
  # Page       4
  # Chapter  Who Is She
  # Page       5
  # Chapter  Niddlety Noddy
  # Page       5
  # Chapter  Little Frogs At School
  # Page       6
  #
  # we telescope the data as so:
  #
  # Chapter  Baby-Land
  # Page       3
  # Page       4
  # Chapter  Who Is She
  # Chapter  Niddlety Noddy
  # Page       5
  # Chapter  Little Frogs At School
  # Page       6
  #


  # All we do in telescope_pages is discard the first occurences of
  # pages if there are multiple occurences of that page, and merge
  # titles of adjacent chapters (or not -)

  def telescope_pages
    page_records = {}

    @sequence.each do |entry|
      if is_page?(entry)
        filename = filename(entry)
        page_records[filename]  = 0 unless page_records[filename]
        page_records[filename] += 1
      end
    end

    telescoped_sequence = []
    @sequence.each do |entry|
      case
      when is_page?(entry)
        filename = filename(entry)
        if page_records[filename] > 1
          page_records[filename] -= 1
        else
          telescoped_sequence.push entry
        end

      # when is_chapter?(entry)
      #   if is_chapter?(telescoped_sequence[-1])
      #     telescoped_sequence[-1].title += "; " + entry.title
      #   else
      #     telescoped_sequence.push entry
      #   end

      when is_chapter?(entry)                    # above collapses chapters... this leaves multiple adjancent chapters
        telescoped_sequence.push entry

      end # of case

    end
    @sequence = telescoped_sequence
  end


  def each
    @sequence.each { |entry| yield entry }
  end

  def pages
    @sequence.select{ |elt| is_page? elt }
  end

  def chapters
    @sequence.select{ |elt| is_chapter? elt }
  end

  def to_json label = nil

    container = {}
    container['title'] = label  unless label.nil? || label.empty?

    toc = []
    seq = 1
    @sequence.each do |entry|
      rec = { 'level' => entry.level, 'title' => entry.title, 'pagenum' => entry.pagenum }
      # case entry
      # when Struct::Chapter
      #   rec['type'] = 'chapter'
      # when Struct::Page
      #   rec['type'] = 'page'
      #   seq += 1
      # end
      toc.push rec
    end

    container['table_of_contents'] = toc  unless toc.empty?

    return JSON.pretty_generate container
  end

  def valid?
    @valid and not errors?
  end


  def print
    @sequence.each do |entry|
      case entry
      when Struct::Chapter
        indent = '* ' * entry.level
      when Struct::Page
        indent = '- ' * entry.level
      end
      puts indent + entry.title
    end

  end


  private


  def is_page? elt
    return elt.class == Struct::Page
  end

  def is_chapter? elt
    return elt.class == Struct::Chapter
  end

  def filename elt
    return unless is_page? elt
    return elt.image_filename || elt.text_filename
  end


  # sometimes we have a TOC structure like this:
  #
  # level-1
  #   level-2
  #   level-2
  #     level-3
  #     level-3
  #   level-2
  #
  # where we really don't need the level-1 there, at all. I call that a bud.

  def nip_it_in_the_bud
    while has_bud?(@sequence) do
      @sequence.shift
      @sequence.each { |entry| entry.level -= 1 }
    end
  end

  def has_bud? seq
    return false unless seq.length > 1
    return true if  seq[0].level == 1 \
                and not is_page? seq[0] \
                and seq[1..-1].all? { |ent| ent.level > 1 }
  end


  # TODO: not too sure how to approach this yet.  it is certainly too
  # early to do actual filesystem checks for the files' existence.
  #
  # So this may be a fatal error (@valid => false) but let's wait and
  # experiment for now; just issue warnings.

  def check_for_page_images

    issues = []
    pages.each do |p|
      issues.push  "The METS file does specify an associated image file for page #{p.title}." unless p.image_filename
    end

    warning issues unless issues.empty?
  end

  # strip off the extension the filename (no directory components)

  def file_name name
    File.basename(name).sub(/\.[^\.]*/, '')
  end



  # Clean up page titles - the sequence of pages titles must exist and
  # be unique for the IA book reader to treat table of contents
  # correctly. Here we'll make that so.


  def cleanup_page_titles

    # This is a little tricky - depends on the fact that Hash uses EQL to determine membership.

    dups = Hash.new()

    pages.each do |p|
      dups[p] = [] unless dups[p]
      dups[p].push p
    end

    # dups.keys now gives us identical pages.

    sequence = 0
    already_processed = Hash.new(false)

    pages.each do |page|

      next if already_processed[page]

      dups[page].each do |p|   # repeated pages must get identical titles

        case
        when (p.title.empty? and p.image_filename)
          p.title = file_name(p.image_filename)
        when (p.title.empty?)
          p.title = sequence.to_s
        end

        already_processed[p] = true
      end
    end

    # But if they are *not* repeated pages, and they have identical titles, we have to differentiate them

    we_have_issues = []
    already_processed = {}
    title_counts = {}
    pages.each do |page|
      dups[page].each do |p|
        next unless title_counts[p]
        next if already_processed[p]
        p.title += " (#{title_counts[p]})"
        we_have_issues.push p.title
      end
      already_processed[page] = true
      title_counts[page] ||= 0
      title_counts[page]  += 1
    end

    if not we_have_issues.empty?
       warning "Not all page labels in the METS file were unique; a parenthesized number was appended for these labels: '" + we_have_issues.join("', '") + "'."
    end
  end



  def cleanup_chapter_titles
    chapters.each { |c| c.title = 'Chapter' if (not c.title or c.title.empty?) }
  end

end # of class TableOfContents




class Mets

  include Errors

  attr_reader :xml_document, :sax_document, :filename, :structmap, :file_dictionary, :text

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
    @file_dictionary = @sax_document.file_dictionary
    @structmap = select_best_structmap @sax_document.structmaps
  end


  # Top level label gets precedence

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

    if sax_document.warnings?
      warning "The SAX parser produced the following warnings for '#{short_filename}':"
      warning  sax_document.warnings
    end

    # SAX errors just treated as warnings (for now).

    if sax_document.errors?
      warning "The SAX parser produced the following errors for '#{short_filename}':"
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
      warning "Multiple structMaps found in METS file, discarding the shortest (least number of referenced files)."
      scores.each { |sm,num| return sm if num == max }
    end

    # Otherwise, we need to do more work.  This tries to do some sort of scoring based on file information.

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
      warning "Multiple structMaps found in METS file, selecting the most likely."
      scores.each { |sm,num| return sm if num == max }
    end

    error "Can't determine which of the #{list.count} METS structMaps should be used."
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
