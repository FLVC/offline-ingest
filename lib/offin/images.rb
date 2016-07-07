$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), "../../lib"))

require 'offin/errors'
require 'open3'
require 'tempfile'
require 'fileutils'
require 'timeout'

# Helpful constants for several ingest classes:

module ImageConstants

  THUMBNAIL_GEOMETRY      = '200x200'      # width, height, for ImageMagick
  MEDIUM_GEOMETRY         = '500x700'
  PDF_PREVIEW_GEOMETRY    = '500x700'
  TIFF_FROM_JP2K_GEOMETRY = '1024x1024'

  SUPPORTED_IMAGES = [
    GIF  = 'image/gif',
    JP2  = 'image/jp2',
    PNG  = 'image/png',
    JPEG = 'image/jpeg',
    TIFF = 'image/tiff',
    PDF  = 'application/pdf',
  ]

  TEXT = 'text/plain'
  HTML = 'text/html'
  OCR  = 'application/x-ocr'
  HOCR = 'application/x-hocr'

  OCR_CANDIDATES = [    # we'll run 'tesseract --list-langs' to figure out which of these are actually compiled into tesseract.

    { :tesseract =>  'eng',           # the code tesseract wants
      :iso639b   =>  'eng',           # the code we expect in the MODS file (iso639b)
      :name      =>  'English' },     # some pretty text for errors/repots

    { :tesseract =>  'fra',
      :iso639b   =>  'fre',
      :name      =>  'French' },

    { :tesseract =>  'deu',
      :iso639b   =>  'ger',
      :name      =>  'German' },

    { :tesseract =>  'ita',
      :iso639b   =>  'ita',
      :name      =>  'Italian' },
  ]

  TESSERACT_TIMEOUT = 250 # 500   # tesseract can waste a lot of time on certain kinds of images

  def self.executable_location(name)
    paths = [ '/usr/local/bin', '/usr/bin' ] + ENV['PATH'].split(':')
    paths.each do |dir|
      bin = File.join(dir, name)
      return bin if (File.exists?(bin) and File.executable?(bin))
    end
    fail "Can't find '#{name}' on path "  + paths.inspect
  end

  TESSERACT_EXECUTABLE   = self.executable_location("tesseract")
  IDENTIFY_EXECUTABLE    = self.executable_location("identify")
  CONVERT_EXECUTABLE     = self.executable_location("convert")
  PDFTOTEXT_EXECUTABLE   = self.executable_location("pdftotext")
  KAKADU_EXECUTABLE      = self.executable_location("kdu_expand")
  GHOSTSCRIPT_EXECUTABLE = self.executable_location("gs")

  # special tokens in following templates:
  #
  #  %o   the path to the output file
  #  %t   the path to the output file, but strip off the extension
  #  %i   the path to the input file
  #  %m   the path to the input file,  '[0]' is appended if input is a TIFF or PDF, which selects first image of potential multi page images.

  CONVERT_TO_BASIC_IMAGE_COMMAND = "#{CONVERT_EXECUTABLE} -quiet -quality 75 -colorspace RGB %m %o"
  CONVERT_TO_JP2_COMMAND = "#{CONVERT_EXECUTABLE} -quiet -quality 75 -define jp2:prg=rlcp -define jp2:numrlvls=7 -define jp2:tilewidth=1024 -define jp2:tileheight=1024 %m %o"
  KAKADU_TO_TIFF_COMMAND = "#{KAKADU_EXECUTABLE} -quiet -i %i -o %o"
  PDF_TO_TEXT_COMMAND = "#{PDFTOTEXT_EXECUTABLE} -nopgbrk %i %o"
  CONVERT_WITH_LZW_COMPRESSION_COMMAND = "#{CONVERT_EXECUTABLE} -quiet -compress LZW %m %o"
  OCR_COMMAND = "#{TESSERACT_EXECUTABLE} %l %i %t"
  HOCR_COMMAND = "#{TESSERACT_EXECUTABLE} %l %i %t hocr"

  # Experiments that resulted in problematic DPIs in some output formats
  #
  # CONVERT_PDF_COMMAND = "#{CONVERT_EXECUTABLE} -units PixelsPerInch -density 96 -quiet -compress LZW %m %o"
  # RASTERIZE_PDF_FIRST_PAGE_COMMAND = "#{GHOSTSCRIPT_EXECUTABLE} -o %o -q -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dTextAlphaBits=4 -dGraphicsAlphaBits=4 -dPrinted=false -dFirstPage=1 -dLastPage=1 -r180 -sDEVICE=pngalpha %i"
end


class Image
  include Errors
  include ImageConstants

  DEBUG = true
  THUNKS = {}
  SUPPORTED_IMAGES.each { |img|  THUNKS[img] = {} }

  attr_reader :mime_type, :file_path, :file_name, :file_io, :size

  def initialize(path)
    @file_path = path
    @file_name = File.basename(path)
    @temp_files = [ ]
    @file_dimensions = nil

    fail "No such file #{@file_path}"    unless File.exists?   @file_path   # TODO: group together with other failures
    fail "File #{@file_path} unreadable" unless File.readable? @file_path

    @mime_type = get_mime_type
    @file_io = File.open(file_path, 'rb')
    @size = File.stat(@file_path).size
    @tesseract_languages = supported_ocr_languages

    # Setup GIF conversion routines

    THUNKS[GIF][JP2]  = image_to_jp2()
    THUNKS[GIF][PDF]  = convert_with_lzw_compression(PDF)
    THUNKS[GIF][TIFF] = convert_with_lzw_compression(TIFF)
    THUNKS[GIF][OCR]  = image_ocr
    THUNKS[GIF][HOCR] = image_hocr

    [ GIF, JPEG, PNG ].each { |target_mime_type| THUNKS[GIF][target_mime_type] = convert_to_basic_image(target_mime_type) }

    # JP2 conversion routines

    THUNKS[JP2][OCR]  = image_ocr
    THUNKS[JP2][HOCR] = image_hocr

    [ GIF, JP2, JPEG, PDF, PNG, TIFF ].each { |target_mime_type| THUNKS[JP2][target_mime_type] = jp2_to_image(target_mime_type) }

    # PDF conversion routines

    THUNKS[PDF][TEXT] = pdf_to_text

    [ GIF, JP2, JPEG, PDF, PNG, TIFF ].each { |target_mime_type| THUNKS[PDF][target_mime_type] = pdf_to_image(target_mime_type) }

    # TIFF conversion routines

    THUNKS[TIFF][JP2]  = image_to_jp2
    THUNKS[TIFF][PDF]  = convert_with_lzw_compression(PDF)
    THUNKS[TIFF][TIFF] = convert_with_lzw_compression(TIFF)
    THUNKS[TIFF][HOCR] = image_hocr
    THUNKS[TIFF][OCR]  = image_ocr

    [ GIF, JPEG, PNG ].each { |target_mime_type| THUNKS[TIFF][target_mime_type] = convert_to_basic_image(target_mime_type) }

    # basic images - GIF routines will work:

    THUNKS[JPEG] = THUNKS[GIF]
    THUNKS[PNG]  = THUNKS[GIF]

    yield self

  rescue => e
    STDERR.puts e  if DEBUG
  ensure
    remove_temp_files
  end

  def file_name_label
    file_name.sub(/\.[^\.]+$/, '')
  end

  def stream
    file_io.rewind
    return file_io
  end

  def dimensions
    return @file_dimensions if @file_dimensions
    data = nil
    oops = nil
    Open3.popen3(IDENTIFY_EXECUTABLE, file_path) do |stdin, stdout, stderr|
      stdin.close
      data = stdout.gets
      oops = stderr.read
    end
    if data =~ /\s+(\d+)x(\d+)\s+/i
      return @file_dimensions = [ $1.to_i, $2.to_i ]
    else
      warning "can't get dimensions for image '#{file_name}'", oops.split("\n")
      return @file_dimensions = [ 0, 0 ]
    end
  rescue => e
    warning "can't get dimensions for image '#{file_name}'", e.message, oops.split("\n")
    return @file_dimensions = [ 0, 0 ]
  end

  # convert our image to target_mime_type, optionally resizing to geometry.
  # returns nil unless derivative creation succeeds with a non-empty file

  def convert(target_mime_type, geometry=nil)
    proc = THUNKS[mime_type][target_mime_type]
    if proc.nil?
      warning "derivative creation failed for image '#{file_name}' - conversion from '#{mime_type}' to '#{target_mime_type}' isn't supported" if proc.nil?
      return nil
    end
    # TODO: check the returned file descriptor for the correct mime type?
    return proc.call(geometry)
  rescue => e
    warning "derivative creation failed for image '#{file_name}' - during conversion from '#{mime_type}' to '#{target_mime_type}' the following error occured: ", e.message
    return nil
  end

  def extension(mime_type)
    case mime_type
    when GIF;    'gif'
    when JPEG;   'jpg'
    when PNG;    'png'
    when TIFF;   'tiff'
    when JP2;    'jp2'
    when PDF;    'pdf'
    when TEXT;   'text'
    when OCR;    'txt'
    when HOCR;   'html'
    else
      fail "Unexpected mimetype '#{mime_type}' encountered when processing '#{file_name}'"  # only expected to fail for bad file, never on internal use
    end
  end

  private

  # TODO - explain

  def run_command(command_template, output_mime_type, geometry=nil, input_file_name=nil)
    input_file_name ||= @file_name
    output_file_name = temp_file_name(output_mime_type)
    cmd = command_template_substitutions(command_template, input_file_name, output_file_name, output_mime_type, geometry)
    run(cmd)

    unless File.exists?(output_file_name)
      warning "failed '#{output_mime_type}' derivative creation for '#{file_name}'"
      return nil
    end

    unless File.stat(output_file_name).size > 0
      warning "failed '#{output_mime_type}' derivative creation for '#{file_name}'"
      return nil
    end

    return open(output_file_name, 'rb')

  rescue => e
      warning "failed derivative creation of '#{output_mime_type}' for '#{file_name}'", e.message
      return nil
  end

  # helper for run_command that execs an array of strings

  def run(argv)
    STDERR.puts argv.join(' ') if DEBUG

    data = nil
    oops = nil
    Open3.popen3(*argv) do |stdin, stdout, stderr|
      stdin.close
      data = stdout.gets
      oops = stderr.read
    end
    unless (oops.nil? or oops.empty? or not_really_errors(oops))
      warning "could not create derivative for '#{file_name}'",  "failed command: '#{argv.join(' ')}'", oops.split("\n")
    end
  end

  # These thunks are called by the convert method,  which will catch any common exception

  def convert_to_basic_image(target_mime_type)
    return Proc.new do |geometry|
      run_command(CONVERT_TO_BASIC_IMAGE_COMMAND, target_mime_type, geometry)
    end
  end

  def image_to_jp2()
    return Proc.new do |geometry|
      run_command(CONVERT_TO_JP2_COMMAND, JP2, geometry)
    end
  end

  def jp2_to_image(target_mime_type)
    return Proc.new do |geometry|
      unless @kakadu_cached_file
        fd = run_command(KAKADU_TO_TIFF_COMMAND, TIFF) # N.B.  Kakadu creates uncompressed TIFFs
        raise "Can't create a '#{target_mime_type}' derivative from the JP2 file #{file_name}" unless fd
        @kakadu_cached_file = fd.path
      end
      case target_mime_type
      when TIFF, PDF
        run_command(CONVERT_WITH_LZW_COMPRESSION_COMMAND, target_mime_type, geometry, @kakadu_cached_file)
      when JP2
        run_command(CONVERT_TO_JP2_COMMAND, target_mime_type, geometry, @kakadu_cached_file)
      when GIF, PNG, JPEG
        run_command(CONVERT_TO_BASIC_IMAGE_COMMAND, target_mime_type, geometry, @kakadu_cached_file)
      else
        raise "no conversion suppport is available for JP2 to #{target_mime_type}"
      end
    end
  end

  def pdf_to_image(target_mime_type)
    return Proc.new do |geometry|
      run_command(CONVERT_WITH_LZW_COMPRESSION_COMMAND, target_mime_type, geometry)

      # I've tried this but can't get DPI tags consistent in derivatives
      #
      # @ghostscript_cached_file ||= run_command(RASTERIZE_PDF_FIRST_PAGE_COMMAND, PNG).path
      # case target_mime_type
      # when TIFF, PDF
      #   run_command(CONVERT_WITH_LZW_COMPRESSION_COMMAND, target_mime_type, geometry, @ghostscript_cached_file)
      # when JP2
      #   run_command(CONVERT_TO_JP2_COMMAND, target_mime_type, geometry, @ghostscript_cached_file)
      # when GIF, PNG, JPEG
      #   run_command(CONVERT_TO_BASIC_IMAGE_COMMAND, target_mime_type, geometry, @ghostscript_cached_file)
      # when TEXT
      #   run_command(PDF_TO_TEXT_COMMAND, target_mime_type, geometry, @ghostscript_cached_file)
      # else
      #   fail "no suppport for PDF to #{target_mime_type}"
      # end
    end
  end

  # works for tiffs and pdfs:

  def convert_with_lzw_compression(target_mime_type)
    return Proc.new do |geometry|
      run_command(CONVERT_WITH_LZW_COMPRESSION_COMMAND, target_mime_type, geometry)
    end
  end

  def pdf_to_text
    return Proc.new do
      run_command(PDF_TO_TEXT_COMMAND, TEXT)
    end
  end

  # given an open file object, read it, strip leadimng and trailing whitespace, save it to the same file path, and reopen read-only

  def strip_file(fd)
    path = fd.path
    fd.rewind
    text = fd.read.strip
    fd.close
    File.open(path, 'w') { |f| f.write text }
    return File.open(path, 'rb')
  rescue => e
    return nil
  end

  def image_hocr
    image_ocr_hairy(HOCR_COMMAND, HOCR)
  end

  def image_ocr
    image_ocr_hairy(OCR_COMMAND, OCR)
  end

  def es(ar)
    args.count == 1 ? '' : 's'
  end


  def image_ocr_hairy(command, type)
    return Proc.new do |languages|
      input_file_name = file_name

      unless (mime_type == JPEG or mime_type == TIFF)
        @tesseract_intermediary_cached ||= run_command(CONVERT_WITH_LZW_COMPRESSION_COMMAND, TIFF)
        raise "unable to process '#{file_name}' for performing OCR" unless @tesseract_intermediary_cached
        input_file_name = @tesseract_intermediary_cached.path
      end

      args = []
      unsupported = []
      if languages
        languages.each do |lang|
          record = @tesseract_languages.select { |rec| rec[:iso639b] == lang }.shift
          record ? args.push record[:tesseract] : unsupported.push lang
        end
      end
      args = [ "eng" ] if args.empty?
      unless unsupported.empty?
        warning "OCR - encountered unsupported language code#{es(unsupported)} '" + unsupported.join("', '") + "' when processing #{file_name}"
        warning "OCR - using language#{es(args)} '" + args.join("', '") + "'"
      end
      command = nil
      opts = args.map { |x| "-l #{x}" }.join(' ')
      begin
        Timeout.timeout(TESSERACT_TIMEOUT) do
          command = template.sub("%l", opts)
          fd = strip_file(run_command(command, type, nil, input_file_name))
        end
        raise "no text detected" unless (fd and File.stat(fd.path).size > 0)
      rescue Timeout::Error => e
        warning "the OCR command '#{command}' for '#{file_name}' was taking too long: stopped after #{TESSERACT_TIMEOUT} seconds"
      rescue => e
        warning "the OCR command '#{command}' for '#{file_name}' failed:", e.message
      else
        return fd
      end
    end

  end


  def command_template_substitutions(command_template, input_file_name, output_file_name, output_mime_type, geometry=nil)
    cmd = []
    command_template.split(/\s+/).each do |str|
      case str
      when /%i/
        str.sub!(/%i/, input_file_name)
      when /%m/
        str.sub!(/%m/, input_file_name)
        str += '[0]'  if [ TIFF, PDF ].include?(get_mime_type(input_file_name))
      when /%o/
        str.sub!(/%o/, output_file_name)
      when /%t/
        str.sub!(/%t/, output_file_name.sub(/\.[^\.]*$/, ''))
      end
      cmd.push str
    end
    cmd = ([ cmd[0], '-resize', geometry ] + cmd[1..-1])  if geometry
    return cmd
  end


  def not_really_errors(oops)
    return true  if (oops =~ /Tesseract Open Source OCR Engine/i and oops.split("\n").size == 1)
    return false
  end


  # create a derivation for this image, on success return an opened file i/o object for the newly created derivative.

  def remove_temp_files
    FileUtils.rm_f @temp_files unless @temp_files.empty?
  rescue
  end

  def get_mime_type(path=nil)
    path ||= file_path
    type  = nil
    oops = nil
    Open3.popen3("/usr/bin/file",  "--mime-type",  "-b", path) do |stdin, stdout, stderr|
      type  = stdout.read
      oops  = stderr.read
    end
    type.strip!
    type = 'image/jp2'    if (path =~ /\.jp2/i and type == 'application/octet-stream')

    ### TODO: this is a pain point... make it gentler
    unless SUPPORTED_IMAGES.include? type
      fail "#{path} is not a supported image: it's mime type is '#{type}', but it must be one of '#{SUPPORTED_IMAGES.join("', '")}'"
    end
    return type
  end

  # We use this to create unique file names, we immediately close the file descriptors.

  def temp_file_name(mime_type)
    ext = '.' + extension(mime_type)
    label = file_name_label + '-'
    tf = ENV['TMPDIR'] ? Tempfile.new([ label,  ext ], ENV['TMPDIR']) : Tempfile.new([ label,  ext ])
    name = tf.path
    tf.close
    tf.unlink
    @temp_files.push name
    return name
  end

  # ask tesseract what languages it supports, select those from our locally supported set in OCR_CANDIDATES.

  def supported_ocr_languages
    tesseract_languages = []
    text = ''
    Open3.popen3(TESSERACT_EXECUTABLE, '--list-langs') do |stdin, stdout, stderr|
      stdin.close
      stdout.close
      text = stderr.read
    end

    text.split("\n").each do |lang|
      lang.strip!
      tesseract_languages += OCR_CANDIDATES.select { |rec| rec[:tesseract] == lang }
    end

  rescue => e
    warning "can't determine the supported OCR languages", e.message
    return tesseract_languages
  end



end # of image class

include ImageConstants

Kernel.trap('INT')  { STDERR.puts "Interrupt"  ; exit }
Kernel.trap('HUP')  { STDERR.puts "Hangup"  ; exit }
Kernel.trap('PIPE') { STDERR.puts "Pipe Closed"  ; exit }

Image.new(input_file_name = ARGV[0]) do |image|
  puts "input #{image.mime_type} image #{image.file_name} size: #{image.size}"
  image.errors?   && puts 'errors: ',   image.errors
  image.warnings? && puts 'warnings: ', image.warnings
  image.notes?    && puts 'notes: ',    image.notes

  [ OCR, HOCR, TEXT, GIF, JP2, PNG, JPEG, TIFF, PDF ].each do |target|
    begin
      image.reset_errors
      image.reset_warnings
      name = "test-from-" + image.file_name_label + "-" + image.extension(image.mime_type) + "-to." + image.extension(target)
      fd = image.convert(target)
      STDERR.puts "errors: ", image.errors  if image.errors?
      STDERR.puts "warnings: ", image.warnings  if image.warnings?
      fd.nil? && next
      open(name, 'w') do |out|
        while (data = fd.read(1048576))  # 1024^2 - should it really be 1000^2?  TODO: run timing tests
          out.write data
        end
      end
    rescue => e
      puts "output #{target} image #{name} error: #{e.message}"
    end
  end

end
