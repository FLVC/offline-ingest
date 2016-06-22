require 'open3'
require 'tempfile'
require 'fileutils'


module ImageConstants

  THUMBNAIL_GEOMETRY      = '200x200'      # width, height, for ImageMagick
  MEDIUM_GEOMETRY         = '500x700'
  PDF_PREVIEW_GEOMETRY    = '500x700'
  TIFF_FROM_JP2K_GEOMETRY = '1024x1024'

  GIF  = 'image/gif'
  JP2  = 'image/jp2'
  PNG  = 'image/png'
  JPEG = 'image/jpeg'
  TIFF = 'image/tiff'
  PDF  = 'application/pdf'

  TEXT = 'text/plain'
  OCR  = 'application/x-ocr'
  HOCR = 'application/x-hocr'

  def ImageConstants.executable_location(name)
    paths = [ '/usr/local/bin', '/usr/bin' ]
    paths.each do |dir|
      bin = File.join(dir, name)
      return bin if (File.exists?(bin) and File.executable?(bin))
    end
    fail "Can't find '#{name}' on path "  + paths.inspect
  end

  TESSERACT_COMMAND      = ImageConstants.executable_location("tesseract")
  IDENTIFY_EXECUTABLE    = ImageConstants.executable_location("identify")
  CONVERT_EXECUTABLE     = ImageConstants.executable_location("convert")
  PDFTOTEXT_EXECUTABLE   = ImageConstants.executable_location("pdftotext")
  KAKADU_EXECUTABLE      = ImageConstants.executable_location("kdu_expand")
  GHOSTSCRIPT_EXECUTABLE = ImageConstants.executable_location("gs")

end


class Image
  private
  include ImageConstants

  SUPPORTED_IMAGES = [ GIF, JP2, PNG, JPEG, TIFF, PDF ]

  THUNKS = Hash[ GIF => {},  JP2 => {},  PNG => {},  JPEG => {},  TIFF => {},  PDF => {} ]

  CONVERT_TO_BASIC_IMAGE_COMMAND = "#{CONVERT_EXECUTABLE} -quiet -quality 75 -colorspace RGB %m %o"
  CONVERT_TO_JP2_COMMAND = "#{CONVERT_EXECUTABLE} -quiet -quality 75 -define jp2:prg=rlcp -define jp2:numrlvls=7 -define jp2:tilewidth=1024 -define jp2:tileheight=1024 %m %o"
  KAKADU_TO_TIFF_COMMAND = "#{KAKADU_EXECUTABLE} -quiet -i %i -o %o"
  PDF_TO_TEXT_COMMAND = "#{PDFTOTEXT_EXECUTABLE} -quiet -nopgbrk %i %o"
  CONVERT_WITH_LZW_COMPRESSION_COMMAND = "#{CONVERT_EXECUTABLE} -quiet -compress LZW %m %o"

  # CONVERT_PDF_COMMAND = "#{CONVERT_EXECUTABLE} -units PixelsPerInch -density 96 -quiet -compress LZW %m %o"
  # RASTERIZE_PDF_FIRST_PAGE_COMMAND = "#{GHOSTSCRIPT_EXECUTABLE} -o %o -q -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dTextAlphaBits=4 -dGraphicsAlphaBits=4 -dPrinted=false -dFirstPage=1 -dLastPage=1 -r180 -sDEVICE=pngalpha %i"

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
      @kakadu_cached_file  ||= run_command(KAKADU_TO_TIFF_COMMAND, TIFF).path # creates uncompressed TIFF

      case target_mime_type
      when TIFF, PDF
        run_command(CONVERT_WITH_LZW_COMPRESSION_COMMAND, target_mime_type, geometry, @kakadu_cached_file)
      when JP2
        run_command(CONVERT_TO_JP2_COMMAND, target_mime_type, geometry, @kakadu_cached_file)
      when GIF, PNG, JPEG
        run_command(CONVERT_TO_BASIC_IMAGE_COMMAND, target_mime_type, geometry, @kakadu_cached_file)
      when TEXT
        fail "no suppport for JP2 to TEXT (yet)"
      else
        fail "no suppport for JP2 to #{target_mime_type}"
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
      end
      cmd.push str
    end
    cmd = ([ cmd[0], '-resize', geometry ] + cmd[1..-1])  if geometry
    return cmd
  end

  def run(argv)
    puts argv.join(' ')

    data = nil
    errors = nil
    Open3.popen3(*argv) do |stdin, stdout, stderr|
      stdin.close
      data = stdout.gets
      errors = stderr.read
    end

    fail "Errors when creating derivative: '" + errors.gsub("\n", ";  ") + "'" unless errors.nil? or errors.empty?
  end

  # create a derivation for this image, on success return an opened file i/o object for the newly created derivative.

  def run_command(command_template, output_mime_type, geometry=nil, input_file_name=nil)
    input_file_name ||= @file_name
    output_file_name = temp_file_name(output_mime_type)
    cmd = command_template_substitutions(command_template, input_file_name, output_file_name, output_mime_type, geometry)
    run(cmd)

    fail "Derivative creation failed" unless File.exists?(output_file_name)
    fail "Derivative creation failed - empty file"  unless File.stat(output_file_name).size > 0

    return open(output_file_name, 'rb')
  end

  public

  attr_reader :mime_type, :file_path, :file_name, :file_io, :size

  def initialize(path)
    @file_path = path
    @file_name = File.basename(path)
    @temp_files = [ ]
    @file_dimensions = nil

    fail "No such file #{@file_path}"    unless File.exists?   @file_path
    fail "File #{@file_path} unreadable" unless File.readable? @file_path

    @mime_type = get_mime_type
    @file_io = File.open(file_path, 'rb')
    @size = File.stat(@file_path).size

    # GIF conversion routines

    THUNKS[GIF][JP2]  = image_to_jp2()
    THUNKS[GIF][PDF]  = convert_with_lzw_compression(PDF)
    THUNKS[GIF][TIFF] = convert_with_lzw_compression(TIFF)
    THUNKS[GIF][OCR]  = nil
    THUNKS[GIF][HOCR] = nil

    [ GIF, JPEG, PNG ].each { |target_mime_type| THUNKS[GIF][target_mime_type] = convert_to_basic_image(target_mime_type) }

    # JP2 conversion routines

    THUNKS[GIF][OCR]  = nil
    THUNKS[GIF][HOCR] = nil

    [ GIF, JP2, JPEG, PDF, PNG, TIFF ].each { |target_mime_type| THUNKS[JP2][target_mime_type] = jp2_to_image(target_mime_type) }

    # PDF conversion routines

    THUNKS[PDF][TEXT] = pdf_to_text

    [ GIF, JP2, JPEG, PDF, PNG, TIFF ].each { |target_mime_type| THUNKS[PDF][target_mime_type] = pdf_to_image(target_mime_type) }

    # TIFF conversion routines

    THUNKS[TIFF][JP2]  = image_to_jp2
    THUNKS[TIFF][PDF]  = convert_with_lzw_compression(PDF)
    THUNKS[TIFF][TIFF] = convert_with_lzw_compression(TIFF)
    THUNKS[TIFF][HOCR] = nil
    THUNKS[TIFF][OCR]  = nil

    [ GIF, JPEG, PNG ].each { |target_mime_type| THUNKS[TIFF][target_mime_type] = convert_to_basic_image(target_mime_type) }

    # basic images - GIF routines will work:

    THUNKS[JPEG] = THUNKS[GIF]
    THUNKS[PNG]  = THUNKS[GIF]

    yield self

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
    errors = nil
    Open3.popen3(IDENTIFY_EXECUTABLE, file_path) do |stdin, stdout, stderr|
      stdin.close
      data = stdout.gets
      errors = stderr.read
    end
    if data =~ /\s+(\d+)x(\d+)\s+/i
      width, height = $1, $2
      return @file_dimensions = [ width.to_i, height.to_i ]
    else
      return nil
    end
  rescue => e
    return nil
  end

  def convert(target_mime_type, geometry=nil)
    proc = THUNKS[mime_type][target_mime_type]
    fail "unsupported conversion from #{mime_type} to #{target_mime_type}" if proc.nil?
    return proc.call(geometry)
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
    when OCR;    'ocr'
    when HOCR;   'hocr'
    else
      fail "Unexpected mimetype '#{mime_type}'"
    end
  end


  private

  def remove_temp_files
    FileUtils.rm_f @temp_files unless @temp_files.empty?
  rescue
  end

  def get_mime_type(path=nil)
    path ||= file_path
    type  = nil
    error = nil
    Open3.popen3("/usr/bin/file",  "--mime-type",  "-b", path) do |stdin, stdout, stderr|
      type   = stdout.read
      error  = stderr.read
    end
    type.strip!
    type = 'image/jp2'    if (path =~ /\.jp2/i and type == 'application/octet-stream')
    unless SUPPORTED_IMAGES.include? type
      fail "#{path} is not a supported image: it's mime type is '#{type}', but it must be one of '#{SUPPORTED_IMAGES.join("', '")}'"
    end
    return type
  end

  # create an anonymous file name

  def temp_file_name(mime_type)
    tf = Tempfile.new([ file_name_label + '-',  '.' + extension(mime_type) ])
    name = tf.path
    tf.close
    tf.unlink
    @temp_files.push name
    return name
  end

end # of image class

include ImageConstants

input_file_name = ARGV[0]

Image.new(input_file_name) do |image|
  puts "input #{image.mime_type} image #{image.file_name} size: #{image.size}"

  [ GIF, JP2, PNG, JPEG, TIFF, PDF, TEXT, OCR, HOCR ].each do |target|
      begin
        name = "test-from-" + image.file_name_label + "-" + image.extension(image.mime_type) + "-to." + image.extension(target)
        # fd = image.convert(target, '1024x1024')
        fd = image.convert(target)
        open(name, 'w') do |out|
          while (data = fd.read(1024 * 1024))
            out.write data
          end
        end
      rescue => e
        puts "output #{target} image #{name} error: #{e.message}"
      end
  end
end
