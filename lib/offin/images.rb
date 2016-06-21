require 'open3'
require 'tempfile'
require 'fileutils'

# TODO: find dynamically


# # %i = inputfile,  %m = inputfile with potentially multiple pages,  %o = outputfile
#
# PDF_TO_TEXT_COMMAND   = "#{PDFTOTEXT_EXECUTABLE} -nopgbrk %i %o"
#
# # CONVERT-WITH-COMPRESS (use XXX => PDF, XXX => Compressed TIFF)
#
# # TIFF_TO_PDF_COMMAND   = "#{CONVERT_EXECUTABLE} -quiet -compress LZW %m %o"
# # TIFF_COMPRESS_COMMAND = "#{CONVERT_EXECUTABLE} -quiet -compress LZW %m %o"
#
# CONVERT_WITH_LZW_COMPRESSION_COMMAND = "#{CONVERT_EXECUTABLE} -quiet -compress LZW %m %o"
#
# # CONVERT-TO-IMAGE (not tiff output, though)
#
# #  TIFF_TO_JPEG_COMMAND  = "#{CONVERT_EXECUTABLE} -quiet -quality 75 -colorspace RGB %m %o"
# #  PDF_TO_JPEG_COMMAND   = "#{CONVERT_EXECUTABLE} -quiet -quality 75 -colorspace RGB %m %o"
#
# CONVERT_TO_BASIC_IMAGE_COMMAND = "#{CONVERT_EXECUTABLE} -quiet -quality 75 -colorspace RGB %m %o"
#
# CONVERT_TO_JP2_COMMAND =  "#{CONVERT_EXECUTABLE} -quiet -quality 70 -define jp2:prg=rlcp -define jp2:numrlvls=7 -define jp2:tilewidth=1024 -define jp2:tileheight=1024 %m %o"
#
# # TIFF_TO_JP2_COMMAND   = "#{CONVERT_EXECUTABLE} -quiet -quality 70 -define jp2:prg=rlcp -define jp2:numrlvls=7 -define jp2:tilewidth=1024 -define jp2:tileheight=1024 %m %o"
# # JPEG_TO_JP2_COMMAND   = "#{CONVERT_EXECUTABLE} -quiet -quality 70 -define jp2:prg=rlcp -define jp2:numrlvls=7 -define jp2:tilewidth=1024 -define jp2:tileheight=1024 %i %o"
#
# KAKADU_TO_TIFF_COMMAND =  "#{KAKADU_EXECUTABLE} -i %i -o %o"  # note: produces uncompressed tiff
#
# # JP2_TO_TIFF_COMMAND   = "#{KAKADU_EXECUTABLE} -i %i -o %o"  # note: produces uncompressed tiff

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


class Image

  def Image.executable_location(name)
    paths = [ '/usr/local/bin', '/usr/bin' ]
    paths.each do |dir|
      bin = File.join(dir, name)
      return bin if (File.exists?(bin) and File.executable?(bin))
    end
    fail "Can't find '#{name}' on path "  + paths.inspect
  end

  TESSERACT_COMMAND     = Image.executable_location("tesseract")
  IDENTIFY_EXECUTABLE   = Image.executable_location("identify")
  CONVERT_EXECUTABLE    = Image.executable_location("convert")
  PDFTOTEXT_EXECUTABLE  = Image.executable_location("pdftotext")
  KAKADU_EXECUTABLE     = Image.executable_location("kdu_expand")

  SUPPORTED_IMAGES = [ GIF, JP2, PNG, JPEG, TIFF, PDF ]

  THUNKS = Hash[ GIF => {},  JP2 => {},  PNG => {},  JPEG => {},  TIFF => {},  PDF => {} ]

  CONVERT_TO_BASIC_IMAGE_COMMAND = "#{CONVERT_EXECUTABLE} -quiet -quality 75 -colorspace RGB %m %o"
  CONVERT_TO_JP2_COMMAND = "#{CONVERT_EXECUTABLE} -quiet -quality 70 -define jp2:prg=rlcp -define jp2:numrlvls=7 -define jp2:tilewidth=1024 -define jp2:tileheight=1024 %m %o"
  KAKADU_TO_TIFF_COMMAND = "#{KAKADU_EXECUTABLE} -i %i -o %o"  # note: produces uncompressed tiff
  PDF_TO_TEXT_COMMAND = "#{PDFTOTEXT_EXECUTABLE} -nopgbrk %i %o"
  CONVERT_WITH_LZW_COMPRESSION_COMMAND = "#{CONVERT_EXECUTABLE} -quiet -compress LZW %m %o"

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
      temp_fd = run_command(KAKADU_TO_TIFF_COMMAND, TIFF)
      temp_file = temp_fd.path
      temp_fd.close
      case target_mime_type
      when TIFF
        return run_command(CONVERT_WITH_LZW_COMPRESSION_COMMAND, target_mime_type, geometry)
      else
        return run_command(CONVERT_TO_BASIC_IMAGE_COMMAND, target_mime_type, geometry)
      end
    end
  end

  def convert_with_lzw_compression(target_mime_type)
    return Proc.new do |geometry|
      run_command(CONVERT_WITH_LZW_COMPRESSION_COMMAND, target_mime_type, geometry)
    end
  end

  def pdf_to_text(target_mime_type)
    return Proc.new do
      run_command(PDF_TO_TEXT_COMMAND, TEXT)
    end
  end

  def run_command(command_template, output_mime_type, geometry=nil)
    cmd = []
    temp_name = temp_file_name(output_mime_type)
    command_template.split(/\s+/).each do |str|
      if str =~ /%i/
        str.sub!(/%i/, file_name)
      end
      if str =~ /%m/
        str.sub!(/%m/, file_name)
        str += '[0]'  if [ TIFF, PDF ].include?(mime_type)
      end
      if str =~ /%o/
        str.sub!(/%o/, temp_name)
      end
      cmd.push str
    end
    cmd = ([ cmd[0], '-resize', geometry ] + cmd[1..-1])  if geometry

    STDERR.puts "Running '" + cmd.join(' ') + "'"
    data = nil
    errors = nil
    Open3.popen3(*cmd) do |stdin, stdout, stderr|
      stdin.close
      data = stdout.gets
      errors = stderr.read
    end

    fail "Derivation errors: '" + errors.gsub("\n", ";  ") + "'" unless errors.nil? or errors.empty?
    fail "Derivation failed" unless File.exists?(temp_name)
    fail "Derivation empty"  unless File.stat(temp_name).size > 0

    return open(temp_name, 'rb')
  end

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
    @size = @file_io.size

    # def convert_to_basic_image(target_mime_type)
    # def image_to_jp2()
    # def jp2_to_image(target_mime_type)
    # def convert_to_lzw(target_mime_type)
    # def pdf_to_text(target_mime_type)

    THUNKS[GIF] = {
      GIF  => convert_to_basic_image(GIF),
      JP2  => image_to_jp2(),
      JPEG => convert_to_basic_image(JPEG),
      PDF  => convert_with_lzw_compression(PDF),
      PNG  => convert_to_basic_image(PNG),
      TEXT => nil,
      TIFF => convert_with_lzw_compression(TIFF),
    }

    THUNKS[JPEG] = THUNKS[GIF]
    THUNKS[PNG]  = THUNKS[GIF]

    # THUNKS[JP2][GIF]  =
    # THUNKS[JP2][JP2]  =
    # THUNKS[JP2][JPEG] =
    # THUNKS[JP2][PDF]  =
    # THUNKS[JP2][PNG]  =
    # THUNKS[JP2][TEXT] =
    # THUNKS[JP2][TIFF] =
    #
    #
    # THUNKS[PDF][GIF]  =
    # THUNKS[PDF][JP2]  =
    # THUNKS[PDF][JPEG] =
    # THUNKS[PDF][PDF]  =
    # THUNKS[PDF][PNG]  =
    # THUNKS[PDF][TEXT] =
    # THUNKS[PDF][TIFF] =
    #
    # THUNKS[PNG][GIF]  =
    # THUNKS[PNG][JP2]  =
    # THUNKS[PNG][JPEG] =
    # THUNKS[PNG][PDF]  =
    # THUNKS[PNG][PNG]  =
    # THUNKS[PNG][TEXT] =
    # THUNKS[PNG][TIFF] =
    #
    # THUNKS[TIFF][GIF]  =
    # THUNKS[TIFF][JP2]  =
    # THUNKS[TIFF][JPEG] =
    # THUNKS[TIFF][PDF]  =
    # THUNKS[TIFF][PNG]  =
    # THUNKS[TIFF][TEXT] =
    # THUNKS[TIFF][TIFF] =

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
    if proc.nil?
      STDERR.puts "unsupported conversion from #{mime_type} to #{target_mime_type}"
    else
      return proc.call(geometry)
    end
  end

  ### PRIVATE

  def remove_temp_files
    FileUtils.rm_f @temp_files unless @temp_files.empty?
  rescue
  end

  def get_mime_type
    type  = nil
    error = nil
    Open3.popen3("/usr/bin/file",  "--mime-type",  "-b", file_path) do |stdin, stdout, stderr|
      type   = stdout.read
      error  = stderr.read
    end
    type.strip!
    type = 'image/jp2'    if (file_path =~ /\.jp2/i and type == 'application/octet-stream')
    unless SUPPORTED_IMAGES.include? type
      fail "#{file_path} is not supported image: it's mime type is '#{type}', but it must be one of '#{SUPPORTED_IMAGES.join("', '")}'"
    end
    return type
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
    else
      fail "Unexpected mimetype '#{mime_type}'"
    end
  end

  # create an anonymous file name

  def temp_file_name(mime_type)
    tf = Tempfile.new([ 'image-process-',  '.' + extension(mime_type) ])
    name = tf.path
    tf.close
    tf.unlink
    @temp_files.push name
    return name
  end

end # of image class


input_filename = ARGV[0]

Image.new(input_filename) do |image|
  puts "input #{image.mime_type} image #{image.file_name} size: #{image.size}"

  [ GIF, JP2, PNG, JPEG, TIFF, PDF, TEXT ].each do |target|
    name = "test-from-" + image.file_name_label + "-" + image.extension(image.mime_type) + "-to." + image.extension(target)

    fd = image.convert(target, '800x800')

    next if fd.nil?

    open(name, 'w') do |out|
      while (data = fd.read(1024 * 1024))
        out.write data
      end
    end

    puts "output #{target} image #{name} size: #{fd.size}"
  end
end

  # convert calls gs like so:

  # /usr/local/bin/gs -q -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT
  # -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2
  # -sDEVICE=pngalpha -dTextAlphaBits=4 -dGraphicsAlphaBits=4 -r72x72
  # -dFirstPage=1 -dLastPage=1
  # -sOutputFile=/var/tmp/magick-8595oe2L5v3Luu7E%d
  # -f/var/tmp/magick-8595Cm2L2mQCJkhp -f/var/tmp/magick-8595F-UHYIb1bnMn

  # but using it directly gives much higher quality:

  # gs -o page.tif \
  #    -q -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT \
  #    -dFirstPage=1 -dLastPage=1 \
  #    -r720x720 \
  #    -sDEVICE=tiff24nc \
  #    -sCompression=lzw \
  #     $1
