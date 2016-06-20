require 'open3'
require 'tempfile'
require 'fileutils'

        # pdf_to_text_command:                '/usr/bin/pdftotext -nopgbrk'
        # pdf_convert_command:                '/usr/bin/convert -quiet -flatten -quality 75 -colorspace RGB'
        # tesseract_command:                  '/usr/bin/tesseract -l eng'
        #
        # kakadu_expand_command:              '/usr/bin/kdu_expand'
        # image_convert_command:              '/usr/bin/convert -compress LZW'
        #
        # thumbnail_geometry:                 '200x200'      # width, height, for ImageMagick
        # medium_geometry:                    '500x700'
        # large_jpg_geometry:                 '600x800'
        # pdf_preview_geometry:               '500x700'
        # tiff_from_jp2k_geometry:            '1024x1024'


  # TODO:

TESSERACT_COMMAND     = "/usr/bin/tesseract"

IDENTIFY_EXECUTABLE   = "/usr/bin/identify"
CONVERT_EXECUTABLE    = "/usr/local/bin/convert"
PDFTOTEXT_EXECUTABLE  = "/usr/local/bin/pdftotext"
KAKADU_EXECUTABLE     = "/usr/bin/kdu_expnd"

  # %i = inputfile,  %m = inputfile with potentially multiple pages,  %o = outputfile

  PDF_TO_TEXT_COMMAND   = "#{PDFTOTEXT_EXECUTABLE} -nopgbrk %i %o"



  # CONVERT-WITH-COMPRESS (use XXX => PDF, XXX => Compressed TIFF)

  TIFF_TO_PDF_COMMAND   = "#{CONVERT_EXECUTABLE} -quiet -compress LZW %m %o"
  TIFF_COMPRESS_COMMAND = "#{CONVERT_EXECUTABLE} -quiet -compress LZW %m %o"

  # CONVERT-TO-IMAGE (not tiff output, though)

  TIFF_TO_JPEG_COMMAND  = "#{CONVERT_EXECUTABLE} -quiet -quality 75 -colorspace RGB %m %o"
  PDF_TO_JPEG_COMMAND   = "#{CONVERT_EXECUTABLE} -quiet -quality 75 -colorspace RGB %m %o"



  TIFF_TO_JP2_COMMAND   = "#{CONVERT_EXECUTABLE} -quiet -quality 70 -define jp2:prg=rlcp -define jp2:numrlvls=7 -define jp2:tilewidth=1024 -define jp2:tileheight=1024 %m %o"
  JPEG_TO_JP2_COMMAND   = "#{CONVERT_EXECUTABLE} -quiet -quality 70 -define jp2:prg=rlcp -define jp2:numrlvls=7 -define jp2:tilewidth=1024 -define jp2:tileheight=1024 %i %o"

  JP2_TO_TIFF_COMMAND   = "#{KAKADU_EXECUTABLE} -i %i -o %o"  # note: produces uncompressed tiff

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


  SUPPORTED_IMAGES = [ GIF, JP2, PNG, JPEG, TIFF, PDF ]

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
    cmd = (cmd[0] + [ '-resize', geometry ] + cmd[1..-1])  if geometry

    puts cmd.join(' ')

    data = nil
    errors = nil
    Open3.popen3(*cmd) do |stdin, stdout, stderr|
      stdin.close
      data = stdout.gets
      errors = stderr.read
    end
    puts data, errors, `file #{temp_name}`
    return open(temp_name, 'rb')
  end



  CONVERSIONS = { TIFF => {},
                  JP2  => {},
                  PNG  => {},
                  JPEG => {},
                  TIFF => {},
                  PDF  => {},
                }




  attr_reader :mime_type, :file_path, :file_name, :file_io

  def initialize(path)
    @file_path = path
    @file_name = File.basename(path)
    @temp_files = [ ]

    fail "No such file #{@file_path}"    unless File.exists?   @file_path
    fail "File #{@file_path} unreadable" unless File.readable? @file_path

    @mime_type = get_mime_type
    @file_io = File.open(file_path, 'rb')

    yield self

  ensure
    remove_temp_files
  end

  def remove_temp_files
    FileUtils.rm_f @temp_files unless @temp_files.empty?
  rescue
  end


  def file_name_label
    file_name.sub(/\.[^\.]+$/, '')
  end

  def resize(geometry, new_mime_type = nil)

  end

  def stream
    file_io.rewind
    return file_io
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
      fail "#{file_path} is not supported image: it's mime type is '#{type}', but it must be of type '#{SUPPORTED_IMAGES.join("', '")}'"
    end
    return type
  end

  def size
    data = nil
    errors = nil
    Open3.popen3(IDENTIFY_EXECUTABLE, file_path) do |stdin, stdout, stderr|
      stdin.close
      data = stdout.gets
      errors = stderr.read
    end
    if data =~ /\s+(\d+)x(\d+)\s+/i
      width, height = $1, $2
      return width.to_i, height.to_i
    else
      return nil
    end
  rescue => e
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
    else
      fail "Unexpected mimetype #{mime_type}"
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

# want simply image.foo('jpeg', 200x200)

Image.new(ARGV[0]) do |image|

  puts image.mime_type

  puts image.stream.read.size
  puts image.size.inspect


  fd = image.run_command(TIFF_TO_PDF_COMMAND, PDF)
  fd.rewind
  open('test.pdf', 'w') do |out|
    while (data = fd.read(1024 * 1024))
      out.write data
    end
  end
  puts fd.size
end
