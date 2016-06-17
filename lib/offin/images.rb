require 'open3'
require 'tempfile'


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



class Image

  GIF  = 'image/gif'
  JP2  = 'image/jp2'
  PNG  = 'image/png'
  JPEG = 'image/jpeg'
  TIFF = 'image/tiff'
  PDF  = 'application/pdf'

  CONVERSIONS = { TIFF => {},
                  JP2  => {},
                  PNG  => {},
                  JPEG => {},
                  TIFF => {},
                  PDF  => {},
                }

  CONVERSIONS[PDF][TIFF] = [ "convert", "-quiet", "-flatten", "-quality", "75", "-colorspace", "RGB", "-resize", "%RESIZE_GEOMETRY", "%INPUT_FILE_NAME", "%OUTPUT_FILE_NAME" ]




  SUPPORTED_IMAGES = [  GIF,   JP2,   PNG,   JPEG,   TIFF,   PDF   ]

  attr_reader :mime_type, :file_path, :file_name, :file_io

  def initialize(path)
    @file_path = path
    @file_name = File.basename(path)
    @files = [ path ]

    fail "No such file #{@file_path}"    unless File.exists?   @file_path
    fail "File #{@file_path} unreadable" unless File.readable? @file_path

    @mime_type = get_mime_type
    @file_io = File.open(file_path, 'rb')
  end

  def close
    @files.each { |file| file.close if file.respond_to? :close and file.respond_to? :closed? and not file.closed? }
  rescue
  end


  def text
    case mime_type
    when PDF;
    end
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
    Open3.popen3('identify', file_path) do |stdin, stdout, stderr|
      stdin.close
      data = stdout.gets
      errors = stderr.read
    end
    puts errors
    if data =~ /\s+(\d+)x(\d+)\s+/i
      width, height = $1, $2
      return width.to_i, height.to_i
    else
      return nil
    end
  rescue => e
    return nil
  end

  def extension
    case mime_type
    when GIF;    'gif'
    when JPEG;   'jpg'
    when PNG;    'png'
    when TIFF;   'tiff'
    when JP2;    'jp2'
    when PDF;    'pdf'
    end
  end


  def temp_file # creat an anonymous file handle
    tmpf = Tempfile.new([ 'image-process-',  '.' + extension ])
    @files.push tmpf.path
  end

  # return file/io streams)

  def convert(new_type)
    return stream if new_type == mime_type
    # command =

  end


end # of image class



image = Image.new(ARGV[0])

puts image.mime_type

puts image.stream.read.size
puts image.size.inspect
