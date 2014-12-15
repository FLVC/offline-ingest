module UtilsHelpers

  Struct::new('MockConfig',
              'pdf_convert_command', 'kakadu_expand_command', 'image_convert_command', 'tesseract_command', 'pdf_to_text_command',  'pdf_preview_geometry', 'thumbnail_geometry')

  # fake the horrible config.yml

  def config
    return Struct::MockConfig::new(
                                   "convert -quality 75 -colorspace RGB",  # pdf_convert_command
                                   "kdu_expand",                           # kakadu_expand_command
                                   "convert -compress LZW",                # image_convert_command
                                   "tesseract -l eng",                     # tesseract_command
                                   "pdftotext -nopgbrk",                   # pdf_to_text_command
                                   "500x700",                              # pdf_preview_geometry
                                   "200x200")                              # thumbnail_geometry
  end



  # give the whole path to a test data file

  def test_data_directory filename
    return File.expand_path(File.join(File.dirname(__FILE__), 'test-data', filename))
  end

  def image_size file, command
    info = ""
    errs = ""
    temp = Tempfile.new('pnm-chain-')

    while (data = file.read(1024 ** 2))  do; temp.write data; end
    temp.close


    cmd = sprintf(command, temp.path)
    Open3.popen3(cmd) do |stdin, stdout, stderr|
      stdin.close
      while (data = stdout.read(1024)) do;  info += data; end
      while (data = stderr.read(1024)) do;  errs += data; end
      stdout.close
      stderr.close
    end

    if info =~ /(\d+)\s+by\s+(\d+)/
      return $1.to_i, $2.to_i
    end
    return

  ensure
    File.unlink temp.path
  end


  def tiff_size file
    return image_size(file, "tifftopnm '%s' | pnmfile")
  end

  # jpeg_size(file) => width, height  - file is a File object open on a JPEG image

  def jpeg_size file

    # Basically, we're doing this:
    #
    # jpegtopnm pdf-test.jpg | pnmfile
    #
    # STDERR: "jpegtopnm: WRITING PPM FILE"
    # STDIN:  "stdin:	PPM raw, 800 by 630  maxval 255"
    #
    # and returning 800, 630

    return image_size(file, "jpegtopnm '%s' | pnmfile")
  end
end
