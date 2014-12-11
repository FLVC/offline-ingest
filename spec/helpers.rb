module UtilRSpecHelpers

  Struct::new('MockConfig',
              'image_to_pdf_command', 'pdf_convert_command', 'kakadu_expand_command', 'image_convert_command',
              'tesseract_command', 'pdf_to_text_command',  'pdf_preview_geometry', 'thumbnail_geometry')

  def config
    return Struct::MockConfig::new("convert -compress LZW",                # image_to_pdf_command
                                   "convert -quality 75 -colorspace RGB",  # pdf_convert_command
                                   "kdu_expand",                           # kakadu_expand_command
                                   "convert -compress LZW",                # image_convert_command
                                   "tesseract -l eng",                     # tesseract_command
                                   "pdftotext -nopgbrk",                   # pdf_to_text_command
                                   "500x700",                              # pdf_preview_geometry
                                   "200x200")                              # thumbnail_geometry
  end

  def test_data base_name
    return File.expand_path(File.join(File.dirname(__FILE__), 'test-data', base_name))
  end

  def jpeg_size file

    info = ""
    errs = ""
    temp = Tempfile.new('pnm-chain-')

    while (data = file.read(1024 ** 2))  do; temp.write data; end
    temp.close

    Open3.popen3("jpegtopnm '#{temp.path}' | pnmfile") do |stdin, stdout, stderr|
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

end
