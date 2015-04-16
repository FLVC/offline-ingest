require 'json'


module CommonHelpers
  # give the whole path to a test data file

  def test_data_path filename
    return File.expand_path(File.join(File.dirname(__FILE__), 'test-data', filename))
  end

end

module ModsHelpers

  include CommonHelpers

  Struct::new('ModsMockConfig',
              'schema_directory',
              'mods_to_dc_transform_filename',
              'mods_to_title_transform_filename',
              'mods_post_processing_filename')

  def config
    schema_dir = File.join(File.dirname(__FILE__), "../lib/include/")
    return Struct::ModsMockConfig::new(schema_dir,                                         # schema_directory
                                       File.join(schema_dir, 'mods_to_dc.xsl'),            # mods_to_dc_transform_filename
                                       File.join(schema_dir, 'extract-mods-title.xslt'),   # mods_to_title_transform_filename
                                       File.join(schema_dir, 'modify-serial-mods.xsl'))    # mods_post_processing_filename
  end



end

module MetsHelpers

  include CommonHelpers

  Struct::new('MetsMockConfig',
              'schema_directory')

  def config
    include_dir = File.join(File.dirname(__FILE__), "../lib/include/")
    return Struct::MetsMockConfig::new(include_dir)   # schema_directory
  end

  def compare_json produced_json, expected_json
    report = []

    produced_toc = produced_json['table_of_contents']
    expected_toc = expected_json['table_of_contents']

    if produced_toc.length != expected_toc.length
      report.push "Length differs: produced TOC length #{produced_toc.length} != expected TOC length #{expected_toc.length}"
      return report
    end

    if produced_json["title"] != expected_json["title"]
      report.push "  Titles differ: produced title: '#{produced_json['title']}' != expected: '#{expected_json['title']}'"
    end

    produced_toc.each_index do |i|
      produced_entry = produced_toc[i]
      expected_entry = expected_toc[i]
      differs = []

      unless produced_entry['type'] == expected_entry['type']
        differs.push  "  Types differ:"
        differs.push  "    Produced:  #{produced_entry['type']}"
        differs.push  "    Expected:  #{expected_entry['type']}"
      end

      unless produced_entry['title'] == expected_entry['title']
        differs.push  "  Titles differ:"
        differs.push  "    Produced:  #{produced_entry['title']}"
        differs.push  "    Expected:  #{expected_entry['title']}"
      end

      unless produced_entry['level'] == expected_entry['level']
        differs.push  "  Levels differ:"
        differs.push  "    Produced:  #{produced_entry['level']}"
        differs.push  "    Expected:  #{expected_entry['level']}"
      end

      unless produced_entry['pagenum'].to_i == expected_entry['pagenum'].to_i
        differs.push  "  Pagenums differ:"
        differs.push  "    Produced:  #{produced_entry['pagenum']}"
        differs.push  "    Expected:  #{expected_entry['pagenum']}"
      end

      unless differs.empty?
        report +=  [ "  Element #{i}" ] + differs
      end

    end

    return report
  end

end


module UtilsHelpers

  include CommonHelpers

  Struct::new('MockConfig',
              'pdf_convert_command', 'kakadu_expand_command', 'image_convert_command', 'tesseract_command', 'pdf_to_text_command',  'pdf_preview_geometry', 'thumbnail_geometry', 'supported_ocr_languages')

  # fake the horrible config.yml

  def config
    return Struct::MockConfig::new(
                                   "convert -quiet -quality 75 -colorspace RGB",  # pdf_convert_command
                                   "kdu_expand",                           # kakadu_expand_command
                                   "convert -quiet -compress LZW",                # image_convert_command
                                   "tesseract -l eng",                     # tesseract_command
                                   "pdftotext -nopgbrk",                   # pdf_to_text_command
                                   "500x700",                              # pdf_preview_geometry
                                   "200x200",                              # thumbnail_geometry
                                    {"eng"=>{"tesseract"=>"eng", "name"=>"English"}, "fre"=>{"tesseract"=>"fra", "name"=>"French"}, "ita"=>{"tesseract"=>"ita", "name"=>"Italian"}})
                                   end


  # TODO:  check for NETPBM toolset and throw error if not installed

  def image_size file, command
    info = ""
    errs = ""
    temp = Tempfile.new('pnm-chain-')
    cmd  = sprintf(command, temp.path)

    while (data = file.read(1024 * 2))  do; temp.write data; end
    temp.close

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
