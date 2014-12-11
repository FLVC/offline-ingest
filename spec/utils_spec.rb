$LOAD_PATH.unshift File.expand_path(File.dirname(__FILE__), '../lib')
require 'offin/utils'
require 'open3'

# TODO: move following to helper (does @config get recreated?)

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

  while (data = file.read(1024))  do; temp.write data; end
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
  else
    return
  end
ensure
  File.unlink temp.path
end


RSpec.describe Utils do

  describe "#mime_type"  do
    it "recognizes JPEG files from an open JPEG File object" do
      type = Utils.mime_type(open(test_data("pdf-test.jpg")))
      expect(type).to  eq('image/jpeg')
    end
  end

  describe "#mime_type"  do
    it "recognizes JPEG files from a JPEG file path" do
      type = Utils.mime_type(test_data("pdf-test.jpg"))
      expect(type).to  eq('image/jpeg')
    end
  end

  describe "#mime_type"  do
    it "recognizes PNG files from an open PNG File object" do
      type = Utils.mime_type(open(test_data("pdf-test.png")))
      expect(type).to  eq('image/png')
    end
  end

  describe "#mime_type"  do
    it "recognizes PNG files from a PNG file path" do
      type = Utils.mime_type(test_data("pdf-test.png"))
      expect(type).to  eq('image/png')
    end
  end

  describe "#mime_type"  do
    it "recognizes GIF files from an open GIF File object" do
      type = Utils.mime_type(open(test_data("pdf-test.gif")))
      expect(type).to  eq('image/gif')
    end
  end

  describe "#mime_type"  do
    it "recognizes GIF files from a GIF file path" do
      type = Utils.mime_type(test_data("pdf-test.gif"))
      expect(type).to  eq('image/gif')
    end
  end

  describe "#mime_type"  do
    it "recognizes TIFF files from an open TIFF File object" do
      type = Utils.mime_type(open(test_data("pdf-test.tiff")))
      expect(type).to  eq('image/tiff')
    end
  end

  describe "#mime_type"  do
    it "recognizes TIFF files from a TIFF file path" do
      type = Utils.mime_type(test_data("pdf-test.tiff"))
      expect(type).to  eq('image/tiff')
    end
  end

  describe "#mime_type"  do
    it "recognizes PDF files from an open PDF File object" do
      type = Utils.mime_type(open(test_data("practical-sailor.pdf")))
      expect(type).to  eq('application/pdf')
    end
  end

  describe "#mime_type"  do
    it "recognizes PDF files from a PDF file path" do
      type = Utils.mime_type(test_data("practical-sailor.pdf"))
      expect(type).to  eq('application/pdf')
    end
  end

  describe "#mime_type"  do
    it "recognizes JP2 files from an open JP2 File object" do
      pending('Limitations of /usr/bin/file returns octet mime-type for JP2 on STDIN')
      type = Utils.mime_type(open(test_data("sample01.jp2")))
      expect(type).to  eq('image/jp2')
    end
  end

  describe "#mime_type"  do
    it "recognizes JP2 files from a JP2 file path" do
      type = Utils.mime_type(test_data("sample01.jp2"))
      expect(type).to  eq('image/jp2')
    end
  end

  describe "#temp_file" do
    it "creates a temporary file that we can read from and write to" do
      io = Utils.temp_file
      io.write "This is a test\n"
      io.rewind
      expect(io.read).to eq("This is a test\n")
    end
  end

  describe "#image_to_pdf" do
    it "produces a PDF from a valid JPEG image without error" do
      file, errors = Utils.image_to_pdf(config, test_data("pdf-test.jpg"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('application/pdf')
    end
  end

  describe "#image_to_pdf" do
    it "produces a PDF from a valid TIFF image file without error" do
      file, errors = Utils.image_to_pdf(config, test_data("pdf-test.tiff"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('application/pdf')
    end
  end

  describe "#image_to_pdf" do
    it "returns without errors a File object on the PDF produced from a valid GIF image file" do
      file, errors = Utils.image_to_pdf(config, test_data("pdf-test.gif"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('application/pdf')
    end
  end

  describe "#image_to_pdf" do
    it "returns with out errors a File object on the PDF produced from a valid PNG image file without error" do
      file, errors = Utils.image_to_pdf(config, test_data("pdf-test.png"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('application/pdf')
    end
  end

  describe "#image_to_pdf" do
    it "produces error diagnostics when it receives an invalid image" do
      file, errors = Utils.image_to_pdf(config, test_data("garbage.rand"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end

  describe "#pdf_to_text"  do
    it "returns a File object with the extracted text from a PDF file" do
      file, errors = Utils.pdf_to_text(config, test_data("practical-sailor.pdf"))
      expect(errors).to be_empty
      expect(file.stat.size).to be > 0
      expect(file.read).to match(/published monthly by Belvoir Publications Inc./)
    end
  end

  describe "#pdf_to_text"  do
    it "creates error diagnostic messages for an invalid file" do
      file, errors = Utils.pdf_to_text(config, test_data("garbage.rand"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end

  describe "#pdf_to_thumbnail"  do
    it "returns without errors a File object on the JPEG thumbnail produced from a valid PDF file" do
      file, errors = Utils.pdf_to_thumbnail(config, test_data("practical-sailor.pdf"))
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('image/jpeg')
      if RUBY_PLATFORM =~ /linux/
        # The linux convert command can produce warning messages on successful conversions - they show up in the error log.
        # For example:
        #
        # When creating a preview image from the PDF '/usr/local/islandora/offline-ingest/spec/test-data/practical-sailor.pdf' with command 'convert -quality 75 -colorspace RGB' the following message was produced:         # **** Warning:  File has an invalid xref entry:  19.  Rebuilding xref table.
        # **** This file had errors that were repaired or ignored.
        # **** The file was produced by:
        # **** >>>> Mac OS X 10.10.1 Quartz PDFContext <<<<
        # **** Please notify the author of the software that produced this
        # **** file that it does not conform to Adobe's published PDF
        # **** specification
      else
        expect(errors).to be_empty
      end
    end
  end

  describe "#pdf_to_thumbnail"  do
    it "returns an array of error diagnostic messages for an invalid file" do
      file, errors = Utils.pdf_to_thumbnail(config, test_data("garbage.rand"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end

  describe "#pdf_to_thumbnail"  do
    it "returns without errors a File object on the JPEG thumbnail produced from a valid PDF file, where the JPEG is less than a 200x200 image size" do
      file, errors = Utils.pdf_to_thumbnail(config, test_data("practical-sailor.pdf"))
      width, height = jpeg_size(file)

      expect(width).to  be_a(Fixnum)
      expect(height).to be_a(Fixnum)

      expect(width).to  be <= 200
      expect(height).to be <= 200
    end
  end


  describe "#pdf_to_preview"  do
    it "returns without errors a File object on the JPEG preview produced from a valid PDF file" do
      file, errors = Utils.pdf_to_preview(config, test_data("practical-sailor.pdf"))
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('image/jpeg')
      if RUBY_PLATFORM =~ /linux/
        # See note on #pdf_to_thumbnail above
      else
        expect(errors).to be_empty
      end
    end
  end

  describe "#pdf_to_preview"  do
    it "returns an array of error diagnostic messages for an invalid file" do
      file, errors = Utils.pdf_to_preview(config, test_data("garbage.rand"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end

  describe "#pdf_to_preview"  do
    it "returns without errors a File object on the JPEG preview produced from a valid PDF file, where the JPEG is less than a 500x700 image size" do
      file, errors = Utils.pdf_to_preview(config, test_data("practical-sailor.pdf"))
      width, height = jpeg_size(file)

      expect(width).to  be_a(Fixnum)
      expect(height).to be_a(Fixnum)

      expect(width).to  be <= 500
      expect(height).to be <= 700
    end
  end

  describe "#kakadu_jp2k_to_tiff" do
    it "returns without errors a File object on the TIFF produced from a valid JP2K file" do
      file, errors = Utils.kakadu_jp2k_to_tiff(config, test_data("sample01.jp2"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('image/tiff')
    end
  end

  describe "#kakadu_jp2k_to_tiff" do
    it "returns an array of error diagnostic messages for an invalid file" do
      file, errors = Utils.kakadu_jp2k_to_tiff(config, test_data("garbage.rand"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end


  describe "#image_magick_to_tiff" do
    it "returns without errors a File object on the TIFF produced from a valid JP2K file" do

      pending("Huh. convert doesn't support TIFF output on my Mac OS X (macports)") if RUBY_PLATFORM =~ /darwin/i

      file, errors = Utils.image_magick_to_tiff(config, test_data("sample01.jp2"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('image/tiff')

    end
  end

  describe "#image_magick_to_tiff" do
    it "returns an array of error diagnostic messages for an invalid file" do
      file, errors = Utils.image_magick_to_tiff(config, test_data("garbage.rand"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end


  # TODO: we need two different kids of JP2K images here, one that
  # ImageMagick can't handle (so it punts to kakadu) and one that it
  # can.

  describe "#image_to_tiff" do
    it "returns without errors a File object on the TIFF produced from a valid JP2K file" do
      file, errors = Utils.image_to_tiff(config, test_data("sample01.jp2"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('image/tiff')
    end
  end

  describe "#image_to_tiff" do
    it "returns an array of error diagnostic messages for an invalid file" do
      file, errors = Utils.image_to_tiff(config, test_data("garbage.rand"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end

  describe "#ocr" do
    it "returns a String of text extracted from an image" do
      text = Utils.ocr(config, test_data("edward-text.tiff"))
      expect(text).to be_a_kind_of(String)
      expect(text).to match(/glorious summer/i)
    end
  end

  describe "#hocr" do
    it "returns a String of text extracted from an image" do
      text = Utils.hocr(config, test_data("edward-text.tiff"))
      expect(text).to be_a_kind_of(String)
      expect(text).to match(/glorious<\/span>/i)
      expect(text).to match(/summer.<\/span>/i)
    end
  end

  # describe "#"  do
  #   it "" do
  #   end
  # end

end
