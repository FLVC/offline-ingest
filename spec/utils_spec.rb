$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '../lib'))
$LOAD_PATH.unshift File.expand_path(File.dirname(__FILE__))

require 'open3'
require 'offin/utils'
require 'helpers'

RSpec.describe Utils do

  include UtilsHelpers

  describe "#mime_type"  do
    it "recognizes JPEG files from an open JPEG File object" do
      type = Utils.mime_type(open(test_data_path("pdf-test.jpg")))
      expect(type).to  eq('image/jpeg')
    end
  end

  describe "#mime_type"  do
    it "recognizes JPEG files from a JPEG file path" do
      type = Utils.mime_type(test_data_path("pdf-test.jpg"))
      expect(type).to  eq('image/jpeg')
    end
  end

  describe "#mime_type"  do
    it "recognizes PNG files from an open PNG File object" do
      type = Utils.mime_type(open(test_data_path("pdf-test.png")))
      expect(type).to  eq('image/png')
    end
  end

  describe "#mime_type"  do
    it "recognizes PNG files from a PNG file path" do
      type = Utils.mime_type(test_data_path("pdf-test.png"))
      expect(type).to  eq('image/png')
    end
  end

  describe "#mime_type"  do
    it "recognizes GIF files from an open GIF File object" do
      type = Utils.mime_type(open(test_data_path("pdf-test.gif")))
      expect(type).to  eq('image/gif')
    end
  end

  describe "#mime_type"  do
    it "recognizes GIF files from a GIF file path" do
      type = Utils.mime_type(test_data_path("pdf-test.gif"))
      expect(type).to  eq('image/gif')
    end
  end

  describe "#mime_type"  do
    it "recognizes TIFF files from an open TIFF File object" do
      type = Utils.mime_type(open(test_data_path("pdf-test.tiff")))
      expect(type).to  eq('image/tiff')
    end
  end

  describe "#mime_type"  do
    it "recognizes TIFF files from a TIFF file path" do
      type = Utils.mime_type(test_data_path("pdf-test.tiff"))
      expect(type).to  eq('image/tiff')
    end
  end

  describe "#mime_type"  do
    it "recognizes PDF files from an open PDF File object" do
      type = Utils.mime_type(open(test_data_path("practical-sailor.pdf")))
      expect(type).to  eq('application/pdf')
    end
  end

  describe "#mime_type"  do
    it "recognizes PDF files from a PDF file path" do
      type = Utils.mime_type(test_data_path("practical-sailor.pdf"))
      expect(type).to  eq('application/pdf')
    end
  end

  describe "#mime_type"  do
    it "recognizes JP2 files from an open JP2 File object" do
      pending('Limitations of /usr/bin/file returns octet mime-type for JP2 on STDIN')
      type = Utils.mime_type(open(test_data_path("sample01.jp2")))
      expect(type).to  eq('image/jp2')
    end
  end

  describe "#mime_type"  do
    it "recognizes JP2 files from a JP2 file path" do
      type = Utils.mime_type(test_data_path("sample01.jp2"))
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
      file, errors = Utils.image_to_pdf(config, test_data_path("pdf-test.jpg"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('application/pdf')
    end
  end

  describe "#image_to_pdf" do
    it "produces a PDF from a valid TIFF image file without error" do
      file, errors = Utils.image_to_pdf(config, test_data_path("pdf-test.tiff"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('application/pdf')
    end
  end

  describe "#image_to_pdf" do
    it "returns without errors a File object on the PDF produced from a valid GIF image file" do
      file, errors = Utils.image_to_pdf(config, test_data_path("pdf-test.gif"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('application/pdf')
    end
  end

  describe "#image_to_pdf" do
    it "returns with out errors a File object on the PDF produced from a valid PNG image file without error" do
      file, errors = Utils.image_to_pdf(config, test_data_path("pdf-test.png"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('application/pdf')
    end
  end

  describe "#image_to_pdf" do
    it "produces error diagnostics when it receives an invalid image" do
      file, errors = Utils.image_to_pdf(config, test_data_path("garbage.rand"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end

  describe "#pdf_to_text"  do
    it "returns a File object with the extracted text from a PDF file" do
      file, errors = Utils.pdf_to_text(config, test_data_path("practical-sailor.pdf"))
      expect(errors).to be_empty
      expect(file.stat.size).to be > 0
      expect(file.read).to match(/published monthly by Belvoir Publications Inc./)
    end
  end

  describe "#pdf_to_text"  do
    it "creates error diagnostic messages for an invalid file" do
      file, errors = Utils.pdf_to_text(config, test_data_path("garbage.rand"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end

  describe "#pdf_to_thumbnail"  do
    it "returns without errors a File object on the JPEG thumbnail produced from a valid PDF file" do
      file, errors = Utils.pdf_to_thumbnail(config, test_data_path("practical-sailor.pdf"))
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('image/jpeg')

      if RUBY_PLATFORM !~ /linux/
        # The ImageMagick convert command under Linux can produce warning messages on successful conversions - they show up in the error log.  For example:
        #
        # When creating a preview image from the PDF '/usr/local/islandora/offline-ingest/spec/test-data/practical-sailor.pdf' with command 'convert -quality 75 -colorspace RGB' the following message was produced:
        # **** Warning:  File has an invalid xref entry:  19.  Rebuilding xref table.
        # **** This file had errors that were repaired or ignored.
        # **** The file was produced by:
        # **** >>>> Mac OS X 10.10.1 Quartz PDFContext <<<<
        # **** Please notify the author of the software that produced this
        # **** file that it does not conform to Adobe's published PDF
        # **** specification
        expect(errors).to be_empty
      end
    end
  end

  describe "#pdf_to_thumbnail"  do
    it "returns an array of error diagnostic messages for an invalid file" do
      file, errors = Utils.pdf_to_thumbnail(config, test_data_path("garbage.rand"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end

  describe "#pdf_to_thumbnail"  do
    it "returns without errors a File object on the JPEG thumbnail produced from a valid PDF file, where the JPEG is less than a 200x200 image size" do
      file, errors = Utils.pdf_to_thumbnail(config, test_data_path("practical-sailor.pdf"))
      width, height = jpeg_size(file)

      expect(width).to  be_a(Fixnum)
      expect(height).to be_a(Fixnum)

      expect(width).to  be <= 200
      expect(height).to be <= 200
    end
  end

  describe "#pdf_to_preview"  do
    it "returns without errors a File object on the JPEG preview produced from a valid PDF file" do
      file, errors = Utils.pdf_to_preview(config, test_data_path("practical-sailor.pdf"))
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('image/jpeg')
      if RUBY_PLATFORM !~ /linux/
        # See note on #pdf_to_thumbnail above
        expect(errors).to be_empty
      end
    end
  end

  describe "#pdf_to_preview"  do
    it "returns an array of error diagnostic messages for an invalid file" do
      file, errors = Utils.pdf_to_preview(config, test_data_path("garbage.rand"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end

  describe "#pdf_to_preview"  do
    it "returns without errors a File object on the JPEG preview produced from a valid PDF file, where the JPEG is less than a 500x700 image size" do
      file, errors = Utils.pdf_to_preview(config, test_data_path("practical-sailor.pdf"))
      width, height = jpeg_size(file)

      expect(width).to  be_a(Fixnum)
      expect(height).to be_a(Fixnum)

      expect(width).to  be <= 500
      expect(height).to be <= 700
    end
  end

  describe "#image_to_tiff" do
    it "returns without errors a File object on the TIFF produced from a valid JP2K file" do
      file, errors = Utils.image_to_tiff(config, test_data_path("sample01.jp2"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('image/tiff')
    end
  end

  describe "#image_to_tiff" do
    it "returns without errors a File object on the TIFF produced from a valid, though problematic, JP2K file" do
      file, errors = Utils.image_to_tiff(config, test_data_path("problematic.jp2"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('image/tiff')
    end
  end

  describe "#image_to_tiff" do
    it "returns an array of error diagnostic messages for a broken (partial-sized) JP2K file" do
      file, errors = Utils.image_to_tiff(config, test_data_path("broken.jp2"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end

  describe "#image_to_tiff" do
    it "returns an array of error diagnostic messages for an invalid file" do
      file, errors = Utils.image_to_tiff(config, test_data_path("garbage.rand"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end

  describe "#image_to_jpeg" do
    it "returns without errors a File object on the JPEG produced from a valid JP2K file" do
      pending("Huh. convert doesn't support JP2K input on my Mac OS X (macports)") if RUBY_PLATFORM =~ /darwin/i

      file, errors = Utils.image_to_jpeg(config, test_data_path("sample01.jp2"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('image/jpeg')
    end
  end

  describe "#image_to_jpeg" do
    it "returns without errors a File object on the JPEG produced from a valid, though problematic, JP2K file" do
      pending("Huh. convert doesn't support JP2K input on my Mac OS X (macports)") if RUBY_PLATFORM =~ /darwin/i

      file, errors = Utils.image_to_jpeg(config, test_data_path("problematic.jp2"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('image/jpeg')
    end
  end

  describe "#image_to_jpeg" do
    it "returns an array of error diagnostic messages for an invalid file" do
      file, errors = Utils.image_to_jpeg(config, test_data_path("garbage.rand"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end

  describe "#image_to_jpeg" do
    it "returns an array of error diagnostic messages for a broken (partial-sized) JP2K file" do
      file, errors = Utils.image_to_jpeg(config, test_data_path("broken.jp2"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end

  describe "#image_to_jp2k" do
    it "returns without errors a File object on the JP2K produced from a valid JP2K file" do
      pending("Huh. convert doesn't support JP2k output on my Mac OS X (macports)") if RUBY_PLATFORM =~ /darwin/i

      file, errors = Utils.image_to_jp2k(config, test_data_path("edward-text.tiff"))
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('application/octet-stream')    # DOH!
    end
  end

  describe "#image_to_jp2k" do
    it "returns an array of error diagnostic messages for an invalid file" do
      file, errors = Utils.image_to_jp2k(config, test_data_path("garbage.rand"))
      expect(errors.length).to be > 1
      expect(file.stat.size).to be == 0
    end
  end

  # This is left for future pondering: right now, there really isn't a
  # good way to detect this broken image - it is the first 15K from a
  # valid jpeg2k.  Doesn't matter: we really don't guarentee valid
  # images, as long as we can convert them (other tests handle that).

  # describe "#image_to_jp2k" do
  #   it "returns an array of error diagnostic messages for a broken (partial-sized) JP2K file" do
  #     file, errors = Utils.image_to_jp2k(config, test_data_path("broken.jp2"))
  #     expect(errors.length).to be > 1
  #     expect(file.stat.size).to be == 0
  #   end
  # end

  describe "#ocr" do
    it "returns a String of text extracted from a TIFF image" do
      text = Utils.ocr(config, test_data_path("edward-text.tiff"))
      expect(text).to be_a_kind_of(String)
      expect(text).to match(/glorious summer/i)
    end
  end

  describe "#ocr" do
    it "returns a String of text extracted from a JP2K image" do
      text = Utils.ocr(config, test_data_path("edward-text.jp2"))
      expect(text).to be_a_kind_of(String)
      expect(text).to match(/glorious summer/i)
    end
  end

  describe "#ocr" do
    it "returns nil when  attempting to extract text from an unsupported file" do
      text = Utils.hocr(config, test_data_path("garbage.rand"))
      expect(text).to be_nil
    end
  end

  describe "#hocr" do
    it "returns a String of text extracted from an image" do
      text = Utils.hocr(config, test_data_path("edward-text.tiff"))
      expect(text).to be_a_kind_of(String)
      expect(text).to match(/glorious<\/span>/i)
      expect(text).to match(/summer.<\/span>/i)
    end
  end

  describe "#image_resize" do
    it "returns a uniformly scaled image of the same type" do
      resized, errors = Utils.image_resize config, test_data_path("edward-text.tiff"), "50x50"
      expect(errors).to be_empty
      expect(resized).to be_a_kind_of(File)
      expect(Utils.mime_type(resized)).to eq("image/tiff")
    end
  end

  describe "#image_resize" do
    it "returns a uniformly scaled image to fit in a given geometry" do
      resized, errors = Utils.image_resize config, test_data_path("edward-text.tiff"), "50x50"
      width, height = tiff_size(resized)

      expect(width).to  be_a(Fixnum)
      expect(height).to be_a(Fixnum)

      expect(width).to  be <= 50
      expect(height).to be <= 50
    end
  end

  describe "#image_resize" do
    it "returns a uniformly scaled JPEG image to fit in a given geometry properly when given a problematic JP2" do
      resized, errors = Utils.image_resize config, test_data_path("problematic.jp2"), "500x700", 'jpeg'
      width, height = jpeg_size(resized)

      expect(width).to  be_a(Fixnum)
      expect(height).to be_a(Fixnum)

      expect(width).to  be <= 500
      expect(height).to be <= 700
    end
  end

  describe "#image_resize" do
    it "returns a uniformly scaled image of a new type" do
      resized, errors = Utils.image_resize config, test_data_path("edward-text.tiff"), "50x50", 'jpeg'
      expect(errors).to be_empty
      expect(resized).to be_a_kind_of(File)
      expect(Utils.mime_type(resized)).to eq("image/jpeg")
    end
  end


  describe "#image_resize" do
    it "returns a uniformly scaled image to fit in a given geometry when changed to a new type" do
      resized, errors = Utils.image_resize config, test_data_path("edward-text.tiff"), "50x50", 'jpeg'
      width, height = jpeg_size(resized)

      expect(width).to  be_a(Fixnum)
      expect(height).to be_a(Fixnum)

      expect(width).to  be <= 50
      expect(height).to be <= 50
    end
  end

  describe "#image_resize" do
    it "returns a uniformly scaled image, and the new type is (incorrectly) specified as the same input type" do
      resized, errors = Utils.image_resize config, test_data_path("edward-text.tiff"), "50x50", 'tiff'
      expect(errors).to be_empty
      expect(resized).to be_a_kind_of(File)
      expect(Utils.mime_type(resized)).to eq("image/tiff")
    end
  end

  describe "#image_size" do
    it "properly returns the size of a TIFF image as two fixnums - width and height" do
      width, height = Utils.size config, test_data_path("edward-text.tiff")
      expect(width).to eq(714)
      expect(height).to eq(216)
    end
  end

  describe "#image_size" do
    it "properly returns the size of a JP2K image as two fixnums - width and height" do
      pending("Huh. convert doesn't support JP2K input on my Mac OS X (macports)") if RUBY_PLATFORM =~ /darwin/i
      width, height = Utils.size config, test_data_path("sample01.jp2")
      expect(width).to eq(1728)
      expect(height).to eq(2376)
    end
  end

  describe "#image_size" do
    it "properly returns the size of a JPEG image as two fixnums - width and height" do
      width, height = Utils.size config, test_data_path("pdf-test.jpg")
      expect(width).to eq(800)
      expect(height).to eq(630)
    end
  end

  describe "#image_size" do
    it "properly returns the size of a GIF image as two fixnums - width and height" do
      width, height = Utils.size config, test_data_path("pdf-test.gif")
      expect(width).to eq(800)
      expect(height).to eq(630)
    end
  end

  describe "#image_size" do
    it "properly returns the size of a PNG image as two fixnums - width and height" do
      width, height = Utils.size config, test_data_path("pdf-test.png")
      expect(width).to eq(800)
      expect(height).to eq(630)
    end
  end

  describe "#image_size" do
    it "returns the size of an invalid image as nil" do
      width, height = Utils.size config, test_data_path("garbage.rand")
      expect(width).to be_nil
      expect(height).to be_nil
    end
  end

  describe "#image_size" do
    it "returns the size of an empty image as nil" do
      width, height = Utils.size config, "/dev/null"
      expect(width).to be_nil
      expect(height).to be_nil
    end
  end

  describe "#image_size" do
    it "returns the size of a missing image as nil" do
      width, height = Utils.size config, test_data_path("erguihgjtgkjsjfsdjfjf")
      expect(width).to be_nil
      expect(height).to be_nil
    end
  end

  describe "#langs_to_tesseract_command_line" do
    it "extracts the proper tesseract codes for supported languages" do
      expect(Utils.langs_to_tesseract_command_line(config, "eng", "fre", "ger", "ita")).to eq("-l eng -l fra -l deu -l ita")
      expect(Utils.langs_to_tesseract_command_line(config)).to eq("-l eng")
    end
  end

  describe "#langs_to_names" do
    it "provides proper english description for supported languages" do
      expect(Utils.langs_to_names(config, "eng", "fre", "ger", "ita")).to eq("English, French, German, Italian")
      expect(Utils.langs_to_names(config, "eng")).to eq("English")
      expect(Utils.langs_to_names(config, "XXX")).to eq("English")
      expect(Utils.langs_to_names(config)).to eq("English")
    end
  end

  describe "#langs_unsupported_comment" do
    it "provides a string listing unsupported language codes" do
      expect(Utils.langs_unsupported_comment(config, "XX")).to eq("XX")
      expect(Utils.langs_unsupported_comment(config, "XX", "XY")).to eq("XX, XY")
      stuff = [ "XX",  "YY",  "ZZ" ]
      expect(Utils.langs_unsupported_comment(config, *stuff)).to eq("XX, YY, ZZ")
      stuff = [ ]
      expect(Utils.langs_unsupported_comment(config, *stuff)).to eq("")
    end
  end



end # of "RSpec.describe Utils do"
