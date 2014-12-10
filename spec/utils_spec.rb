$LOAD_PATH.unshift File.expand_path(File.dirname(__FILE__), '../lib')
require 'offin/utils'
require 'open3'

# TODO: move following to helper (does @config get recreated?)

Struct::new 'MockConfig',  'image_to_pdf_command', 'pdf_convert_command',  'pdf_to_text_command',  'pdf_preview_geometry', 'thumbnail_geometry'

def config
  return Struct::MockConfig::new("convert -compress LZW",                # image_to_pdf_command
                                 "convert -quality 75 -colorspace RGB",  # pdf_convert_command
                                 "pdftotext -nopgbrk",                   # pdf_to_text_command
                                 "500x700",                              # pdf_preview_geometry
                                 "200x200")                              # thumbnail_geometry
end

def test_data base_name
  return File.expand_path(File.join(File.dirname(__FILE__), 'test-data', base_name))
end

def jpeg_size file
  info = ""
  Open3.popen3("jpegtopnm | pnmfile") do |stdin, stdout, stderr|
    while (data = file.read(1024 * 8))
      stdin.write data
    end
    stdin.close
    while (data = stdout.read(1024))
      info += data
    end
    stderr.read
  end

  if info =~ /(\d+)\s+by\s+(\d+)/
    return $1.to_i, $2.to_i
  else
    return
  end
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
      pending('limitations in /usr/bin/file only allow return octet-type')
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
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('image/jpeg')
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
      expect(errors).to be_empty
      expect(file).to be_a_kind_of(File)
      expect(Utils.mime_type(file)).to  eq('image/jpeg')
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



  # describe "#"  do
  #   it "" do
  #   end
  # end

end
