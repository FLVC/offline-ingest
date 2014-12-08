$LOAD_PATH.unshift File.expand_path(File.dirname(__FILE__), '../lib')
require 'offin/utils'

# TODO: move following to helper (does @config get recreated?)

Struct::new 'MockConfig',  'image_to_pdf_command'

def config
  return Struct::MockConfig::new("#{`which convert`.strip} -compress LZW")   # :image_to_pdf_command
end


def test_data base_name
  return File.expand_path(File.join(File.dirname(__FILE__), 'test-data', base_name))
end



RSpec.describe Utils do
  it "can create a temporary file we can read and write to" do
    io = Utils.temp_file
    io.write "This is a test\n"
    io.rewind
    expect(io.read).to eq("This is a test\n")
  end
end


RSpec.describe Utils do
  it "can produce a PDF from a JPEG image without error" do
    file, errors = Utils.image_to_pdf(config, test_data("pdf-test.jpg"))
    expect(errors).to be_empty
    expect(file).to be_a_kind_of(File)
    expect(Utils.mime_type(file)).to  eq('application/pdf')
  end
end

RSpec.describe Utils do
  it "can produce a PDF from a JPEG image without error" do
    file, errors = Utils.image_to_pdf(config, test_data("pdf-test.tiff"))
    expect(errors).to be_empty
    expect(file).to be_a_kind_of(File)
    expect(Utils.mime_type(file)).to  eq('application/pdf')
  end
end
