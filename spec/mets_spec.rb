$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '../lib'))
$LOAD_PATH.unshift File.expand_path(File.dirname(__FILE__))


require 'open3'
require 'helpers'
require 'offin/mets'
require 'json'

RSpec.describe Mets do

  include MetsHelpers

  describe "#toc" do
    it "correctly returns a table of contents for example double-entried FI05040402 METS" do

      mets = Mets.new(config, test_data_path("FI05040402.mets"))

      expect(mets.valid?).to be_equal(true)

      errors = mets.errors
      expect(errors).to  be_empty

      toc  = TableOfContents.new(mets.structmap)

      report = compare_json(toc.to_json(mets.label),  JSON.parse(File.read(test_data_path("FI05040402.json"))))
      expect(report).to be_empty
    end
  end


  # describe "#toc for double-entried FI05040402 METS is correct" do

  #   mets = Mets.new(config, test_data_path(".mets"))
  #   expect(mets.valid?).to be_equal(true)

  #   errors = mets.errors
  #   expect(errors).to  be_empty

  #   warnings = mets.warnings
  #   expect(warnings).to be_empty

  #   report = compare_json(mets.toc,  File.read(test_data_path(".json")))
  #   expect(report).to be_empty
  # end


  # describe "#toc for double-entried FS66750002rf METS is correct" do

  #   mets = Mets.new(config, test_data_path(".mets"))
  #   expect(mets.valid?).to be_equal(true)

  #   errors = mets.errors
  #   expect(errors).to  be_empty

  #   warnings = mets.warnings
  #   expect(warnings).to be_empty

  #   report = compare_json(mets.toc,  File.read(test_data_path(".json")))
  #   expect(report).to be_empty
  # end

end
