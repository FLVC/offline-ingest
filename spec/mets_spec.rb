
$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '../lib'))
$LOAD_PATH.unshift File.expand_path(File.dirname(__FILE__))


require 'open3'
require 'helpers'
require 'offin/mets'
require 'json'

RSpec.describe Mets do

  include MetsHelpers

  [ "FS66750002rf", "FI00534683rf", "FI05040402" ].each do |package_id|

    describe "#toc" do
      it "correctly returns a table of contents for example double-entried #{package_id} METS" do

        mets = Mets.new(config, test_data_path("#{package_id}.mets.xml"))

        expect(mets.valid?).to be_equal(true)

        errors = mets.errors
        expect(errors).to  be_empty

        toc  = TableOfContents.new(mets.structmap)

        report = compare_json(JSON.parse(toc.to_json(mets.label)),  JSON.parse(File.read(test_data_path("#{package_id}.TOC.json"))))
        expect(report).to be_empty
      end
    end
  end

  [ [ "FS66750002rf", 20 ], [ "FI00534683rf", 41 ], [ "FI05040402", 51 ] ].each do |package_id, count|

    describe "#toc" do
      it "correctly returns the number of unique pages in example double-entried #{package_id} METS" do
        mets = Mets.new(config, test_data_path("#{package_id}.mets.xml"))
        toc  = TableOfContents.new(mets.structmap)
        expect(toc.unique_pages.length).to be == count
      end
    end
  end


end
