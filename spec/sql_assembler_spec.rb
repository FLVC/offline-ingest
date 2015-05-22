$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '../lib'))
$LOAD_PATH.unshift File.expand_path(File.dirname(__FILE__))

require 'offin/sql-assember'
require 'helpers'

RSpec.describe SqlAssembler do

  include SqlAssemblerHelpers

  describe "#.."  do
    it ".." do
    end
  end

end
