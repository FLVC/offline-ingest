require 'offin/document-parsers'

# All things manifest.  Parse the XML, etc

class Manifest

  attr_reader :content, :filepath, :config, :errors, :warnings

  def initialize config, filepath
    @config = config
    @filepath = filepath
    @content = File.read(filepath)
    @errors = []
    @warnings = []
    @valid = false

    if @content.empty?
      @errors.push "Manifest file '#{@filename}' is empty."
      return
    end

    ManifestSaxDocument.content_models = @config.content_models.keys - [ "islandora:collectionCModel" ]
    ManifestSaxDocument.institutions = @config.institutions

    @manifest_sax_doc = ManifestSaxDocument.new
    Nokogiri::XML::SAX::Parser.new(@manifest_sax_doc).parse(@content)

    @valid     = @manifest_sax_doc.valid
    @errors   += @manifest_sax_doc.errors
    @warnings += @manifest_sax_doc.warnings
  end

  # valid is a boolean that tells us whether the manifest xml document is valid.  This goes beyond schema validation.

  def valid?
    @valid
  end

  def warnings?
    not @warnings.empty?
  end

  def errors?
    not @errors.empty?
  end


  # Need to do this the ruby metaprogramming way, whatever that is...

  # Collections, identifiers and other_logos are (possibly empty) lists

  def collections
    @manifest_sax_doc.nil? ?  [] : @manifest_sax_doc.collections
  end

  def identifiers
    @manifest_sax_doc.nil? ?  [] : @manifest_sax_doc.identifiers
  end

  def other_logos
    @manifest_sax_doc.nil? ?  [] : @manifest_sax_doc.other_logos
  end

  # label (title), content_model, owning_user, owning_institution, submitting_institution are strings or nil

  def label
    @manifest_sax_doc.nil? ?  nil : @manifest_sax_doc.label
  end

  def content_model
    @manifest_sax_doc.nil? ?  nil : @manifest_sax_doc.content_model
  end

  def object_history
    @manifest_sax_doc.nil? ?  nil : @manifest_sax_doc.object_history
  end

  def owning_institution
    @manifest_sax_doc.nil? ?  nil : @manifest_sax_doc.owning_institution
  end

  def submitting_institution
    @manifest_sax_doc.nil? ?  nil : @manifest_sax_doc.submitting_institution
  end

  def owning_user
    @manifest_sax_doc.nil? ?  nil : @manifest_sax_doc.owning_user
  end


end
