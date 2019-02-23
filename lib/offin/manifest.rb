# -*- coding: utf-8 -*-
require 'offin/document-parsers'

# All things manifest.  Parse the XML, etc

# What manifests are all about:
#
#    Manifest Element      | Required | Repeatable    | Allowed Character Data                                  | Notes
#    ----------------------+----------+---------------+---------------------------------------------------------+------------------------------------------------------
#    collection            | yes      | yes           | existing Islandora collection object id                 | will create collection on the fly for digitool
#    contentModel          | yes      | no            | islandora:{sp_pdf,sp_basic_image,sp_large_image_cmodel} |
#    identifier            | no       | yes           | no embedded spaces?                                     | additional identifiers to be saved
#    label                 | no       | no            | any UTF-8 string?                                       | used when displaying the object or object thumbnail
#    otherLogo             | no       | yes           | existing drupal code                                    | determines logo for multibranding
#    owningInstitution     | yes      | no            | FLVC, UF, FIU, FSU, FAMU, UNF, UWF, FIU, FAU, NCF, UCF  |
#    owningUser            | yes      | no            | valid drupal user                                       | should have submitter role across owningInstitution
#    submittingInstitution | no       | no            | FLVC, UF, FIU, FSU, FAMU, UNF, UWF, FIU, FAU, NCF, UCF  | defaults to owningInstitution
#    embargo               | no       | not currently | n/a                                                     | required attribute rangeName, optional expirationDate
#    pageProgression       | no       | no            | rl, lr                                                  | left-to-right or right-to-left pagination
#    languageCode          | no       | no            | three character language code                           | eng, fre, ger, ita
#    ingestPID             | no       | no            | new Islandora object id                                 | must not exist yet
#
# For example.... TODO: manifest.xml here......

class Manifest

  include Errors

  attr_reader :content, :filepath, :config

  def initialize config, filepath
    @config = config
    @filepath = filepath
    @content = File.read(filepath)
    @valid = false

    if @content.empty?
      error "Manifest file '#{@filename}' is empty."
      return
    end

    ManifestSaxDocument.content_models = @config.content_models.keys - [ "islandora:collectionCModel" ]
    ManifestSaxDocument.institutions = @config.institutions

    @manifest_sax_doc = ManifestSaxDocument.new
    Nokogiri::XML::SAX::Parser.new(@manifest_sax_doc).parse(@content)

    @valid     = @manifest_sax_doc.valid

    error   @manifest_sax_doc.errors    if @manifest_sax_doc.errors?
    warning @manifest_sax_doc.warnings  if @manifest_sax_doc.warnings?
  end

  # valid is a boolean that tells us whether the manifest xml document is valid.  This goes beyond schema validation.

  def valid?
    @valid
  end


  # Need to do this the ruby metaprogramming way, whatever that is...

  # Collections, identifiers and other_logos are (possibly empty) lists

  def collections
    @manifest_sax_doc.nil? ?  [] : @manifest_sax_doc.collections
  end

  def identifiers
    @manifest_sax_doc.nil? ?  [] : @manifest_sax_doc.identifiers
  end

  def object_history
    @manifest_sax_doc.nil? ?  [] : @manifest_sax_doc.object_history
  end

  def embargo
    @manifest_sax_doc.nil? ?  [] : @manifest_sax_doc.embargo
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

  def owning_institution
    @manifest_sax_doc.nil? ?  nil : @manifest_sax_doc.owning_institution
  end

  def submitting_institution
    # return nil if @manifest_sax_doc.nil?
    # return (@manifest_sax_doc.submitting_institution || @manifest_sax_doc.owning_institution)
    @manifest_sax_doc.nil? ?  nil : @manifest_sax_doc.submitting_institution
  end

  def owning_user
    @manifest_sax_doc.nil? ?  nil : @manifest_sax_doc.owning_user
  end

  def page_progression
    @manifest_sax_doc.nil? ?  nil : @manifest_sax_doc.page_progression
  end

  def language_code
    @manifest_sax_doc.nil? ?  nil : @manifest_sax_doc.language_code
  end

  def ingest_pid
    @manifest_sax_doc.nil? ?  nil : @manifest_sax_doc.ingest_pid
  end

end
