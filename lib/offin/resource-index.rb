require 'rest-client'

# TODO: exception handling. Connection errors should be treated transiently, i.e. give up on processing this particular package.

class ResourceIndex

  attr_reader :url, :results, :xml

  def initialize config

    address    = config['fedora-address']    # e.g. localhost:8080
    scheme     = config['scheme']  || 'http'

    @username  = config['fedora-username']
    @password  = config['fedora-password']

    @url = "#{scheme}://#{address}/fedora/risearch"

    # TODO: sanity check for values above
    # TODO: initially check for a connection, maybe establish a cookiejar in the process?
  end

  # TODO: exception handling, e.g., server unreachable, etc.

  def sparql str
    params = {}
    params['type'] = 'tuples'
    params['lang'] = 'sparql'
    params['format'] = 'Sparql'
    params['limit'] = ''
    params['query'] = str

    feed(params)
  end

  def itql str
    params = {}
    params['type'] = 'tuples'
    params['lang'] = 'itql'
    params['format'] = 'Sparql'
    params['limit'] = ''
    params['query'] = str

    feed(params)
  end


  def feed params
    # params['stream'] = 'off'

    t1 = Time.now
    xml = RestClient::Resource.new(@url, @username, @password).post params
    t2 = Time.now
    STDERR.puts "Query took #{(t2 - t1) * 1000} ms"

    return parse_sparql(xml)
  end

  # return a hash (pid => title) of all collections in the system

  def collections
    records = {}
    itql("select $object $title from <#ri> where ($object <fedora-model:label> $title and $object <fedora-model:hasModel> <info:fedora/islandora:collectionCModel>)").each do |rec|
      records[rec.object.sub('info:fedora/', '')] = rec.title
    end

    return records
  end


  private

  # shorten the query string for logging, enough to give us a clue about what was executed

  def query_hint str
    if str =~ /(^.*?<#ri>)/
      return "\"#{$1}...\""
    else
      return '"' + str + '"'
    end
  end

  # parse the Sparql XML returned by the query, and return a list of OpenStructs with that data

  def parse_sparql text
    sax_document = SaxDocumentExtractSparql.new
    Nokogiri::XML::SAX::Parser.new(sax_document).parse(text)
    return sax_document.results
  end


end
