require 'cgi'
require 'uri'
require 'net/http'
require 'nokogiri'
require 'timeout'

# Our purl requirements:
# https://docs.google.com/document/d/1TrFsFfAiqpQSCpitDMb6fMblGHcB-_rnEJhUpAtAO0g
#
# Various docs:
# http://purl.oclc.org/docs/help.html
# http://code.google.com/p/persistenturls/wiki/PURLFAQ
#
# TODO: get list of admin groups, existing domains,


class PurlzError < StandardError;            end
class PurlzTimeoutError < PurlzError;        end
class PurlzAuthorizationError < PurlzError;  end


class SaxDocument < Nokogiri::XML::SAX::Document

  # Parse this kind of XML:
  #
  # <purl status="1">
  #   <id>/flvc/fd/fischer003</id>
  #
  #   <type>302</type>
  #
  #   <maintainers>
  #       <uid>admin</uid>
  #       <uid>fischer</uid>
  #       <gid>FCLA</gid>
  #       <gid>FLVC</gid>
  #       <gid>FSU</gid>
  #   </maintainers>
  #
  #   <target>
  #      <url>http://islandora7d.fcla.edu/islandora/object/fischer003</url>
  #   </target>
  # </purl>
  #
  # ...into this kind of hash:
  #
  # { :id      => "/flvc/fd/fischer003",
  #   :gids    => [ "FCLA", "FLVC", "FSU" ],
  #   :uids    => [ "admin", "fischer" ],
  #   :status  => "1",
  #   :target  => "http://islandora7d.fcla.edu/islandora/object/fischer003",
  #   :type    => "302" }

  attr_reader :record

  def initialize
    @current_string = ''
    @status = nil
    @record = { :id => nil,    :type => nil,    :gids => [],    :uids => [],    :target => nil,   :status => nil }
    super()
  end

  def characters string
    @current_string += string.strip
  end

  # stash the status attribute

  def start_element name, attributes = []
    return unless name == 'purl'
    attributes.each { |attribute|  @status = attribute[1] if attribute[0] == 'status' }
  end

  # main event:

  def end_element name
    case name
    when 'id';      record[:id] = @current_string;
    when 'type';    record[:type] = @current_string;
    when 'url';     record[:target] = @current_string;
    when 'gid';     record[:gids].push @current_string;
    when 'uid';     record[:uids].push @current_string;
    when 'purl';    record[:status] = @status
    end
    @current_string = ''
  end
end

# Example use of the Purlz class:
#
# require 'purlz'
#
# purlz = Purlz.new('http://purl.fcla.edu/' ,'admin', 'top secret')
#
# purlz.set('/flvc/fd/fischer003', 'http://islandora7d.fcla.edu/islandora/object/fischer003', 'flvc')                  # create
# purlz.set('/flvc/fd/fischer003', 'http://islandora7d.fcla.edu/islandora/object/fischer003', 'flvc', 'fcla', 'fau')   # modify
#
# # get the data:
# record = purlz.get('/flvc/fd/fischer003') #  => { :id=>"/flvc/fd/fischer003", :type=>"302", :maintainers=>["FCLA", "FLVC", "FAU"],
#                                           #       :status => "1", :target=>"http://islandora7d.fcla.edu/islandora/object/fischer003" }
# # delete the purl:
# purlz.delete('/flvc/fd/fischer003')

class Purlz

  LOGIN_PATH = '/admin/login/login-submit.bsh'
  REDIRECT_TYPE = 302  # integer 301 is for a permanent redirect, 302 is for a temporary redirect (which makes the most sense for a PURL system)

  attr_reader :server_uri, :username, :password, :cookie, :http

  def initialize server_uri, username, password

    @server_uri = URI.parse(server_uri)
    @username   = username
    @password   = password
    @http       = Net::HTTP.new(@server_uri.host, @server_uri.port)
    @cookie     = get_login_cookie()
  end

  def to_s
    "<#purl server: #{server_uri}>"
  end


  private

  # get_login_cookie() => cookie
  #
  # use authentication info from object creation to get an authorization
  # cookie for later HTTP requests.

  def get_login_cookie
    auth_info = [ "id=#{CGI.escape(username)}", "passwd=#{CGI.escape(password)}" ].join('&')

    http.request_post(LOGIN_PATH, auth_info) do |response|
      raise PurlzAuthorizationError, "purl login failed for #{username} on #{server_uri.host}:#{server_uri.port}" if not response or not response['location'] or response['location'] =~ /failure/i
      return { 'Cookie' => 'NETKERNELSESSION=session:' + response['set-cookie'][%r(session:([0-9A-F]+)), 1] }
    end

  rescue TimeoutError => e
    raise PurlzTimeoutError, "Timeout fetching login cookie when connecting to purl server #{server_uri.host}:#{server_uri.port}"
  end

  # request_helper(PURL_ID, optional TARGET_URL, optional *MAINTAINERS) => Struct::PurlInfo
  #
  # Utility function to bundle up http request parameters.  Note that
  # the maintainters parameter can have both users and groups.

  Struct.new('PurlInfo', :admin_url, :target_url, :redirect_type, :maintainers)

  def request_helper purl_id, target_url = '', *maintainers

    admin_url  = '/admin/purl/' + purl_id.sub(/^\/+/, '')
    all_ids    = maintainers.map{ |g| g.strip.downcase }.uniq.join(',')  # includes both gids and uids

    # I can't really imagine ever using anything but 302 here, for a purl server

    return Struct::PurlInfo.new(admin_url, target_url, REDIRECT_TYPE, all_ids)
  end

  # assemble_params(DATA) => STRING
  #
  # DATA is Struct::PurlInfo,  return URL parameter string.

  def assemble_params data
    return [ "target=#{CGI.escape(data.target_url)}",
             "maintainers=#{CGI.escape(data.maintainers)}",
             "type=#{data.redirect_type}" ].join('&')
  end

  # xml_to_hash(XML) => HASH
  #
  # Use sax parser for the XML returned by purl server /admin/purl/id
  # See comments for SaxDocument class above for detailed example.

  def xml_to_hash xml_text
    doc = SaxDocument.new
    Nokogiri::XML::SAX::Parser.new(doc).parse(xml_text)
    return doc.record
  end

  public

  # create(..), delete(..), modify(..)
  #
  # The semantics for a purl server are odd: POST for create; PUT for
  # modification; and, more naturally, DELETE for removal, but DELETE
  # is permanent (can't re-create same purl id).  While external
  # programs probably only need to use 'get', 'exists?', 'delete',
  # 'tombstoned?' and 'set' methods, the lower level 'create', 'modify'
  # methods are also publicly available.  Returns true on success.

  def create purl_id, target_url, *maintainers
    data = request_helper(purl_id, target_url, *maintainers)
    params = assemble_params(data)
    response = http.request_post(data.admin_url,  params,  cookie)
    return (response.code == "201")
  rescue TimeoutError => e
    raise PurlzTimeoutError, "Timeout for purl create when connecting to purl server #{server_uri.host}:#{server_uri.port}"
  end

  def modify purl_id, target_url, *maintainers
    data = request_helper(purl_id, target_url, *maintainers)
    params = assemble_params(data)
    response = http.request_put(data.admin_url + '?' + params, '', cookie)
    return (response.code == "200")
  rescue TimeoutError => e
    raise PurlzTimeoutError, "Timeout for purl modify when connecting to purl server #{server_uri.host}:#{server_uri.port}"
  end

  def delete purl_id
    data = request_helper(purl_id)
    response = http.delete(CGI.escape(data.admin_url), cookie)
    return (response.code == "200")
  rescue TimeoutError => e
    raise PurlzTimeoutError, "Timeout for purl delete when connecting to purl server #{server_uri.host}:#{server_uri.port}"
  end

  # exists?(PURL_ID)
  #
  # The 'exists?' method uses the 'get' method to see if the purl
  # exists, but also checks that any purl status code returned equals
  # "1" ("2" indicates it used to exist, but was deleted); returns
  # true/false as appropriate.

  def exists? purl_id
    record = get(purl_id)
    return false if record.nil?
    return true  if record[:status] == "1"
    return false
  end

  # tombstoned?(PURL_ID)
  #
  # A deleted purl can't be re-created (this is referred to as
  # tombstoning the PURL), so we'll need the boolean method
  # 'tombstoned?'

  def tombstoned? purl_id
    record = get(purl_id)
    return false if record.nil?
    return true  if record[:status] == "2"
    return false
  end

  # set(PURL_ID, TARGET_URL, *MAINTAINERS)
  #
  # Uses create if it's new purl, modify otherwise.  Returns data if
  # successful; nil return means purl was previously deleted and
  # can't be re-created (yuck: do better here).

  def set purl_id, target_url, *maintainers
    return if tombstoned? purl_id
    return (exists?(purl_id) ? modify(purl_id, target_url, *maintainers) : create(purl_id, target_url, *maintainers))
  end


  # get(PURL_ID) => HASH
  #
  # Given a PURL_ID such as '/fsu/fd/FSDT36939', get it's relevant
  # information (status, target, maintainers, redirect_type, and id)
  # returned as a hash (entirely string-valued).  Returns nil if the
  # purl doesn't exist. Example:
  #
  #   :id      => "/flvc/fd/fischer003"
  #   :type    => "302"
  #   :uids    => [ 'fischer', 'admin' ]
  #   :gids    => [ "FCLA", "FLVC", "FSU", ]
  #   :target  => "http://islandora7d.fcla.edu/islandora/object/fischer003"
  #   :status  => "1"
  #
  # Note: :status == "2" if the purl was deleted, 'tombstoned' in PURL parlance.

  def get purl_id
    data = request_helper(purl_id)
    response = http.request_get(data.admin_url)
    return (response.code == '200' ? xml_to_hash(response.body) : nil)
  rescue TimeoutError => e
    raise PurlzTimeoutError, "Timeout in purl fetch when connecting to purl server #{server_uri.host}:#{server_uri.port}"
  end

end # of class Purlz
