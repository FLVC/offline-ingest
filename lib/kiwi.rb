require 'cgi'
require 'uri'
require 'net/http'
require 'timeout'
require 'json'

class PurlzError < StandardError;            end
class PurlzTimeoutError < PurlzError;        end
class PurlzAuthorizationError < PurlzError;  end
class PurlzInvalidError < PurlzError;        end
class PurlzNotFoundError < PurlzError;       end


class PurlKiwi

  REDIRECT_TYPE = 302  # integer 301 is for a permanent redirect, 302 is for a temporary redirect (which makes the most sense for a PURL system)

  attr_reader :server_uri, :apikey, :http, :header

  def initialize server_uri, apikey

    @server_uri = URI.parse(server_uri)
    @apikey     = apikey
    @http       = Net::HTTP.new(@server_uri.host, @server_uri.port)
    @http.use_ssl = true
    @header     = build_header
  end

  def to_s
    "<#purl server: #{server_uri}>"
  end


  private

  def build_header

    return { 'Content-type' => 'application/json', 'KiwiApiKey' => apikey } 

  end

  # request_helper
  #
  # Utility function to bundle up http request parameters.

  Struct.new('PurlKiwiInfo', :api_url, :admin_url, :purl_path, :target_url, :redirect_type, :institution)

  def request_helper purl_path, target_url = '', institution = ''

    api_url = '/api/purl/';
    admin_url  = '/admin/purl/' + purl_path.sub(/^\/+/, '')

    # I can't really imagine ever using anything but 302 here, for a purl server

    return Struct::PurlKiwiInfo.new(api_url, admin_url, purl_path, target_url, REDIRECT_TYPE.to_s, institution)
  end

  # assemble_params(DATA) => STRING
  #
  # DATA is Struct::PurlKiwiInfo

  def assemble_request_body data
    request = { :purlPath => data.purl_path, :type => data.redirect_type, :target => data.target_url, :institutionCode => data.institution }
    return request.to_json
  end

  def raise_on_error code, action
    if (code == "400")
      raise PurlzInvalidError, "Error on PURL #{action} - invalid PURL"
    end
    if (code == "401")
      raise PurlzAuthorizationError, "Error on PURL #{action} - invalid API key"
    end
    if (code == "404")
      raise PurlzNotFoundError, "Error on PURL #{action} - PURL not found"
    end
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

  def create purl_path, target_url, institution
    data = request_helper(purl_path, target_url, institution)
    body = assemble_request_body(data)
    response = http.request_post(data.api_url,  body,  header)
    raise_on_error response.code, "create"
    return (response.code == "201")
  rescue TimeoutError => e
    raise PurlzTimeoutError, "Timeout for purl create when connecting to purl server #{server_uri.host}:#{server_uri.port}"
  rescue => error
    raise "Exception: #{error.message}"
  end

  def modify purl_id, purl_path, target_url, institution
    data = request_helper(purl_path, target_url, institution)
    body = assemble_request_body(data)
    response = http.request_put(data.api_url + purl_id, body, header)
    raise_on_error response.code, "update"
    return (response.code == "200")
  rescue TimeoutError => e
    raise PurlzTimeoutError, "Timeout for purl modify when connecting to purl server #{server_uri.host}:#{server_uri.port}"
  rescue => error
    raise "Exception: #{error.message}"
  end

  def delete purl_id
    data = request_helper(purl_id)
    response = http.delete(data.api_url + purl_id, header)
    raise_on_error response.code, "delete"
    return (response.code == "200")
  rescue TimeoutError => e
    raise PurlzTimeoutError, "Timeout for purl delete when connecting to purl server #{server_uri.host}:#{server_uri.port}"
  end

  # exists?(PURL_PATH)
  #
  # The 'exists?' method uses the 'get' method to see if the purl
  # exists, but also checks that any purl status code returned equals
  # "1" ("2" indicates it used to exist, but was deleted); returns
  # purlId/nil as appropriate.

  def exists? purl_path
    record = get(purl_path)
    return nil if record.nil?
    return record["purlId"].to_s  if record["status"] == 1
    return nil
  end

  # tombstoned?(PURL_PATH)
  #
  # A deleted purl can't be re-created (this is referred to as
  # tombstoning the PURL), so we'll need the boolean method
  # 'tombstoned?'

  def tombstoned? purl_path
    record = get(purl_path)
    return false if record.nil?
    return true  if record["status"] == 2
    return false
  end

  # set(PURL_PATH, TARGET_URL, INSTITUTION)
  #
  # Uses create if it's new purl, modify otherwise.  Returns data if
  # successful; nil return means purl was previously deleted and
  # can't be re-created.

  def set purl_path, target_url, institution
    return if tombstoned? purl_path
    purl_id = exists?(purl_path)
    return (purl_id ? modify(purl_id, purl_path, target_url, institution) : create(purl_path, target_url, institution))
  end


  # get(PURL_PATH) => HASH
  #
  # Given a PURL_PATH such as '/fsu/fd/FSDT36939', get it's relevant
  # information (status, target, institution, redirect_type, and id)
  # returned as json.  Returns nil if the purl doesn't exist. 
  # Example:
  #
  #   :id      => "/flvc/fd/fischer003"
  #   :type    => "302"
  #   :uids    => [ 'fischer', 'admin' ]
  #   :gids    => [ "FCLA", "FLVC", "FSU", ]
  #   :target  => "http://islandora7d.fcla.edu/islandora/object/fischer003"
  #   :status  => "1"
  #
  # Note: :status == "2" if the purl was deleted, 'tombstoned' in PURL parlance.

  def get purl_path
    data = request_helper(purl_path)
    response = http.request_get(data.admin_url)
    if (response.code == '200')
      body_json = JSON.parse(response.body)
      return body_json
    end
    return nil
  rescue TimeoutError => e
    raise PurlzTimeoutError, "Timeout in purl fetch when connecting to purl server #{server_uri.host}:#{server_uri.port}"
  rescue => error
    raise "Exception: #{error.message}"
  end

end # of class PurlKiwi
