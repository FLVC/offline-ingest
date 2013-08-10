# Paginator is a class to help simplify the page-by-page displays of a
# list of packages, where that list may be growing faster than the
# person can page through it.  It provides a list of
# datamapper::package records based on filtering and pagination data.
#
# It is primarily used in our sinatra data analysis app within view
# templates.

require 'cgi'
require 'offin/db'

class PackageListPaginator

  PACKAGES_PER_PAGE = 24

  # Think of a page as descending list of numeric IDs (those IDs are
  # in fact the surrogate auto-incremented keys produced for a package
  # table, created when a package starts to get ingested, so this
  # list gives us a reverse chronological browsable list).

  # There are main three ways to initialize an object in the paginator class depending on the params hash (from sinatra)
  #
  # * Provide neither BEFORE_ID nor AFTER_ID.
  #     provide a list of PAGE_SIZE packages from most recent
  #     (highest ID) to lowest.
  #
  # * Provide BEFORE_ID
  #     generate a PAGE_SIZE list of packages that should be displayed
  #     just prior to the ID with value BEFORE_ID
  #
  # * Provide AFTER_ID
  #     generate a PAGE_SIZE list of packages that should be displayed
  #     just after the ID with value AFTER_ID
  #
  # If both BEFORE_ID and AFTER_ID are used, we'll not pay any
  # attention to either of them.
  #
  # There are additional filtering parameters that are applied to the
  # entire packages list before the above logic comes into play.
  #
  # SITE is required and is the value returned from DataBase::IslandoraSite.first(:hostname => '...')
  # Note that BEFORE_ID and AFTER_ID are derived from user input and must be sanitized.

  attr_reader :packages, :count, :comment, :params

  def initialize site, params = {}
    @site = site
    @comment = ''
    @params = params
    ids = process_params  # cleans out unset @params as a side effect
    @packages = DataBase::IslandoraPackage.all(:order => [ :id.desc ], :id => ids)
  end

  # methods to use in views to set links, e.g "<< first < previous | next > last >>" where links which may be inactive, depending.

  def has_next_page_list?
    return false if @packages.empty? or @min.nil?
    return @packages.last[:id] > @min
  end

  def has_previous_page_list?
    return false if @packages.empty? or @max.nil?
    return @packages.first[:id] < @max
  end

  def no_pages?
    @packages.empty?
  end

  def any_pages?
    not @packages.empty?
  end

  def is_first_page_list?
    @packages.map { |p| p[:id] }.include? @max
  end

  def is_last_page_list?
    @packages.map { |p| p[:id] }.include? @min
  end

  def previous_page_list
    return "/packages" + query_string if @packages.empty?
    return "/packages" + query_string('before' => @packages.first[:id],  'after' => nil)
  end

  def next_page_list
    return "/packages" + query_string if @packages.empty?
    return "/packages" + query_string('after' => @packages.last[:id],  'before' => nil)
  end

  def first_page_list
    return "/packages" + query_string('after' => nil, 'before' => nil)
  end

  def last_page_list
    skip  = (@count / PACKAGES_PER_PAGE) * PACKAGES_PER_PAGE
    skip -= PACKAGES_PER_PAGE if skip == @count

    ids = repository(:default).adapter.select("SELECT id FROM islandora_packages WHERE islandora_site_id = ? ORDER BY id DESC OFFSET ? LIMIT 1", @site[:id], skip)
    return "/packages" + query_string if ids.empty?
    return "/packages" + query_string('after' => ids[0] + 1,  'before' => nil)
  end

  def is_content_type? str
    @params['content-type'] == str
  end

  def is_status? str
    @params['status'] == str
  end

  def add_comment str
    @comment = '' unless @comment
    @comment += str + '<br>'
  end


  private

  # We do database queries in two steps.  First, subject to pagination
  # (:before, :after) and search parameters (everything else), we
  # create an SQL statement that selects a page's worth of the DB's
  # islandora_packages.id's, a sequence of integers.  Once that's
  # done, we'll use the id's to instantiate the datamapper objects that
  # will be passed to our view templates.
  #
  # process_params() supplies the logic for this first part. Note that
  # params is user-supplied input and untrustworthy - thus the placeholders.
  #
  # N.B.: this is very PostgreSQL-specific SQL.

  def process_params
    temper_params()

    # TODO: reorder from/to dates

    conditions = []
    placeholder_values = []
    order_by_and_limit   = 'ORDER BY id DESC LIMIT ?'

    @params.keys.each do |name|
      val = @params[name]
      # next unless val and not val.empty?
      case name
      when 'from-date'
        # TODO
      when 'to-date'
        # TODO
      when 'title'
        conditions.push 'title ilike ?'
        placeholder_values.push "%#{val}%"
      when 'ids'
        conditions.push '(CAST(digitool_id AS TEXT) ilike ? OR islandora_pid ilike ? OR package_name ilike ?)'
        placeholder_values += [ "%#{val}%" ] * 3
      when 'content-type'
        conditions.push 'content_model = ?'
        placeholder_values.push val
      when 'status'
        conditions.push 'islandora_packages.id IN (SELECT warning_messages.islandora_package_id FROM warning_messages)'  if val == 'warning'
        conditions.push 'islandora_packages.id IN (SELECT error_messages.islandora_package_id FROM error_messages)'      if val == 'error'
      end
    end

    # First, get the global limits of what our current search criteria would return, and stash in @min, @max, @count:

    sql_text = "SELECT min(id), max(id), count(*) FROM islandora_packages WHERE islandora_site_id = ?"
    parameters = [ @site[:id] ]

    unless conditions.empty?
      sql_text += ' AND ' + conditions.join(' AND ')
      parameters += placeholder_values
    end

    rec = repository(:default).adapter.select(sql_text, *parameters)[0]
    @min, @max, @count = rec.min, rec.max, rec.count

    # now we can add the page restictions (one of the params 'before', 'after') if any, and get the page-sized list we want:

    if val = @params['before']
      conditions.push 'id > ?'
      placeholder_values.push val
      order_by_and_limit  = 'ORDER BY id ASC LIMIT ?'
    end

    if val = @params['after']
      conditions.push 'id < ?'
      placeholder_values.push val
    end

    if conditions.empty?
      sql_text = 'SELECT DISTINCT id FROM islandora_packages WHERE islandora_site_id = ? ' + order_by_and_limit
      parameters = [ @site[:id],  PACKAGES_PER_PAGE ]
    else
      sql_text = 'SELECT DISTINCT id FROM islandora_packages WHERE islandora_site_id = ? AND ' + conditions.join(' AND ') + ' ' + order_by_and_limit
      parameters = [ @site[:id] ] + placeholder_values + [ PACKAGES_PER_PAGE ]
    end

    return repository(:default).adapter.select(sql_text, *parameters)
  end

  def temper_params additional_params = {}
    @params.merge! additional_params
    @params.each { |k,v| @params[k] = v.to_s;  @params.delete(k) if @params[k].empty? }
    # if @params['before'] and @params['after']  # special case - there should be at most one of these, if not, remove both (do we have to?)
    #   @params.delete('before')
    #   @params.delete('after')
    # end
  end

  def query_string additional_params = {}
    temper_params(additional_params)
    pairs = []
    @params.each { |k,v| pairs.push "#{CGI::escape(k)}=#{CGI::escape(v)}" }
    return '' if pairs.empty?
    return '?' + pairs.join('&')
  end

end # of class PackageListPaginator
