require 'cgi'
require 'offin/db'
require 'offin/sql-assembler'
require 'offin/utils'


# Paginator is a class to help simplify the page-by-page displays of a
# list of packages, where that list may be growing faster than the
# person can page through it.  It provides a list of
# datamapper::package records based on filtering and pagination data
# provided by query parameters.
#
# It is used in our sinatra data analysis app within view templates.

class PackageListPaginator

  PACKAGES_PER_PAGE = 16

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

  attr_reader :packages, :count, :comment, :params

  def initialize site, params = {}
    @site     = site
    @params   = params
    @packages = DataBase::IslandoraPackage.all(:order => [ :id.desc ], :id => process_params())
    @comment  = nil
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
    return "/packages" + query_string('after' => nil, 'before' => nil) if @packages.empty?
    return "/packages" + query_string('after' => nil, 'before' => @packages.first[:id])
  end

  def next_page_list
    return "/packages" + query_string('before' => nil, 'after' => nil) if @packages.empty?
    return "/packages" + query_string('before' => nil, 'after' => @packages.last[:id])
  end

  def first_page_list
    return "/packages" + query_string('after' => nil, 'before' => nil)
  end

  def csv_link
    "/csv" + query_string('after' => nil, 'before' => nil)
  end


  def offset_to_last_page
    skip  = (@count / PACKAGES_PER_PAGE) * PACKAGES_PER_PAGE
    skip -= PACKAGES_PER_PAGE if skip == @count
    return skip
  end

  # last_page_list => URL that will take us to the last page of our
  # list of packages (subject to our filters)

  def last_page_list

    sql = Utils.setup_basic_filters(SqlAssembler.new, @params.merge('site_id' => @site[:id]))

    sql.set_select 'SELECT id FROM islandora_packages'
    sql.set_order  'ORDER BY id DESC'
    sql.set_limit  'OFFSET ? LIMIT 1', offset_to_last_page

    ids = sql.execute

    return "/packages" + query_string('before' => nil, 'after' => nil) if ids.empty?
    return "/packages" + query_string('before' => nil, 'after' => ids[0] + 1)
  end


  # parameter checking convience funtions for view, e.g.  :select => paginator.is_content_type?('islandora:sp_pdf')

  def is_content_type? str
    @params['content-type'] == str
  end

  def is_status? str
    @params['status'] == str
  end

  # add a comment to place on a page - useful for debugging

  def add_comment str
    @comment = '' unless @comment
    @comment += str + '<br>'
  end

  def comment?
    @comment
  end


  private

  # We do database queries in two steps.  First, subject to page positions
  # (:before, :after) and search parameters (everything else), we
  # create an SQL statement that selects a page's worth of the DB's
  # islandora_packages.id's, a sequence of integers.  Once that's
  # done, we'll use the id's to instantiate the datamapper objects that
  # will be passed to our view templates.
  #
  # process_params() supplies the logic for this first part. Note that
  # @params is user-supplied input and untrustworthy - thus we use
  # placeholders.
  #
  # N.B.: some of this is very PostgreSQL-specific SQL.

  def process_params
    temper_params()

    # first: find the limits of our package set - largest id, smallest id, total siet

    sql = Utils.setup_basic_filters(SqlAssembler.new, @params.merge('site_id' => @site[:id]))

    sql.set_select 'SELECT min(id), max(id), count(*) FROM islandora_packages'

    rec = sql.execute()[0]

    @min, @max, @count = rec.min, rec.max, rec.count

    # now we can figure out where the page of interest starts (using one of the params 'before', 'after') and get the page-sized list we want;
    # if there are no 'before' or 'after' parameters we'll generate a page list starting from the most recent package (i.e. the first page)

    sql.set_select 'SELECT DISTINCT id FROM islandora_packages'
    sql.set_limit  'LIMIT ?', PACKAGES_PER_PAGE
    sql.set_order  'ORDER BY id DESC'

    if val = @params['before']
      sql.add_condition('id > ?', val)
      sql.set_order('ORDER BY id ASC')
    end

    if val = @params['after']
      sql.add_condition('id < ?', val)
    end

    return sql.execute
  end

  def temper_params additional_params = {}
    @params.merge! additional_params
    @params.each { |k,v| @params[k] = v.to_s;  @params.delete(k) if @params[k].empty? }

    if @params['before'] and @params['after']  # special case - there should be at most one of these, if not, remove both (do we really have to do this? it's mostly defensive programming...)
       @params.delete('before')
       @params.delete('after')
    end
  end

  def query_string additional_params = {}
    temper_params(additional_params)
    pairs = []
    @params.each { |k,v| pairs.push "#{CGI::escape(k)}=#{CGI::escape(v)}" } # ecaping the key is purely defensive.
    return '' if pairs.empty?
    return '?' + pairs.join('&')
  end

end # of class PackageListPaginator
