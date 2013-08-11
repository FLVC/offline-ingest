require 'cgi'
require 'offin/db'

# SqlAssembler is a helper class for our main event here,
# PackageListPaginator.  It lets us gather up the parts of a basic SQL
# select statement in a re-usable way.

Struct.new('SqlFragment', :text, :parameters)

class SqlAssembler

  def initialize
    @select = new_statement_fragment
    @where  = new_statement_fragment
    @order  = new_statement_fragment
    @limit  = new_statement_fragment
  end

  def add_condition text, *parameters
    add @where, text, *parameters
  end

  def set_select text, *parameters
    update @select, text, *parameters
  end

  def set_order text, *parameters
    update @order, text, *parameters
  end

  def set_limit  text, *parameters
    update @limit, text, *parameters
  end

  def execute
    sql, placeholder_values = assemble()
    return repository(:default).adapter.select(sql, *placeholder_values)
  end

  private

  def new_statement_fragment
    fragment = Struct::SqlFragment.new;
    fragment.text = [];
    fragment.parameters = [];
    return fragment
  end

  def update fragment, text, *parameters
    parameters = [] if parameters.nil?
    fragment.text = [ text.strip ]
    fragment.parameters = parameters.flatten
  end

  def add fragment, text, *parameters
    parameters = [] if parameters.nil?
    fragment.text.push text.strip
    fragment.parameters += parameters.flatten
  end

  def assemble

    # We assume exactly one select text; start out with this  "SELECT ..."

    sql_text = @select.text.first
    placeholder_values = @select.parameters

    # handle multiple conditions:  "WHERE ... AND ..."

    unless @where.text.length < 1
      sql_text += " WHERE " + @where.text[0]
    end

    unless @where.text.length < 2
      sql_text += " AND " + @where.text[1..-1].join(" AND ")
    end

    unless @where.parameters.empty?
      placeholder_values.push *@where.parameters
    end

    # we assume zero or one order and limit fragments

    # "ORDER BY..."

    unless @order.text.empty?
      sql_text += " " + @order.text.first
    end

    unless @order.parameters.empty?
      placeholder_values.push *@order.parameters
    end

    # "OFFSET ... LIMIT ..."

    unless @limit.text.empty?
      sql_text += " " + @limit.text.first
    end

    unless @limit.parameters.empty?
      placeholder_values.push *@limit.parameters
    end

    return sql_text, placeholder_values
  end
end


# Paginator is a class to help simplify the page-by-page displays of a
# list of packages, where that list may be growing faster than the
# person can page through it.  It provides a list of
# datamapper::package records based on filtering and pagination data.
#
# It is primarily used in our sinatra data analysis app within view
# templates.

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
    return "/packages" + query_string('before' => @packages.first[:id],  'after' => nil)
  end

  def next_page_list
    return "/packages" + query_string('after' => nil, 'before' => nil) if @packages.empty?
    return "/packages" + query_string('after' => @packages.last[:id],  'before' => nil)
  end

  def first_page_list
    return "/packages" + query_string('after' => nil, 'before' => nil)
  end


  ## TODO: add date support here

  def setup_basic_filters sql

    sql.add_condition('islandora_site_id = ?', @site[:id])

    if val = @params['title']
      sql.add_condition('title ilike ?', "%#{val}%")
    end

    if val = @params['ids']
      sql.add_condition('(package_name ilike ? OR CAST(digitool_id AS TEXT) ilike ? OR islandora_pid ilike ?)', [ "%#{val}%" ] * 3)
    end

    if val = @params['content-type']
      sql.add_condition('content_model = ?', val)
    end

    if @params['status'] == 'warning'
      sql.add_condition('islandora_packages.id IN (SELECT warning_messages.islandora_package_id FROM warning_messages)')
    end

    if @params['status'] == 'error'
      sql.add_condition('islandora_packages.id IN (SELECT error_messages.islandora_package_id FROM error_messages)')
    end

    return sql
  end

  # last_page_list => URL that will take us to the last page of our
  # list of packages (subject to our filters)

  def last_page_list
    skip  = (@count / PACKAGES_PER_PAGE) * PACKAGES_PER_PAGE
    skip -= PACKAGES_PER_PAGE if skip == @count

    sql = setup_basic_filters(SqlAssembler.new)

    sql.set_select('SELECT id FROM islandora_packages')
    sql.set_order('ORDER BY id DESC')
    sql.set_limit('OFFSET ? LIMIT 1', skip)

    ids = sql.execute

    return "/packages" + query_string('after' => nil, 'before' => nil) if ids.empty?
    return "/packages" + query_string('after' => ids[0] + 1,  'before' => nil)
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

    sql = setup_basic_filters(SqlAssembler.new)

    sql.set_select('SELECT min(id), max(id), count(*) FROM islandora_packages')

    rec = sql.execute()[0]

    @min, @max, @count = rec.min, rec.max, rec.count

    # now we can figure out where the page of interest starts (using one of the params 'before', 'after') and get the page-sized list we want:
    # if there are no 'before' or 'after' parameters we'll generate a page list starting from the most recent package.

    sql.set_select('SELECT DISTINCT id FROM islandora_packages')
    sql.set_limit('LIMIT ?', PACKAGES_PER_PAGE)
    sql.set_order('ORDER BY id DESC')

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
