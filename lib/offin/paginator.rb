# Paginator is a class to help simplify the page-by-page displays of a
# list of packages, where that list may be growing faster than the
# person can page through it.  It provides a list of
# datamapper::package records based on filtering and pagination data.
#
# It is primarily used in our sinatra data analysis app within view
# templates.

require 'offin/db'

class PackageListPaginator

  PACKAGES_PER_PAGE = 20

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

  attr_reader :packages, :count, :comment

  def initialize site, params = {}
    @site = site

    sql_text, placeholder_values = process_params(params)  # cleans out unset params as a side effect


######## debugging..

    foo = "SELECT DISTINCT id FROM islandora_packages WHERE islandora_site_id = ? AND #{sql_text} ORDER BY id DESC LIMIT ?"

    bar = [ @site[:id] ] + placeholder_values +  [ PACKAGES_PER_PAGE ]

    @comment  = "params: " + params.inspect + "<br>"
    @comment += "SQL: \"#{sql_text}\", " + placeholder_values.map { |vl| vl.inspect }.join(', ')
    @comment += "<BR>"
    @comment += foo + ", " + bar.map { |vl| vl.inspect }.join(', ')

    ids = repository(:default).adapter.select(foo, *bar)

    @comment += "<BR>" + ids.inspect

########


    case
    when (params[:before] and params[:after]);   @before_id, @after_id = nil, nil
    when (params[:before]);                      @before_id, @after_id = params[:before].to_i, nil
    when (params[:after]);                       @before_id, @after_id = nil, params[:after].to_i
    else;                                        @before_id, @after_id = nil, nil
    end

    rec = repository(:default).adapter.select("SELECT min(id), max(id), count(*) FROM islandora_packages WHERE islandora_site_id = ?", @site[:id])[0]
    @min, @max, @count = rec.min, rec.max, rec.count

    @packages = list_packages()
  end

  # provide a list of packages

  def list_packages
    return case
           when @before_id; packages_before
           when @after_id;  packages_after
           else;            packages_start
           end
  end

  def packages_start
    ids = repository(:default).adapter.select("SELECT id FROM islandora_packages WHERE islandora_site_id = ? ORDER BY id DESC LIMIT ?", @site[:id], PACKAGES_PER_PAGE)
    return [] if ids.empty?
    return DataBase::IslandoraPackage.all(:order => [ :id.desc ], :id => ids)
  end

  def packages_after
    ids = repository(:default).adapter.select("SELECT id FROM islandora_packages WHERE islandora_site_id = ? AND id < ? ORDER BY id DESC LIMIT ?", @site[:id], @after_id, PACKAGES_PER_PAGE)
    return [] if ids.empty?
    return DataBase::IslandoraPackage.all(:order => [ :id.desc ], :id => ids)
  end

  def packages_before
    ids = repository(:default).adapter.select("SELECT id FROM islandora_packages WHERE islandora_site_id = ? AND id > ? ORDER BY id ASC LIMIT ?", @site[:id], @before_id, PACKAGES_PER_PAGE)
    return [] if ids.empty?
    return DataBase::IslandoraPackage.all(:order => [ :id.desc ], :id => ids)
  end

  # methods to use in views to set links, e.g "<< first < previous | next > last >>" where links which may be inactive, depending.

  def has_next_page_list?
    return false if @packages.empty? or @min_id.nil?
    return @packages.last[:id] > @min_id
  end

  def has_previous_page_list?
    return false if @packages.empty? or @max_id.nil?
    return @packages.first[:id] < @max_id
  end

  def is_first_page_list?
    @packages.map { |p| p[:id] }.include? @max_id
  end

  def is_last_page_list?
    @packages.map { |p| p[:id] }.include? @min_id
  end

  def previous_page_list
    return "/packages" if @packages.empty?
    return "/packages" unless has_previous_page_list?        # just defensive: use has_previous_page_list? before calling this
    return "/packages?before=#{@packages.first[:id]}"
  end

  def next_page_list
    return "/packages" if @packages.empty?
    return "/packages" unless has_next_page_list?           # just defensive: use has_next_page_list? before calling this
    return "/packages?after=#{@packages.last[:id]}"
  end

  def first_page_list
    return "/packages"
  end

  def last_page_list
    skip  = (@count / PACKAGES_PER_PAGE) * PACKAGES_PER_PAGE
    skip -= PACKAGES_PER_PAGE if skip == @count

    ids = repository(:default).adapter.select("SELECT id FROM islandora_packages WHERE islandora_site_id = ? ORDER BY id DESC OFFSET ? LIMIT 1", @site[:id], skip)
    return "/packages" if ids.empty?
    return "/packages?after=#{ids[0].to_i + 1}"
  end


  private

  # We do database queries in two steps.  First, subject to pagination
  # (:before, :after) and search parameters (everything else), we
  # create an SQL statement that selects a page's worth of the DB's
  # islandora_packages.id's, a sequence of integers.  Once that's
  # done, we'll use the id's to instantiate the datamapper objects that
  # will be passed to our view templates.
  #
  # process_params(params) supplies the logic for this first part. Note that
  # params is user-supplied input and untrustworthy - thus the placeholders.
  #
  # Note also: this is very PostgreSQL specific SQL.

  def process_params params

    # we remove non-values from params, which will modify the params hash for the caller:

    params.each { |k,v| params.delete(k) if v.nil? or v.empty? }

    # special case - there should be at most one of these, if not, remove both:

    if params['before'] and params['after']
      params.delete('before')
      params.delete('after')
    end

    # special case: reorder from/to dates
    # TODO

    conditions = []
    placeholder_values = []
    order_by   = 'ORDER BY id DESC LIMIT ?'

    params.keys.each do |name|
      val = params[name]
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
      when 'before'
        conditions.push 'id > ?'
        placeholder_values.push val
        order_by  = 'ORDER BY id ASC LIMIT ?'
      when 'after'
        conditions.push 'id < ?'
        placeholder_values.push val
      end

    end

    # TODO: just do sql calls here...

    return ('SELECT DISTINCT id FROM islandora_packages WHERE islandora_site_id = ? ' + conditions.join(' AND ') + ' ' + order_by), ([ @site[:id] ] + placeholder_values + [ PACKAGES_PER_PAGE ])
  end
end



# this is like above, will add
