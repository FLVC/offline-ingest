# Paginator is a class to help simplify the page-by-page displays of a
# list of packages, where that list may be growing faster than the person
# can page through it.   It provides a list of datamapper::package records.
#
# It is only used in our sinatra app.


require 'offin/db'

class Paginator

  PACKAGES_PER_PAGE = 15

  # Think of a page as descending list of numeric ids (they are in
  # fact the surrogate auto-incremented keys produced for a package
  # table, created when a package starts to get ingested, so this
  # arrangement gives us a reverse chronological browsable list).


  # There are three ways to initialize an object in the paginator class.
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
  # attention to them.
  #
  # There are some additional convenience methods for dealing with
  # setting up links, etc, in a view.
  #
  # SITE is required and is the value returned from DataBase::IslandoraSite.first(:hostname => '...')
  # Note that BEFORE_ID and AFTER_ID are derived from user input and must be sanitized.

  attr_reader :packages

  def initialize site, before_id = nil, after_id = nil
    @site      = site
    @before_id = before_id
    @after_id  = after_id

    @before_id, @after_id = (( before_id and after_id) ? [ nil, nil ] : [ before_id.to_i, after_id.to_i ])

    min = repository(:default).adapter.select("SELECT min(\"id\") FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]}")
    max = repository(:default).adapter.select("SELECT max(\"id\") FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]}")

    @count = repository(:default).adapter.select("SELECT count(*) FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]}")[0]

    @min_id = min.empty? ? nil : min[0]
    @max_id = max.empty? ? nil : max[0]

    @packages  = packages()
  end

  # provide a list of packages

  def packages
    return case
           when @before_id; packages_before
           when @after_id;  packages_after
           else;            packages_start
           end
  end

  def packages_start
    ids = repository(:default).adapter.select("SELECT \"id\" FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]} ORDER BY \"id\" DESC LIMIT #{PACKAGES_PER_PAGE}")
    return [] if ids.empty?
    return DataBase::IslandoraPackage.all(:order => [ :id.desc ], :id => ids)
  end

  def packages_after
    ids = repository(:default).adapter.select("SELECT \"id\" FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]} AND \"id\" < #{@after_id} ORDER BY \"id\" DESC LIMIT #{PACKAGES_PER_PAGE}")
    return [] if ids.empty?
    return DataBase::IslandoraPackage.all(:order => [ :id.desc ], :id => ids)
  end

  def packages_before
    ids = repository(:default).adapter.select("SELECT \"id\" FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]} AND \"id\" > #{@before_id} ORDER BY \"id\" ASC LIMIT #{PACKAGES_PER_PAGE}")
    return [] if ids.empty?
    return DataBase::IslandoraPackage.all(:order => [ :id.desc ], :id => ids)
  end

  def has_next?
    return false if @packages.empty?
    return @packages.last[:id] > @min_id
  end

  def has_previous?
    return false if @packages.empty?
    return @packages.first[:id] < @max_id
  end

  def is_first_page?
    @packages.map { |p| p[:id] }.include? @max_id
  end

  def is_last_page?
    @packages.map { |p| p[:id] }.include? @min_id
  end

  def previous_page
    return "/packages/" if @packages.empty?
    return "/packages/" unless has_previous?
    return "/packages/before=#{@packages.first[:id]}"
  end

  def next_page
    return "/packages/" if @packages.empty?
    return "/packages/" unless has_next?
    return "/packages/after=#{@packages.last[:id]}"
  end

  def first_page
    return "/packages/"
  end

  def last_page
    size_on_last_page  = @count % PACKAGES_PER_PAGE
    size_on_last_page  = PACKAGES_PER_PAGE if size_on_last_page == 0
    ids = repository(:default).adapter.select("SELECT \"id\" FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]} ORDER BY \"id\" desc LIMIT #{size_on_last_page}")
    return "/packages/" if ids.empty?
    return "/packages/after=#{ids[0].to_i + 1}"
  end

end
