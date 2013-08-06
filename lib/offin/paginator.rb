# Paginator is a class to help simplify the page-by-page displays of a
# list of packages, where that list may be growing faster than the person
# can page through it.   It provides a list of datamapper::package records.
#
# It is only used in our sinatra app.


require 'offin/db'

class PackagePaginator

  PACKAGES_PER_PAGE = 20

  # Think of a page as descending list of numeric IDs (those IDs are
  # in fact the surrogate auto-incremented keys produced for a package
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

  attr_reader :packages, :comment

  def initialize site, before_id = nil, after_id = nil
    @site      = site

    case
    when (before_id and after_id);   @before_id, @after_id = nil, nil
    when (before_id);                @before_id, @after_id = before_id.to_i, nil
    when (after_id);                 @before_id, @after_id = nil, after_id.to_i
    else;                            @before_id, @after_id = nil, nil
    end

    min = repository(:default).adapter.select("SELECT min(\"id\") FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]}")
    max = repository(:default).adapter.select("SELECT max(\"id\") FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]}")

    @count = repository(:default).adapter.select("SELECT count(*) FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]}")[0]

    @min_id = min.empty? ? nil : min[0]
    @max_id = max.empty? ? nil : max[0]

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

  def has_next_page?
    return false if @packages.empty?
    return @packages.last[:id] > @min_id
  end

  def has_previous_page?
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
    return "/packages" if @packages.empty?
    return "/packages" unless has_previous_page?
    return "/packages?before=#{@packages.first[:id]}"
  end

  def next_page
    return "/packages" if @packages.empty?
    return "/packages" unless has_next_page?
    return "/packages?after=#{@packages.last[:id]}"
  end

  def first_page
    return "/packages"
  end

  def last_page
    skip  = (@count / PACKAGES_PER_PAGE) * PACKAGES_PER_PAGE
    skip -= PACKAGES_PER_PAGE if skip == @count

    ids = repository(:default).adapter.select("SELECT \"id\" FROM \"islandora_packages\" WHERE \"islandora_site_id\" = #{@site[:id]} ORDER BY \"id\" desc OFFSET #{skip} LIMIT 1")
    return "/packages" if ids.empty?
    return "/packages?after=#{ids[0].to_i + 1}"
  end

end
