require 'data_mapper'
require 'time'
require 'offin/exceptions'


# A class for querying drupal databases.  While we use the datamapper
# library to set up the connections, this really doesn't make much use
# of the datamapper objects.


class DrupalDataBase

  @@debug = false

  def self.debug= bool
    @@debug = bool
  end

  def initialize config
    @config = config

    # We seem to have three different kinds of drupal database setups;
    # sometimes we support multiple hosts with underscores before
    # table names; sometimes with schemas, sometimes there are more

    if str = config.drupal_table_prefix
      str += "_" unless str =~ /_$/
      @table_prefix = str
    else
      @table_prefix = ''
    end

    if str = config.drupal_user_table_prefix
      str += "_" unless str =~ /_$/
      @user_table_prefix = str
    else
      @user_table_prefix = ''
    end

    @schema_required = config.drupal_schema
    @user_schema_required = config.drupal_user_schema

    @db = DrupalDataBase.setup @config

    set_search_path

  end

  def set_search_path
    @db.select("set search_path = '#{@schema_required}'") if @schema_required
  rescue => e
    return
  end

  def set_user_search_path
    @db.select("set search_path = '#{@user_schema_required}'") if @user_schema_required
  rescue => e
    return
  end


  def self.setup config
    DataMapper::Logger.new($stderr, :debug)  if @@debug
    drup = DataMapper.setup(:drupal, config.drupal_database)
    DataMapper.finalize

    # do something on database so we can fail fast

    drup.select('select 1 + 1')

    return drup
  rescue => e
    raise SystemError, "Fatal error: can't connect to drupal database: #{e.class}: #{e.message}"
  end



  def downcase_keys hash
    res = {}
    hash.each { |k,v| res[k.downcase] = v }
    return res
  end

  def list_ranges
    name_ids = {}
    set_search_path
    @db.select("SELECT name, lid FROM #{@table_prefix}islandora_ip_embargo_lists").each do |record|
      name_ids[record.name.strip] = record.lid
    end
    return name_ids
  rescue => e
    puts e.backtrace
    raise SystemError, "Can't read embargo network range name list from drupal database: #{e.class} #{e.message}."
  end

  def is_embargoed? islandora_pid
    rec = @db.select("SELECT lid, expiry FROM #{@table_prefix}islandora_ip_embargo_embargoes WHERE pid = ?", islandora_pid)
    return (not rec.empty?)
  rescue => e
    raise SystemError, "Can't read embargoes list from drupal database (see config.yml): #{e.class} #{e.message}."
  end

  def insert_embargo islandora_pid, range_id, date_ob = nil
    @db.select("INSERT INTO #{@table_prefix}islandora_ip_embargo_embargoes(pid, lid, expiry) VALUES(?, ?, ?)", islandora_pid, range_id, date_ob)
  rescue => e
    raise SystemError, "Can't insert into drupal database embargoes table for #{islandora_pid}: #{e.class} #{e.message}."
  end

  def update_embargo islandora_pid, range_id, date_ob = nil
    @db.select("UPDATE #{@table_prefix}islandora_ip_embargo_embargoes SET lid = ?, expiry = ? WHERE pid = ?", range_id, date_ob, islandora_pid)
  rescue => e
    raise SystemError, "Can't update drupal database embargoes table for #{islandora_pid}: #{e.class} #{e.message}."
  end

  def users
    set_user_search_path
    @db.select("SELECT name FROM #{@user_table_prefix}users").map { |name| name.strip }.select { |name| not name.empty? }
  rescue => e
    return []
  ensure
    set_search_path
  end

  # Given a well-formed date string 'yyyy-mm-dd', return it as an epoch-formatted string. Returns nil if string ill-formed.

  def check_date str
    time = Time.parse(str)
    return if time.strftime('%F') != str
    return time.strftime('%s').to_s
  rescue => e
    return
  end

  # Given a range name 'str', make sure it's defined (though we ignore case), and return its corresponding finum 'lid'.

  def check_range_name str
    return downcase_keys(list_ranges)[str.downcase]
  rescue => e
    raise SystemError, "Can't check drupal database for embargo ranges"
  end

  def add_embargo  islandora_pid, ip_range_name, expiration_date = nil

    epoch_format = nil

    # When expiration_date is given to us as nil, it means there is no
    # explicit expiration date - it is 'forever'.  We can use a nil
    # value for the update.  However, if a non-nil value is provided,
    # check_date will return nil on error.

    unless expiration_date.nil?
      epoch_format = check_date(expiration_date)
      raise PackageError, "Embargo date '#{expiration_date}' is not a valid date in YYYY-MM-DD format."  if not epoch_format
    end

    range_name_to_id_mappings = list_ranges

    unless range_id = downcase_keys(range_name_to_id_mappings)[ip_range_name.downcase]
      raise PackageError, "The provided embargo network name '#{ip_range_name}' is not defined - it must be one of \"#{range_name_to_id_mappings.keys.sort.join('", "')}\" - case is not significant."
    end

    if is_embargoed? islandora_pid
      update_embargo islandora_pid, range_id, epoch_format
    else
      insert_embargo islandora_pid, range_id, epoch_format
    end
  end
end
