$LOAD_PATH.unshift '..'


require 'data_mapper'
require 'time'
require 'offin/exceptions'


module DrupalDataBase

  @@table_prefix = ''    # one of our drupal systems (test servers)  uses prefixed tables instead of per-site databases. This will handle that issue.

  def self.table_prefix= str
    str += "_" unless str =~ /_$/
    @@table_prefix = str
  end

  @@debug = false

  def self.debug= bool
    @@debug = bool
  end

  def self.setup config
    DataMapper::Logger.new($stderr, :debug)  if @@debug
    drup = DataMapper.setup(:drupal, config.drupal_database)
    DataMapper.finalize

    # one of our drupal systems (production servers) uses posttrgres schemas instead of per-site databases. This should work with that.

    if config.drupal_default
      drup.select("set search_path to '#{config.drupal_default}'")
    end

    # ping database so we can fail fast

    return drup.select('select 1 + 1') == [ 2 ]

  rescue => e
    raise SystemError, "Fatal error: can't connect to drupal database: #{e.class}: #{e.message}"
  end


  def self.downcase_keys hash
    res = {}
    hash.each { |k,v| res[k.downcase] = v }
    return res
  end

  def self.list_ranges
    name_ids = {}
    repository(:drupal).adapter.select("SELECT name, lid FROM #{@@table_prefix}islandora_ip_embargo_lists").each do |record|
      name_ids[record.name.strip] = record.lid
    end
    return name_ids
  rescue => e
    raise SystemError, "Can't read embargo network range name list from drupal database: #{e.class} #{e.message}."
  end

  def self.is_embargoed? islandora_pid
    rec = repository(:drupal).adapter.select("SELECT lid, expiry FROM #{@@table_prefix}islandora_ip_embargo_embargoes WHERE pid = ?", islandora_pid)
    return (not rec.empty?)
  rescue => e
    raise SystemError, "Can't read embargoes list from drupal database (see config.yml): #{e.class} #{e.message}."
  end

  def self.insert_embargo islandora_pid, range_id, date_ob = nil
    repository(:drupal).adapter.select("INSERT INTO #{@@table_prefix}islandora_ip_embargo_embargoes(pid, lid, expiry) VALUES(?, ?, ?)", islandora_pid, range_id, date_ob)
  rescue => e
    raise SystemError, "Can't insert into drupal database embargoes table for #{islandora_pid}: #{e.class} #{e.message}."
  end

  def self.update_embargo islandora_pid, range_id, date_ob = nil
    repository(:drupal).adapter.select("UPDATE #{@@table_prefix}islandora_ip_embargo_embargoes SET lid = ?, expiry = ? WHERE pid = ?", range_id, date_ob, islandora_pid)
  rescue => e
    raise SystemError, "Can't update drupal database embargoes table for #{islandora_pid}: #{e.class} #{e.message}."
  end

  # Given a well-formed date string 'yyyy-mm-dd', return it as an epcoh-formatted string. Returns nil if string ill-formed.

  def self.check_date str
    time = Time.parse(str)
    return if time.strftime('%F') != str
    return time.strftime('%s').to_s
  rescue => e
    return
  end

  # Given a range name 'str', make sure it's defined (though we ignore case), and return its corresponding finum 'lid'.

  def self.check_range_name str
    return self.downcase_keys(self.list_ranges)[str.downcase]
  rescue => e
    raise SystemError, "Can't check drupal database for embargo ranges"
  end

  def self.add_embargo  islandora_pid, ip_range_name, expiration_date = nil

    epoch_format = nil

    # When expiration_date is given to us as nil, it means there is no
    # explicit expiration date - it is 'forever'.  We can use a nil
    # value for the update.  However, if provided, check_date will
    # return nil on error.

    unless expiration_date.nil?
      epoch_format = self.check_date(expiration_date)
      raise PackageError, "Embargo date '#{expiration_date}' is not a valid date in YYYY-MM-DD format."  if not epoch_format
    end

    range_name_to_id_mappings = self.list_ranges

    unless range_id = self.downcase_keys(range_name_to_id_mappings)[ip_range_name.downcase]
      raise PackageError, "The provided embargo network name '#{ip_range_name}' is not defined - it must be one of \"#{range_name_to_id_mappings.keys.sort.join('", "')}\" - case is not significant."
    end

    if self.is_embargoed? islandora_pid
      self.update_embargo islandora_pid, range_id, epoch_format
    else
      self.insert_embargo islandora_pid, range_id, epoch_format
    end
  end
end

# e.g.
# Struct.new('MockConfig', :drupal_database)
# config = Struct::MockConfig.new("postgres://islandora7:X5r4z!3p@localhost/islandora7")
# DrupalDataBase.debug = true;
# DrupalDataBase.setup(config)
# puts DrupalDataBase.list_ranges.inspect
# DrupalDataBase.add_embargo 'fsu:1', 'fsu campus', '2013-12-01'
# DrupalDataBase.add_embargo 'fsu:4', 'fsu campus'
