require 'data_mapper'
require 'time'
require 'offin/exceptions'


module DrupalDataBase

  def self.debug= bool
    @@debug = bool
  end

  def self.setup config
    DataMapper::Logger.new($stderr, :debug)  if @@debug
    drup = DataMapper.setup(:drupal, config.drupal_database)

    # repository(:drupal).adapter.resource_naming_convention = DataMapper::NamingConventions::Resource::UnderscoredAndPluralizedWithoutModule
    DataMapper.finalize

    # ping database so we can fail fast

    return drup.select('select 1 + 1') == [ 2 ]
  rescue => e
    raise SystemError, "Fatal error: can't connect to drupal database: #{e.class}: #{e.message}"
  end

  def self.list_ranges
    name_ids = {}
    repository(:drupal).adapter.select("SELECT name, lid FROM islandora_ip_embargo_lists").each do |record|
      name_ids[record.name.strip.downcase] = record.lid
    end
    return name_ids
  rescue => e
    raise SystemError, "Can't read embargo range name list from drupal database: #{e.class} #{e.message}."
  end

  def self.is_embargoed? islandora_pid
    rec = repository(:drupal).adapter.select("SELECT lid, expiry FROM islandora_ip_embargo_embargoes WHERE pid = ?", islandora_ip)
    not rec.nil?
  rescue => e
    raise SystemError, "Can't read embargoes list from drupal database (see config.yml): #{e.class} #{e.message}."
  end


  def self.insert_embargo islandora_pid, range_id, date_ob = nil
    return repository(:drupal).adapter.select("INSERT INTO islandora_ip_embargo_embargoes(pid, lid, expiry) VALUES(?, ?, ?)", islandora_pid, range_id, date_ob)
    # #####
  rescue => e
    return nil
  end


  def self.update_embargo islandora_pid, range_id, date_ob = nil
    return repository(:drupal).adapter.select("UPDATE islandora_ip_embargo_embargoes SET lid = ?, expiry = ?  WHERE pid = ?", range_id, date_ob, islandora_pid)
  rescue => e
    raise SystemError, "Can't update embargoes for #{islandora_pid} for drupal database (see config.yml): #{e.class} #{e.message}."
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
    return DrupalDataBase.list_ranges[str.downcase]
  rescue => e
    raise SystemErrorr, "Can't check drupal database for embargo ranges"
  end

  def self.add_embargo  islandora_pid, ip_range_name, expiration_date = nil

    epoch_format = nil

    # When expiration_date is given to us as nil, it means there is no
    # explicit expiration date - it is 'forever'.  We can use a nil
    # value for the update.  However, if it is a string, check_date
    # returns nil on a formatting error. (we need it to be a valid
    # yyyy-mm-dd date)

    unless expiration_date.nil?
      epoch_format = DrupalDataBase.check_date(expiration_date)
      raise PackageError, "Embargo date '#{expiration_date}' is not a valid date in YYYY-MM-DD format."  if not epoch_format
    end

    range_name_to_id_mappings = DrupalDataBase.list_ranges(ip_range_name)

    # TODO: this is alread downcased: confusing?

    unless range_id = range_name_to_id_mappings[ip_range_names.downcase]
      raise PackageError, "Embargo network range name '#{ip_range_names}' is not defined - it must be one of #{range_name_to_id_mappings.keys.sort}.join(', ')."
    end

    # These raise their own errors:

    if self.is_embargoed? islandora_pid
      self.update_embargo islandora_pid, range_id, epoch_format
    else
      self.insert_embargo islandora_pid, range_id, epoch_format
    end
end





puts check_date('2013-12-32')

Struct.new('MockConfig', :drupal_database)
config = Struct::MockConfig.new("postgres://islandora7:X5r4z!3p@localhost/islandora7")


DrupalDataBase.debug = true;
DrupalDataBase.setup(config)


puts DrupalDataBase.list_ranges.inspect



DrupalDataBase.add_embargo 'fsu:1', 4, check_date('2013-12-01')
