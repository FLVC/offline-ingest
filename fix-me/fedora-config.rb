require 'yaml'

module Fedora

  class Config

    # @param [String] yaml_path, a filepath to a YAML configuration file

    def initialize yaml_path

      # We try to be very specific with our error messages here, since
      # configuration is a pain point for new installers.

      raise "Configuration setup wasn't supplied a valid YAML file path - instead it got a #{yaml_path.class}" unless yaml_path.class == String
      raise "Configuration setup can't find the specified YAML file #{yaml_path}" unless File.exists? yaml_path
      raise "Configuration setup can't read the specified YAML file #{yaml_path}" unless File.readable? yaml_path

      begin
        @yaml = YAML.load_file yaml_path
      rescue => e
        raise "Configuration setup did not correctly parse the specified YAML file #{yaml_path}: #{e.message}"
      end

      if @yaml.class != Hash
        raise "Configuration setup parsed the specified YAML file #{yaml_path}, but it's not the expected simple hash (it's a #{@yaml.class})"
      end

    end

    def [] key
      return @yaml[key.to_s]
    end

    # return all keys in arbitrary order
    #
    # @return [Array] a list of keys

    def keys
      @yaml.keys
    end

    # return all values, in the same order as #keys
    #
    # @return [Array] a list of values

    def values
      @yaml.values
    end

    # iterate over all defined key/value pairs

    def each
      @yaml.each do |k, v|
        yield k, v
      end
    end

    # Add an accessor to a config object (e.g. to set defaults for missing keys)
    #
    # @param [String] key, a new key for the configuration object
    # @param [Object] value, the associated value

    def []= key, value
      raise "method #{key} already exists on this configuration object" if @yaml.key? key
      @yaml[key.to_s] = value
    end

  end # of class Config

end # of module Fedora
