#!/usr/bin/env ruby

$LOAD_PATH.unshift  File.join(File.dirname(__FILE__), '..')
require 'offin/config'

CONFIG_PATH = "/home/fischer/offline-ingest/config.yml"


def load_average()
  return File.read('/proc/loadavg').split(/\s+/)[0].to_f
rescue => e
  return 0.0
end


def list_ftp_queues(path)
  config = Datyl::Config.new path, :default
  queues = []
  config.all_sections.each do |section|
    next if section == 'default'
    cfg = Datyl::Config.new path, section
    queues.push cfg.site_namespace if cfg.ftp_queue
  end
  return queues.sort
end


def subrotate(config_path, repeats)
  list = list_ftp_queues(config_path)
  list.length.times do
    yield list[0..repeats-1]
    head = list.shift
    list = list + [head]
  end
end


puts "load: #{load_average}"
subrotate(CONFIG_PATH, 3) { |sublist| puts "        #{sublist.join("\t")}" }
