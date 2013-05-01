
# All our main classes include 'error' and 'warning' methods (arrays
# of strings), accessors (e.g. 'errors') and setters (e.g. 'error
# string, string...').

module Errors

  def errors
    @errors = [] if @errors.nil?
    return @errors
  end

  def errors?
    not errors.empty?
  end

  def error *strings
    errors.push *strings.flatten    unless strings.empty?
  end

  def warnings
    @warnings = [] if @warnings.nil?
    return @warnings
  end

  def warnings?
    not warnings.empty?
  end

  def warning *strings
    warnings.push *strings.flatten  unless strings.empty?
  end

end
