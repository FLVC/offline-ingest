
# All our main classes mix in these error and warning utilities:
#
#   errors                       - get the error messages, an array of strings
#   error string [, string]      - add error message(s) to the end of an array of strings, return nil
#   errors?                      - boolean, true if there are any error messages
#
# and there are similar warnings, warning, and warnings? methods.
#
# Note: the error and warning methods work well with over-riding the
# nokogiri sax methods of the same name.


module Errors

  def errors
    @errors ||= []
    return @errors
  end

  def errors?
    not errors.empty?
  end

  def error *strings
    errors.push *strings.flatten.compact  unless strings.empty?
    return nil
  end

  def warnings
    @warnings ||= []
    return @warnings
  end

  def warnings?
    not warnings.empty?
  end

  def warning *strings
    warnings.push *strings.flatten.compact  unless strings.empty?
    return nil
  end

  def notes
    @notes ||= []
    return @notes
  end

  def notes?
    not notes.empty?
  end

  def note *strings
    notes.push *strings.flatten.compact  unless strings.empty?
    return nil
  end

  def reset_errors
    @errors  = []
  end

  def reset_notes
    @notes  = []
  end

  def reset_warnings
    @warnings  = []
  end
end
