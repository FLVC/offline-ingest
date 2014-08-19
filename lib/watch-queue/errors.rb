
# Our Resque main classes mix in these error and warning utilities:
#
#   errors                       - get the error messages, an array of strings
#   error string [, string]      - add error message(s) to the end of an array of strings
#   errors?                      - boolean, true if there are any error messages
#
# and there are similar warnings, warning, and warnings? methods.
#

# These are both logged (so Resque.Logger must have been initialized)
# and entered into a database (so the ingest DB must have been
# initialized)


module ResqueErrors

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
